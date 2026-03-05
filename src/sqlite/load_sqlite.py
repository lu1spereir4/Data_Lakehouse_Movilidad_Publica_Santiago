"""
load_sqlite.py — Cargador portable: Silver Parquet → Gold SQLite (DTPM).
Versión: 1.0.0

CLI:
  python -m src.sqlite.load_sqlite --db gold_sqlite.db --dataset all --overwrite
  python -m src.sqlite.load_sqlite --db gold_sqlite.db --dataset viajes --cut 2025-04-21
  python -m src.sqlite.load_sqlite --db gold_sqlite.db --dataset subidas_30m --cut 2025-04 --overwrite
  python -m src.sqlite.load_sqlite --db gold_sqlite.db --dry-run

Requisitos: duckdb, sqlite3 (stdlib).  Sin pandas para cargas completas.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

# ── Constantes de proyecto ────────────────────────────────────────────────────
LOADER_VERSION  = "1.0.0"
_PROJECT_ROOT   = Path(__file__).resolve().parents[2]
PROCESSED_ROOT  = _PROJECT_ROOT / "lake" / "processed" / "dtpm"
QUALITY_ROOT    = _PROJECT_ROOT / "lake" / "processed" / "_quality"
QUARANTINE_ROOT = _PROJECT_ROOT / "lake" / "processed" / "_quarantine"
DOCS_DIR        = _PROJECT_ROOT / "docs" / "diagnostics"
DDL_PATH        = _PROJECT_ROOT / "models" / "sqlite" / "ddl_sqlite.sql"

BATCH_SIZE = 5_000   # filas por executemany

log = logging.getLogger(__name__)

# ── Nombres de días en español ────────────────────────────────────────────────
_DAY_NAMES   = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
_MONTH_NAMES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio",
                "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

_MODE_STATIC = [
    ("BUS",       "Bus Transantiago"),
    ("METRO",     "Metro de Santiago"),
    ("METROTREN", "Metrotren"),
    ("ZP",        "Zona Paga / Intermodal"),
    ("UNKNOWN",   "Modo desconocido"),
]

# =============================================================================
# I.  DESCUBRIMIENTO DE PARTICIONES
# =============================================================================

def _scan_silver_partitions(
    dataset_filter: Optional[str] = None,
    cut_filter: Optional[str]     = None,
) -> list[dict]:
    """
    Escanea lake/processed/dtpm/ buscando todos los archivos .parquet silver.
    Intenta primero importar src.silver.catalog.Catalog para obtener metadatos
    adicionales; si falla, usa un scanner puro de filesystem.

    Devuelve una lista de dicts con:
      dataset, cut, year, month, parquet_type, path (Path)
    Un mismo (dataset, cut) puede generar varias entradas (trip + leg, etc.).
    """
    partitions = []

    for dataset_dir in sorted(PROCESSED_ROOT.iterdir()):
        if not dataset_dir.is_dir():
            continue
        dataset = dataset_dir.name.replace("dataset=", "")
        if dataset_filter and dataset_filter != "all" and dataset != dataset_filter:
            continue

        for year_dir in sorted(dataset_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            year = int(year_dir.name.replace("year=", ""))

            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue
                month = int(month_dir.name.replace("month=", ""))

                for cut_dir in sorted(month_dir.iterdir()):
                    if not cut_dir.is_dir():
                        continue
                    cut = cut_dir.name.replace("cut=", "")
                    if cut_filter and cut != cut_filter:
                        continue

                    for pq in sorted(cut_dir.glob("*.parquet")):
                        partitions.append({
                            "dataset":      dataset,
                            "cut":          cut,
                            "year":         year,
                            "month":        month,
                            "parquet_type": pq.stem,   # e.g. "viajes_trip"
                            "path":         pq,
                        })

    return partitions


def _group_by_cut(partitions: list[dict]) -> dict[tuple, list[dict]]:
    """Agrupa particiones por (dataset, cut) para procesar por cut."""
    g: dict[tuple, list[dict]] = defaultdict(list)
    for p in partitions:
        g[(p["dataset"], p["cut"])].append(p)
    return dict(g)


# =============================================================================
# II.  UTILIDADES
# =============================================================================

def _sk_to_date(sk: Any) -> Optional[str]:
    """Convierte un integer YYYYMMDD a 'YYYY-MM-DD'. Retorna None si inválido."""
    try:
        sk = int(sk)
        if sk <= 0:
            return None
        y, m, d = sk // 10000, (sk % 10000) // 100, sk % 100
        return datetime.date(y, m, d).isoformat()
    except (TypeError, ValueError):
        return None


def _row_hash(*vals: Any) -> str:
    """SHA-256 de la concatenación UPPER(TRIM(COALESCE(x,''))) con separador '|'."""
    parts = "|".join(str(v or "").strip().upper() for v in vals)
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def _prev_date(date_str: str) -> str:
    """Devuelve la fecha del día anterior en formato ISO."""
    d = datetime.date.fromisoformat(date_str)
    return (d - datetime.timedelta(days=1)).isoformat()


def _load_quality_json(dataset: str, cut: str) -> Optional[dict]:
    """Lee el quality.json de una partición; None si no existe."""
    # Buscar en cualquier year/month bajo _quality/dataset=<d>/
    pattern = f"dataset={dataset}/**/cut={cut}/quality.json"
    candidates = list(QUALITY_ROOT.glob(pattern))
    if not candidates:
        return None
    return json.loads(candidates[0].read_text(encoding="utf-8"))


# =============================================================================
# III.  CONNEXIÓN Y DDL
# =============================================================================

def _open_db(db_path: Path, *, overwrite: bool = False) -> sqlite3.Connection:
    """Abre la DB SQLite y aplica DDL."""
    if overwrite and db_path.exists():
        log.warning("--overwrite: eliminando DB anterior %s", db_path)
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # PRAGMAs de performance
    conn.executescript("""
        PRAGMA foreign_keys   = ON;
        PRAGMA journal_mode   = WAL;
        PRAGMA synchronous    = NORMAL;
        PRAGMA temp_store     = MEMORY;
        PRAGMA cache_size     = -65536;
    """)

    if not DDL_PATH.exists():
        raise FileNotFoundError(f"DDL no encontrado: {DDL_PATH}")
    conn.executescript(DDL_PATH.read_text(encoding="utf-8"))
    log.info("DDL aplicado: %s", DDL_PATH.name)
    return conn


# =============================================================================
# IV.  DIMENSIONES
# =============================================================================

def _load_dim_cut(conn: sqlite3.Connection, partitions: list[dict]) -> dict[tuple, int]:
    """
    Inserta una fila en dim_cut por cada (dataset, cut) encontrado.
    Devuelve cache: {(dataset, cut) -> cut_sk}
    """
    loaded_at = datetime.datetime.utcnow().isoformat() + "Z"
    seen: set[tuple] = set()
    rows = []
    for p in partitions:
        key = (p["dataset"], p["cut"])
        if key in seen:
            continue
        seen.add(key)
        q = _load_quality_json(p["dataset"], p["cut"])
        extracted_at = q.get("generated_at") if q else None
        rows.append((p["dataset"], p["cut"], p["year"], p["month"],
                     extracted_at, loaded_at))

    conn.executemany(
        "INSERT OR IGNORE INTO dim_cut(dataset,cut,year,month,extracted_at,loaded_at) VALUES(?,?,?,?,?,?)",
        rows
    )
    conn.commit()

    cache = {}
    for row in conn.execute("SELECT cut_sk, dataset, cut FROM dim_cut"):
        cache[(row["dataset"], row["cut"])] = row["cut_sk"]
    log.info("dim_cut: %d filas", len(cache))
    return cache


def _load_dim_time_30m(conn: sqlite3.Connection) -> dict[int, int]:
    """Inserta las 48 franjas horarias 0..47.  Devuelve {time_30m_sk -> time_30m_sk}."""
    rows = []
    for sk in range(48):
        hour, minute = divmod(sk * 30, 60)
        label = f"{hour:02d}:{minute:02d}-{(hour + (minute + 30 >= 60)):02d}:{(minute + 30) % 60:02d}"
        rows.append((sk, hour, minute, label))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_time_30m(time_30m_sk,hour,minute,period_label) VALUES(?,?,?,?)",
        rows,
    )
    conn.commit()
    return {sk: sk for sk in range(48)}


def _load_dim_mode(conn: sqlite3.Connection) -> dict[str, int]:
    """Inserta dim_mode estático.  Devuelve {mode_code -> mode_sk}."""
    conn.executemany(
        "INSERT OR IGNORE INTO dim_mode(mode_code, mode_name) VALUES(?,?)",
        _MODE_STATIC,
    )
    conn.commit()
    cache = {}
    for r in conn.execute("SELECT mode_sk, mode_code FROM dim_mode"):
        cache[r["mode_code"]] = r["mode_sk"]
    return cache


def _load_dim_date(conn: sqlite3.Connection, duck_con: Any, partitions: list[dict]) -> dict[int, int]:
    """
    Construye dim_date generando fechas para el rango de date_*_sk encontrados
    en los parquets.  Devuelve {date_sk (YYYYMMDD) -> date_sk}.
    """
    # Recolectar rango de SKs con DuckDB (MIN/MAX por archivo es suficiente)
    sk_min, sk_max = 99999999, 0
    sk_cols_by_type = {
        "viajes_trip":        ["date_start_sk", "date_end_sk"],
        "viajes_leg":         ["date_board_sk", "date_alight_sk"],
        "etapas_validation":  ["date_board_sk", "date_alight_sk"],
        "subidas_30m":        [],
    }
    for p in partitions:
        cols = sk_cols_by_type.get(p["parquet_type"], [])
        if not cols:
            # subidas: usar year/month para month_date_sk
            # fabricar un SK para YYYYMM01
            sk = p["year"] * 10000 + p["month"] * 100 + 1
            sk_min = min(sk_min, sk)
            sk_max = max(sk_max, sk)
            continue
        where_clauses = " OR ".join(f"{c} > 0" for c in cols)
        q = f"SELECT {', '.join(f'MIN({c}), MAX({c})' for c in cols)} FROM read_parquet(?) WHERE {where_clauses}"
        try:
            row = duck_con.execute(q, [str(p["path"])]).fetchone()
            for v in row:
                if v and v > 0:
                    sk_min = min(sk_min, int(v))
                    sk_max = max(sk_max, int(v))
        except Exception as e:
            log.warning("dim_date scan error en %s: %s", p["path"].name, e)

    if sk_min > sk_max:
        log.warning("dim_date: no se encontraron SKs válidos; cargando año 2025")
        sk_min, sk_max = 20250101, 20251231

    # Generar todas las fechas entre min..max
    date_min = datetime.date(sk_min // 10000, (sk_min % 10000) // 100, max(sk_min % 100, 1))
    date_max = datetime.date(sk_max // 10000, (sk_max % 10000) // 100, max(sk_max % 100, 1))

    rows = []
    d = date_min
    one_day = datetime.timedelta(days=1)
    while d <= date_max + datetime.timedelta(days=30):  # margen de 30 días
        sk = d.year * 10000 + d.month * 100 + d.day
        dow = d.weekday()
        rows.append((
            sk, d.isoformat(), d.year, d.month, d.day,
            (d.month - 1) // 3 + 1,     # quarter
            dow,
            _DAY_NAMES[dow],
            _MONTH_NAMES[d.month],
            1 if dow >= 5 else 0,
        ))
        d += one_day

    conn.executemany(
        """INSERT OR IGNORE INTO dim_date
           (date_sk,full_date,year,month,day,quarter,day_of_week,day_name,month_name,is_weekend)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()

    cache: dict[int, int] = {}
    for r in conn.execute("SELECT date_sk FROM dim_date"):
        cache[r[0]] = r[0]
    log.info("dim_date: %d fechas (%s … %s)", len(rows), date_min, date_max)
    return cache


def _distinct_text(duck_con: Any, path: str, col: str) -> list[str]:
    """Helper: devuelve DISTINCT valores TEXT no nulos ni vacíos de una columna."""
    try:
        rows = duck_con.execute(
            f"SELECT DISTINCT {col} FROM read_parquet(?) WHERE {col} IS NOT NULL AND {col} != ''",
            [path],
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _load_dim_simple(
    conn: sqlite3.Connection,
    duck_con: Any,
    partitions: list[dict],
    table: str,
    code_col_db: str,
    extractions: list[tuple[str, str]],    # [(parquet_type, col_in_parquet)]
) -> dict[str, int]:
    """
    Carga una dimensión simple (una columna de código) con INSERT OR IGNORE.
    extractions: lista de (parquet_type, columna_en_parquet) para escanear.
    Devuelve {code -> sk}.
    """
    sk_col = f"{table.replace('dim_', '')}_sk"
    codes: set[str] = set()
    for p in partitions:
        for ptype, col in extractions:
            if p["parquet_type"] == ptype:
                codes.update(_distinct_text(duck_con, str(p["path"]), col))

    if codes:
        conn.executemany(
            f"INSERT OR IGNORE INTO {table}({code_col_db}) VALUES(?)",
            [(c,) for c in sorted(codes)],
        )
        conn.commit()

    cache: dict[str, int] = {}
    for r in conn.execute(f"SELECT {sk_col}, {code_col_db} FROM {table}"):
        cache[r[1]] = r[0]
    log.info("%s: %d códigos", table, len(cache))
    return cache


def _load_dim_operator_contract(
    conn: sqlite3.Connection,
    duck_con: Any,
    partitions: list[dict],
) -> dict[tuple, int]:
    """
    Carga dim_operator_contract.  Devuelve {(operator_code, contract_code) -> sk}.
    Fuentes:
      viajes_trip  → (contrato, contrato) [operador no disponible en trip → 'UNKNOWN']
      viajes_leg   → (operator_code, 'UNKNOWN')
      etapas       → (operador, contrato)
    """
    pairs: set[tuple] = set()
    specs = [
        ("viajes_trip",       None,            "contrato"),
        ("viajes_leg",        "operator_code", None),
        ("etapas_validation", "operador",      "contrato"),
    ]
    for p in partitions:
        for ptype, op_col, ct_col in specs:
            if p["parquet_type"] != ptype:
                continue
            path = str(p["path"])
            # build SELECT
            select_op = op_col if op_col else "'UNKNOWN'"
            select_ct = ct_col if ct_col else "'UNKNOWN'"
            try:
                rows = duck_con.execute(
                    f"SELECT DISTINCT {select_op}, {select_ct} FROM read_parquet(?)"
                    f" WHERE 1=1"
                    + (f" AND {op_col} IS NOT NULL" if op_col else "")
                    + (f" AND {ct_col} IS NOT NULL" if ct_col else ""),
                    [path],
                ).fetchall()
                for r in rows:
                    op = (r[0] or "UNKNOWN").strip().upper()
                    ct = (r[1] or "UNKNOWN").strip().upper()
                    pairs.add((op, ct))
            except Exception as e:
                log.warning("dim_operator_contract scan error: %s", e)

    if pairs:
        conn.executemany(
            "INSERT OR IGNORE INTO dim_operator_contract(operator_code, contract_code) VALUES(?,?)",
            sorted(pairs),
        )
        conn.commit()

    cache: dict[tuple, int] = {}
    for r in conn.execute("SELECT operator_contract_sk, operator_code, contract_code FROM dim_operator_contract"):
        cache[(r["operator_code"], r["contract_code"])] = r["operator_contract_sk"]
    log.info("dim_operator_contract: %d pares", len(cache))
    return cache


# =============================================================================
# V.  DIMS SCD2 (dim_stop y dim_service)
# =============================================================================

def _apply_scd2(
    conn: sqlite3.Connection,
    table: str,
    bk_col: str,
    sk_col: str,
    attr_cols: list[str],
    candidates: dict[str, list[tuple]],  # {bk -> [(event_date, hash, attrs_tuple)]}
) -> int:
    """
    Aplica la lógica SCD2 para una dimensión.

    candidates: dict donde cada valor es una lista de
      (event_date_str, hash_str, tuple_of_attr_values)
      YA ORDENADA por event_date ASC.

    Regla:
      - Si el BK no existe: INSERT con valid_from=event_date, valid_to=NULL, is_current=1
      - Si existe y hash cambió: cerrar fila actual (valid_to=prev_date, is_current=0)
                                  + INSERT nueva fila
      - Si hash no cambió: no-op
    Returns total de filas insertadas.
    """
    inserted = 0
    attr_placeholders = ",".join("?" * len(attr_cols))
    insert_sql = (
        f"INSERT OR IGNORE INTO {table}"
        f"({bk_col},{','.join(attr_cols)},valid_from,valid_to,is_current,row_hash)"
        f" VALUES(?,{attr_placeholders},?,NULL,1,?)"
    )
    close_sql = (
        f"UPDATE {table} SET valid_to=?, is_current=0"
        f" WHERE {bk_col}=? AND is_current=1 AND valid_from<=?"
    )

    for bk, timeline in candidates.items():
        for event_date, h, attrs in timeline:
            # Buscar la fila vigente más reciente
            cur_row = conn.execute(
                f"SELECT {sk_col}, row_hash, valid_from FROM {table}"
                f" WHERE {bk_col}=? AND is_current=1 ORDER BY valid_from DESC LIMIT 1",
                (bk,),
            ).fetchone()

            if cur_row is None:
                # primera vez
                conn.execute(insert_sql, (bk, *attrs, event_date, h))
                inserted += 1
            elif cur_row["row_hash"] != h:
                # cambio → cerrar fila actual y abrir nueva
                close_date = _prev_date(event_date) if event_date > cur_row["valid_from"] else event_date
                conn.execute(close_sql, (close_date, bk, event_date))
                conn.execute(insert_sql, (bk, *attrs, event_date, h))
                inserted += 1
            # else: sin cambio → no-op

    conn.commit()
    return inserted


def _collect_stop_candidates(duck_con: Any, partitions: list[dict]) -> dict[str, list[tuple]]:
    """
    Recolecta candidatos SCD2 para dim_stop de todos los parquets.
    Devuelve {stop_code -> [(event_date, hash, (comuna, zona))]} ordenado por fecha.
    """
    # (stop_code, comuna, zona, event_date) aggregated
    seen: dict[str, dict] = {}  # {stop_code -> {event_date -> (comuna, zona)}}

    specs = [
        # (parquet_type, stop_col, comuna_col, zona_col, date_sk_col, date_from_ym)
        ("viajes_trip",       "paradero_inicio_viaje", "comuna_inicio_viaje", "zona_inicio_viaje",  "date_start_sk", False),
        ("viajes_trip",       "paradero_fin_viaje",    "comuna_fin_viaje",    "zona_fin_viaje",     "date_start_sk", False),
        ("viajes_leg",        "board_stop_code",       None,                  "zone_board",          "date_board_sk", False),
        ("viajes_leg",        "alight_stop_code",      None,                  "zone_alight",         "date_board_sk", False),
        ("etapas_validation", "parada_subida",         "comuna_subida",       "zona_subida",         "date_board_sk", False),
        ("etapas_validation", "parada_bajada",         "comuna_bajada",       "zona_bajada",         "date_board_sk", False),
        ("subidas_30m",       "stop_code",             "comuna",              None,                  None,            True),
    ]

    for p in partitions:
        for ptype, s_col, c_col, z_col, d_col, from_ym in specs:
            if p["parquet_type"] != ptype:
                continue
            path = str(p["path"])

            sel_c = c_col if c_col else "NULL"
            sel_z = z_col if z_col else "NULL"
            if from_ym:
                # subidas_30m: event_date = YYYYMM01 derivado de year/month del parquet
                event_date_str = f"{p['year']:04d}-{p['month']:02d}-01"
                try:
                    rows = duck_con.execute(
                        f"SELECT DISTINCT {s_col}, {sel_c}, {sel_z} FROM read_parquet(?)"
                        f" WHERE {s_col} IS NOT NULL",
                        [path],
                    ).fetchall()
                except Exception as e:
                    log.warning("stop candidates scan %s: %s", ptype, e)
                    continue
                for r in rows:
                    sc = (r[0] or "").strip().upper()
                    if not sc:
                        continue
                    Comuna = (r[1] or "").strip() if r[1] else None
                    Zona   = int(r[2]) if r[2] is not None else None
                    seen.setdefault(sc, {})[event_date_str] = (
                        _prefer_nonempty(seen.get(sc, {}).get(event_date_str, (None, None))[0], Comuna),
                        _prefer_notnone(seen.get(sc, {}).get(event_date_str, (None, None))[1], Zona),
                    )
            else:
                try:
                    rows = duck_con.execute(
                        f"SELECT DISTINCT {s_col}, {sel_c}, {sel_z}, {d_col}"
                        f" FROM read_parquet(?)"
                        f" WHERE {s_col} IS NOT NULL AND {d_col} IS NOT NULL AND {d_col} > 0",
                        [path],
                    ).fetchall()
                except Exception as e:
                    log.warning("stop candidates scan %s: %s", ptype, e)
                    continue
                for r in rows:
                    sc = (r[0] or "").strip().upper()
                    if not sc:
                        continue
                    ed = _sk_to_date(r[3])
                    if not ed:
                        continue
                    Comuna = (r[1] or "").strip() if r[1] else None
                    Zona   = int(r[2]) if r[2] is not None else None
                    prev = seen.get(sc, {}).get(ed, (None, None))
                    seen.setdefault(sc, {})[ed] = (
                        _prefer_nonempty(prev[0], Comuna),
                        _prefer_notnone(prev[1], Zona),
                    )

    # Construir timeline ordenada
    result: dict[str, list[tuple]] = {}
    for sc, dates in seen.items():
        timeline = []
        for ed in sorted(dates):
            comuna, zona = dates[ed]
            h = _row_hash(sc, comuna or "", "" if zona is None else str(zona))
            timeline.append((ed, h, (comuna, zona)))
        result[sc] = timeline

    log.info("dim_stop SCD2 candidates: %d stop_codes únicos", len(result))
    return result


def _collect_service_candidates(duck_con: Any, partitions: list[dict]) -> dict[str, list[tuple]]:
    """
    Recolecta candidatos SCD2 para dim_service.
    Fuentes:
      viajes_leg       → service_code, mode_code, date_board_sk
      etapas_validation → servicio_subida, tipo_transporte como mode, date_board_sk
                          servicio_bajada, NULL como mode, date_board_sk
    """
    seen: dict[str, dict] = {}  # {svc_code -> {event_date -> mode_code}}

    specs = [
        ("viajes_leg",        "service_code",    "mode_code",       "date_board_sk"),
        ("etapas_validation", "servicio_subida",  "tipo_transporte", "date_board_sk"),
        # TODO: servicio_bajada no tiene mode_code directo en etapas → NULL
        ("etapas_validation", "servicio_bajada",  None,              "date_board_sk"),
    ]

    for p in partitions:
        for ptype, svc_col, mc_col, d_col in specs:
            if p["parquet_type"] != ptype:
                continue
            path = str(p["path"])
            sel_mc = mc_col if mc_col else "NULL"
            try:
                rows = duck_con.execute(
                    f"SELECT DISTINCT {svc_col}, {sel_mc}, {d_col}"
                    f" FROM read_parquet(?)"
                    f" WHERE {svc_col} IS NOT NULL AND {d_col} IS NOT NULL AND {d_col} > 0",
                    [path],
                ).fetchall()
            except Exception as e:
                log.warning("service candidates scan %s %s: %s", ptype, svc_col, e)
                continue

            for r in rows:
                sc = (r[0] or "").strip().upper()
                if not sc:
                    continue
                ed = _sk_to_date(r[2])
                if not ed:
                    continue
                mc = (r[1] or "").strip().upper() if r[1] else None
                prev_mc = seen.get(sc, {}).get(ed)
                # Preferir mode_code no nulo
                seen.setdefault(sc, {})[ed] = _prefer_nonempty(prev_mc, mc)

    result: dict[str, list[tuple]] = {}
    for sc, dates in seen.items():
        timeline = []
        for ed in sorted(dates):
            mc = dates[ed]
            h = _row_hash(sc, mc or "")
            timeline.append((ed, h, (mc,)))
        result[sc] = timeline

    log.info("dim_service SCD2 candidates: %d service_codes únicos", len(result))
    return result


def _prefer_nonempty(a: Optional[str], b: Optional[str]) -> Optional[str]:
    """Devuelve el primero no-vacío entre a y b."""
    if a and a.strip():
        return a
    return b


def _prefer_notnone(a: Optional[int], b: Optional[int]) -> Optional[int]:
    """Devuelve el primero no-None entre a y b."""
    return a if a is not None else b


# =============================================================================
# VI.  RESOLUCIÓN DE SKs (caches + as-of)
# =============================================================================

class DimCaches:
    """Contenedor de todos los lookups de dimensiones."""

    def __init__(self):
        self.cut:     dict[tuple, int] = {}    # (dataset,cut) -> cut_sk
        self.date:    dict[int, int]   = {}    # date_sk -> date_sk
        self.time:    dict[int, int]   = {}    # time_30m_sk -> time_30m_sk
        self.mode:    dict[str, int]   = {}    # mode_code -> mode_sk
        self.fare:    dict[str, int]   = {}    # fare_period_code -> fare_period_sk
        self.purpose: dict[str, int]   = {}    # purpose_code -> purpose_sk
        self.op_ct:   dict[tuple, int] = {}    # (op,ct) -> operator_contract_sk
        # SCD2: (code, event_date) -> sk | "MISS"
        self._stop_cache:    dict[tuple, Optional[int]] = {}
        self._service_cache: dict[tuple, Optional[int]] = {}

    def resolve_stop(self, conn: sqlite3.Connection, code: Optional[str], event_date: Optional[str]) -> Optional[int]:
        if not code or not event_date:
            return None
        code = code.strip().upper()
        key = (code, event_date)
        if key in self._stop_cache:
            return self._stop_cache[key]
        row = conn.execute(
            """SELECT stop_sk FROM dim_stop
               WHERE stop_code=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>=?)
               ORDER BY valid_from DESC LIMIT 1""",
            (code, event_date, event_date),
        ).fetchone()
        sk = row[0] if row else None
        self._stop_cache[key] = sk
        return sk

    def resolve_service(self, conn: sqlite3.Connection, code: Optional[str], event_date: Optional[str]) -> Optional[int]:
        if not code or not event_date:
            return None
        code = code.strip().upper()
        key = (code, event_date)
        if key in self._service_cache:
            return self._service_cache[key]
        row = conn.execute(
            """SELECT service_sk FROM dim_service
               WHERE service_code=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>=?)
               ORDER BY valid_from DESC LIMIT 1""",
            (code, event_date, event_date),
        ).fetchone()
        sk = row[0] if row else None
        self._service_cache[key] = sk
        return sk


# =============================================================================
# VII.  LECTURA PARQUET (DuckDB streaming)
# =============================================================================

def _duckdb_conn():
    """Abre una conexión DuckDB en memoria para leer parquets."""
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("SET threads=2")
    return con


def _iter_parquet_batches(duck_con: Any, path: str, select_sql: str, batch: int = BATCH_SIZE):
    """
    Ejecuta select_sql (con FROM read_parquet(?)) y produce listas de tuplas
    de tamaño ≤ batch.  Usa fetchmany() para no cargar todo en memoria.
    """
    rel = duck_con.execute(select_sql, [path])
    while True:
        rows = rel.fetchmany(batch)
        if not rows:
            break
        yield rows


# =============================================================================
# VIII.  HECHOS
# =============================================================================

def _miss_counter() -> dict:
    return defaultdict(lambda: {"total": 0, "miss": 0})


def _load_fct_trip(
    conn: sqlite3.Connection,
    duck_con: Any,
    cut_parts: list[dict],
    caches: DimCaches,
    dataset: str,
    cut: str,
    diag: dict,
) -> tuple[int, int]:
    """Carga fct_trip desde viajes_trip.parquet.  Devuelve (inserted, ignored)."""
    trip_parts = [p for p in cut_parts if p["parquet_type"] == "viajes_trip"]
    if not trip_parts:
        return 0, 0

    cut_sk = caches.cut.get((dataset, cut))
    misses = _miss_counter()
    inserted_total = ignored_total = 0

    sql_insert = """
    INSERT OR IGNORE INTO fct_trip
    (cut_sk,date_start_sk,time_start_30m_sk,date_end_sk,time_end_30m_sk,
     origin_stop_sk,dest_stop_sk,purpose_sk,operator_contract_sk,
     cut,id_viaje,id_tarjeta,tipo_dia,factor_expansion,n_etapas,
     distancia_eucl,distancia_ruta,tviaje_min)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for p in trip_parts:
        path = str(p["path"])
        q = """SELECT cut, id_viaje, id_tarjeta, tipo_dia, proposito, contrato,
                      factor_expansion, n_etapas, distancia_eucl, distancia_ruta,
                      tviaje_min, date_start_sk, time_start_30m_sk,
                      date_end_sk, time_end_30m_sk,
                      paradero_inicio_viaje, paradero_fin_viaje
               FROM read_parquet(?)"""
        batch_rows = []
        for chunk in _iter_parquet_batches(duck_con, path, q):
            for r in chunk:
                (r_cut, id_viaje, id_tarjeta, tipo_dia, proposito, contrato,
                 factor_exp, n_etapas, dist_eucl, dist_ruta, tviaje,
                 dstart, tstart, dend, tend, pini, pfin) = r

                ed = _sk_to_date(dstart)

                # Lookups
                misses["date_start"]["total"] += 1
                d_start_sk = caches.date.get(int(dstart)) if dstart else None
                if d_start_sk is None and dstart: misses["date_start"]["miss"] += 1

                misses["date_end"]["total"] += 1
                d_end_sk = caches.date.get(int(dend)) if dend else None
                if d_end_sk is None and dend: misses["date_end"]["miss"] += 1

                ts_start = caches.time.get(int(tstart)) if tstart else None
                ts_end   = caches.time.get(int(tend)) if tend else None

                misses["origin_stop"]["total"] += 1
                o_sk = caches.resolve_stop(conn, pini, ed)
                if o_sk is None and pini: misses["origin_stop"]["miss"] += 1

                misses["dest_stop"]["total"] += 1
                d_sk = caches.resolve_stop(conn, pfin, ed)
                if d_sk is None and pfin: misses["dest_stop"]["miss"] += 1

                pur_sk = caches.purpose.get((proposito or "").strip().upper())
                misses["purpose"]["total"] += 1
                if pur_sk is None and proposito: misses["purpose"]["miss"] += 1

                ct = (contrato or "UNKNOWN").strip().upper()
                oc_sk = caches.op_ct.get(("UNKNOWN", ct)) or caches.op_ct.get((ct, ct))
                misses["operator_contract"]["total"] += 1
                if oc_sk is None and contrato: misses["operator_contract"]["miss"] += 1

                cut_str = str(r_cut) if r_cut else cut
                batch_rows.append((
                    cut_sk, d_start_sk, ts_start, d_end_sk, ts_end,
                    o_sk, d_sk, pur_sk, oc_sk,
                    cut_str, id_viaje, id_tarjeta, tipo_dia,
                    factor_exp, n_etapas, dist_eucl, dist_ruta, tviaje,
                ))

            # Flush batch
            if len(batch_rows) >= BATCH_SIZE:
                _before = conn.total_changes
                conn.executemany(sql_insert, batch_rows)
                ins = conn.total_changes - _before
                inserted_total += ins
                ignored_total  += len(batch_rows) - ins
                batch_rows = []

        if batch_rows:
            _before = conn.total_changes
            conn.executemany(sql_insert, batch_rows)
            ins = conn.total_changes - _before
            inserted_total += ins
            ignored_total  += len(batch_rows) - ins

        conn.commit()

    diag["facts"]["fct_trip"]["miss_rates"] = {
        k: {"total": v["total"], "miss": v["miss"],
            "rate_pct": round(100 * v["miss"] / v["total"], 2) if v["total"] else 0}
        for k, v in misses.items()
    }
    return inserted_total, ignored_total


def _load_fct_trip_leg(
    conn: sqlite3.Connection,
    duck_con: Any,
    cut_parts: list[dict],
    caches: DimCaches,
    dataset: str,
    cut: str,
    diag: dict,
) -> tuple[int, int]:
    """Carga fct_trip_leg desde viajes_leg.parquet."""
    leg_parts = [p for p in cut_parts if p["parquet_type"] == "viajes_leg"]
    if not leg_parts:
        return 0, 0

    cut_sk = caches.cut.get((dataset, cut))
    misses = _miss_counter()
    inserted_total = ignored_total = 0

    sql_insert = """
    INSERT OR IGNORE INTO fct_trip_leg
    (cut_sk,date_board_sk,time_board_30m_sk,date_alight_sk,time_alight_30m_sk,
     mode_sk,service_sk,board_stop_sk,alight_stop_sk,fare_period_sk,
     cut,id_viaje,id_tarjeta,leg_seq,operator_code,
     zone_board,zone_alight,tv_leg_min,tc_transfer_min,te_wait_min)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for p in leg_parts:
        path = str(p["path"])
        q = """SELECT cut, id_viaje, id_tarjeta, leg_seq, mode_code, service_code,
                      operator_code, board_stop_code, alight_stop_code,
                      date_board_sk, time_board_30m_sk, date_alight_sk, time_alight_30m_sk,
                      fare_period_alight_code, zone_board, zone_alight,
                      tv_leg_min, tc_transfer_min, te_wait_min
               FROM read_parquet(?)"""
        batch_rows = []
        for chunk in _iter_parquet_batches(duck_con, path, q):
            for r in chunk:
                (r_cut, id_viaje, id_tarjeta, leg_seq, mode_code, svc_code,
                 op_code, b_stop, a_stop,
                 dboard, tboard, dalight, talight,
                 fare_code, z_board, z_alight,
                 tv, tc, te) = r

                ed = _sk_to_date(dboard)

                misses["date_board"]["total"] += 1
                db_sk = caches.date.get(int(dboard)) if dboard else None
                if db_sk is None and dboard: misses["date_board"]["miss"] += 1

                misses["date_alight"]["total"] += 1
                da_sk = caches.date.get(int(dalight)) if dalight else None
                if da_sk is None and dalight: misses["date_alight"]["miss"] += 1

                tb_sk = caches.time.get(int(tboard)) if tboard else None
                ta_sk = caches.time.get(int(talight)) if talight else None

                misses["mode"]["total"] += 1
                m_sk = caches.mode.get((mode_code or "").strip().upper())
                if m_sk is None and mode_code: misses["mode"]["miss"] += 1

                misses["service"]["total"] += 1
                s_sk = caches.resolve_service(conn, svc_code, ed)
                if s_sk is None and svc_code: misses["service"]["miss"] += 1

                misses["board_stop"]["total"] += 1
                bs_sk = caches.resolve_stop(conn, b_stop, ed)
                if bs_sk is None and b_stop: misses["board_stop"]["miss"] += 1

                misses["alight_stop"]["total"] += 1
                as_sk = caches.resolve_stop(conn, a_stop, ed)
                if as_sk is None and a_stop: misses["alight_stop"]["miss"] += 1

                misses["fare_period"]["total"] += 1
                fp_sk = caches.fare.get((fare_code or "").strip().upper())
                if fp_sk is None and fare_code: misses["fare_period"]["miss"] += 1

                cut_str = str(r_cut) if r_cut else cut
                batch_rows.append((
                    cut_sk, db_sk, tb_sk, da_sk, ta_sk,
                    m_sk, s_sk, bs_sk, as_sk, fp_sk,
                    cut_str, id_viaje, id_tarjeta, leg_seq,
                    op_code, z_board, z_alight, tv, tc, te,
                ))

            if len(batch_rows) >= BATCH_SIZE:
                _before = conn.total_changes
                conn.executemany(sql_insert, batch_rows)
                ins = conn.total_changes - _before
                inserted_total += ins
                ignored_total  += len(batch_rows) - ins
                batch_rows = []

        if batch_rows:
            _before = conn.total_changes
            conn.executemany(sql_insert, batch_rows)
            ins = conn.total_changes - _before
            inserted_total += ins
            ignored_total  += len(batch_rows) - ins

        conn.commit()

    diag["facts"]["fct_trip_leg"]["miss_rates"] = {
        k: {"total": v["total"], "miss": v["miss"],
            "rate_pct": round(100 * v["miss"] / v["total"], 2) if v["total"] else 0}
        for k, v in misses.items()
    }
    return inserted_total, ignored_total


def _load_fct_validation(
    conn: sqlite3.Connection,
    duck_con: Any,
    cut_parts: list[dict],
    caches: DimCaches,
    dataset: str,
    cut: str,
    diag: dict,
) -> tuple[int, int]:
    """Carga fct_validation desde etapas_validation.parquet."""
    val_parts = [p for p in cut_parts if p["parquet_type"] == "etapas_validation"]
    if not val_parts:
        return 0, 0

    cut_sk = caches.cut.get((dataset, cut))
    misses = _miss_counter()
    inserted_total = ignored_total = 0

    sql_insert = """
    INSERT OR IGNORE INTO fct_validation
    (cut_sk,date_board_sk,time_board_30m_sk,date_alight_sk,time_alight_30m_sk,
     board_stop_sk,alight_stop_sk,board_service_sk,alight_service_sk,
     fare_period_board_sk,fare_period_alight_sk,operator_contract_sk,
     cut,id_etapa,tipo_dia,tipo_transporte,factor_expansion,tiene_bajada,
     tiempo_etapa,dist_ruta_paraderos,dist_eucl_paraderos,t_espera_media)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for p in val_parts:
        path = str(p["path"])
        q = """SELECT cut, id_etapa, operador, contrato, tipo_dia, tipo_transporte,
                      fExpansionServicioPeriodoTS, tiene_bajada, tiempo_etapa,
                      date_board_sk, time_board_30m_sk, date_alight_sk, time_alight_30m_sk,
                      dist_ruta_paraderos, dist_eucl_paraderos, tEsperaMediaIntervalo,
                      parada_subida, parada_bajada,
                      servicio_subida, servicio_bajada,
                      periodoSubida, periodoBajada
               FROM read_parquet(?)"""
        batch_rows = []
        for chunk in _iter_parquet_batches(duck_con, path, q):
            for r in chunk:
                (r_cut, id_etapa, operador, contrato, tipo_dia, tipo_trans,
                 factor_exp, tiene_baj, tiempo_etapa,
                 dboard, tboard, dalight, talight,
                 dist_ruta, dist_eucl, t_espera,
                 p_subida, p_bajada,
                 s_subida, s_bajada,
                 per_subida, per_bajada) = r

                ed = _sk_to_date(dboard)

                misses["date_board"]["total"] += 1
                db_sk = caches.date.get(int(dboard)) if dboard else None
                if db_sk is None and dboard: misses["date_board"]["miss"] += 1

                misses["date_alight"]["total"] += 1
                da_sk = caches.date.get(int(dalight)) if dalight else None
                if da_sk is None and dalight: misses["date_alight"]["miss"] += 1

                tb_sk = caches.time.get(int(tboard)) if tboard else None
                ta_sk = caches.time.get(int(talight)) if talight else None

                misses["board_stop"]["total"] += 1
                bs_sk = caches.resolve_stop(conn, p_subida, ed)
                if bs_sk is None and p_subida: misses["board_stop"]["miss"] += 1

                misses["alight_stop"]["total"] += 1
                as_sk = caches.resolve_stop(conn, p_bajada, ed)
                if as_sk is None and p_bajada: misses["alight_stop"]["miss"] += 1

                misses["board_service"]["total"] += 1
                bsvc_sk = caches.resolve_service(conn, s_subida, ed)
                if bsvc_sk is None and s_subida: misses["board_service"]["miss"] += 1

                misses["alight_service"]["total"] += 1
                asvc_sk = caches.resolve_service(conn, s_bajada, ed)
                if asvc_sk is None and s_bajada: misses["alight_service"]["miss"] += 1

                misses["fare_board"]["total"] += 1
                fpb_sk = caches.fare.get((per_subida or "").strip().upper())
                if fpb_sk is None and per_subida: misses["fare_board"]["miss"] += 1

                misses["fare_alight"]["total"] += 1
                fpa_sk = caches.fare.get((per_bajada or "").strip().upper())
                if fpa_sk is None and per_bajada: misses["fare_alight"]["miss"] += 1

                op_n = (operador  or "UNKNOWN").strip().upper()
                ct_n = (contrato  or "UNKNOWN").strip().upper()
                oc_sk = caches.op_ct.get((op_n, ct_n))
                misses["operator_contract"]["total"] += 1
                if oc_sk is None: misses["operator_contract"]["miss"] += 1

                tiene_baj_int = 1 if tiene_baj else 0
                cut_str = str(r_cut) if r_cut else cut
                batch_rows.append((
                    cut_sk, db_sk, tb_sk, da_sk, ta_sk,
                    bs_sk, as_sk, bsvc_sk, asvc_sk,
                    fpb_sk, fpa_sk, oc_sk,
                    cut_str, id_etapa, tipo_dia, tipo_trans,
                    factor_exp, tiene_baj_int, tiempo_etapa,
                    dist_ruta, dist_eucl, t_espera,
                ))

            if len(batch_rows) >= BATCH_SIZE:
                _before = conn.total_changes
                conn.executemany(sql_insert, batch_rows)
                ins = conn.total_changes - _before
                inserted_total += ins
                ignored_total  += len(batch_rows) - ins
                batch_rows = []

        if batch_rows:
            _before = conn.total_changes
            conn.executemany(sql_insert, batch_rows)
            ins = conn.total_changes - _before
            inserted_total += ins
            ignored_total  += len(batch_rows) - ins

        conn.commit()

    diag["facts"]["fct_validation"]["miss_rates"] = {
        k: {"total": v["total"], "miss": v["miss"],
            "rate_pct": round(100 * v["miss"] / v["total"], 2) if v["total"] else 0}
        for k, v in misses.items()
    }
    return inserted_total, ignored_total


def _load_fct_boardings_30m(
    conn: sqlite3.Connection,
    duck_con: Any,
    cut_parts: list[dict],
    caches: DimCaches,
    dataset: str,
    cut: str,
    diag: dict,
) -> tuple[int, int]:
    """Carga fct_boardings_30m desde subidas_30m.parquet."""
    sub_parts = [p for p in cut_parts if p["parquet_type"] == "subidas_30m"]
    if not sub_parts:
        return 0, 0

    cut_sk = caches.cut.get((dataset, cut))
    misses = _miss_counter()
    inserted_total = ignored_total = 0

    sql_insert = """
    INSERT OR IGNORE INTO fct_boardings_30m
    (cut_sk,time_30m_sk,stop_sk,mode_sk,
     cut,month_date_sk,stop_code,mode_code,tipo_dia,subidas_promedio)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    """

    for p in sub_parts:
        path = str(p["path"])
        # event_date para as-of stop = primer día del mes
        event_date = f"{p['year']:04d}-{p['month']:02d}-01"
        month_date_sk = p["year"] * 10000 + p["month"] * 100 + 1

        q = """SELECT cut, stop_code, mode_code, tipo_dia, time_30m_sk, subidas_promedio
               FROM read_parquet(?)"""
        batch_rows = []
        for chunk in _iter_parquet_batches(duck_con, path, q):
            for r in chunk:
                r_cut, stop_code, mode_code, tipo_dia, t30m_sk, subidas = r

                t_sk = caches.time.get(int(t30m_sk)) if t30m_sk is not None else None

                misses["stop"]["total"] += 1
                s_sk  = caches.resolve_stop(conn, stop_code, event_date)
                if s_sk is None and stop_code: misses["stop"]["miss"] += 1

                misses["mode"]["total"] += 1
                m_sk  = caches.mode.get((mode_code or "").strip().upper())
                if m_sk is None and mode_code: misses["mode"]["miss"] += 1

                cut_str = str(r_cut) if r_cut else cut
                sc_norm = (stop_code or "").strip().upper()
                mc_norm = (mode_code  or "").strip().upper()
                batch_rows.append((
                    cut_sk, t_sk, s_sk, m_sk,
                    cut_str, month_date_sk, sc_norm, mc_norm, tipo_dia or "", subidas,
                ))

            if len(batch_rows) >= BATCH_SIZE:
                _before = conn.total_changes
                conn.executemany(sql_insert, batch_rows)
                ins = conn.total_changes - _before
                inserted_total += ins
                ignored_total  += len(batch_rows) - ins
                batch_rows = []

        if batch_rows:
            _before = conn.total_changes
            conn.executemany(sql_insert, batch_rows)
            ins = conn.total_changes - _before
            inserted_total += ins
            ignored_total  += len(batch_rows) - ins

        conn.commit()

    diag["facts"]["fct_boardings_30m"]["miss_rates"] = {
        k: {"total": v["total"], "miss": v["miss"],
            "rate_pct": round(100 * v["miss"] / v["total"], 2) if v["total"] else 0}
        for k, v in misses.items()
    }
    return inserted_total, ignored_total


# =============================================================================
# IX.  DIAGNÓSTICO
# =============================================================================

def _count_parquet_rows(duck_con: Any, path: str) -> int:
    """Cuenta filas en un parquet."""
    try:
        return duck_con.execute("SELECT COUNT(*) FROM read_parquet(?)", [path]).fetchone()[0]
    except Exception:
        return -1


def _detect_duplicates(duck_con: Any, path: str, grain_cols: list[str]) -> tuple[int, list]:
    """
    Detecta duplicados en el parquet según las columnas de grano indicadas.
    Devuelve (dup_count, top20_keys).
    """
    cols = ", ".join(grain_cols)
    try:
        q = f"""
        SELECT {cols}, COUNT(*) AS cnt
        FROM read_parquet(?)
        GROUP BY {cols}
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 20
        """
        rows = duck_con.execute(q, [path]).fetchall()
        total_dups = duck_con.execute(
            f"""SELECT COUNT(*) FROM (
                SELECT {cols} FROM read_parquet(?) GROUP BY {cols} HAVING COUNT(*)>1
            )""",
            [path],
        ).fetchone()[0]
        return total_dups, [list(r) for r in rows]
    except Exception as e:
        return -1, [str(e)]


def _quarantine_top_reasons(duck_con: Any, dataset: str, cut: str, n: int = 10) -> list[dict]:
    """Lee el invalid.parquet de cuarentena y devuelve top N reason_codes."""
    pattern = f"dataset={dataset}/**/cut={cut}/invalid.parquet"
    paths = list(QUARANTINE_ROOT.glob(pattern))
    if not paths:
        return []
    path = str(paths[0])
    try:
        rows = duck_con.execute(
            f"""SELECT _reason_code, COUNT(*) AS cnt
                FROM read_parquet(?)
                WHERE _reason_code IS NOT NULL
                GROUP BY _reason_code
                ORDER BY cnt DESC
                LIMIT {n}""",
            [path],
        ).fetchall()
        total = duck_con.execute("SELECT COUNT(*) FROM read_parquet(?)", [path]).fetchone()[0]
        return [{"reason": r[0], "count": r[1],
                 "pct": round(100 * r[1] / total, 2) if total else 0} for r in rows]
    except Exception as e:
        log.warning("quarantine read error %s/%s: %s", dataset, cut, e)
        return []


def _write_diagnostics(records: list[dict]) -> None:
    """Escribe sqlite_load_report.json y sqlite_load_report.md en docs/diagnostics/."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # JSON
    json_path = DOCS_DIR / "sqlite_load_report.json"
    json_path.write_text(
        json.dumps({"generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "loader_version": LOADER_VERSION,
                    "partitions": records}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Markdown
    md_lines = [
        "# SQLite Gold Load Report",
        f"Generated: {datetime.datetime.utcnow().isoformat()}Z  |  Version: {LOADER_VERSION}",
        "",
        "| dataset | cut | silver_rows | dup_count | inserted | ignored | status |",
        "|---------|-----|-------------|-----------|----------|---------|--------|",
    ]
    for rec in records:
        for fact, fd in rec.get("facts", {}).items():
            status_icon = "✅" if rec.get("quality_pass") else "⚠️"
            md_lines.append(
                f"| {rec['dataset']} | {rec['cut']} | {rec.get('silver_rows','-')} "
                f"| {fd.get('dup_count','-')} | {fd.get('inserted','-')} "
                f"| {fd.get('ignored','-')} | {status_icon} {rec.get('status','?')} |"
            )

    md_lines += ["", "## Miss Rates por Dimensión", ""]
    for rec in records:
        for fact, fd in rec.get("facts", {}).items():
            mrs = fd.get("miss_rates", {})
            if mrs:
                md_lines.append(f"### {rec['dataset']} / {rec['cut']} / {fact}")
                md_lines.append("| dim | total | miss | miss% |")
                md_lines.append("|-----|-------|------|-------|")
                for dim, m in mrs.items():
                    md_lines.append(f"| {dim} | {m['total']} | {m['miss']} | {m['rate_pct']}% |")
                md_lines.append("")

    md_lines += ["", "## Cuarentena (top reason_codes)", ""]
    for rec in records:
        qr = rec.get("quarantine_top", [])
        if qr:
            md_lines.append(f"### {rec['dataset']} / {rec['cut']}")
            md_lines.append("| reason | count | pct% |")
            md_lines.append("|--------|-------|------|")
            for q in qr:
                md_lines.append(f"| {q['reason']} | {q['count']} | {q['pct']}% |")
            md_lines.append("")

    md_path = DOCS_DIR / "sqlite_load_report.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    log.info("Diagnóstico escrito → %s", DOCS_DIR)


# =============================================================================
# X.  ORQUESTADOR PRINCIPAL
# =============================================================================

class SqliteLoader:
    """Orquesta la carga completa Silver → SQLite Gold."""

    def __init__(
        self,
        db_path: Path,
        dataset: str,
        cut: Optional[str],
        overwrite: bool,
        dry_run: bool,
    ):
        self.db_path   = db_path
        self.dataset   = dataset
        self.cut       = cut
        self.overwrite = overwrite
        self.dry_run   = dry_run

    # ── Paso 1: Descubrir particiones ─────────────────────────────────────────
    def _discover(self) -> list[dict]:
        parts = _scan_silver_partitions(self.dataset, self.cut)
        if not parts:
            log.warning("No se encontraron particiones para dataset=%s cut=%s",
                        self.dataset, self.cut)
        return parts

    # ── Paso 2: Dry-run ──────────────────────────────────────────────────────
    def _print_dry_run(self, parts: list[dict]) -> None:
        groups = _group_by_cut(parts)
        print(f"\n{'='*60}")
        print(f"  DRY-RUN: {len(groups)} partición(es) a cargar → {self.db_path}")
        print(f"{'='*60}")
        for (ds, cut), ps in sorted(groups.items()):
            print(f"  ├─ dataset={ds}  cut={cut}")
            for p in ps:
                print(f"  │    {p['parquet_type']:25s}  {p['path'].stat().st_size // 1024:>8d} KB")
            q = _load_quality_json(ds, cut)
            if q:
                print(f"  │    quality → valid={q.get('valid_row_count','?')} "
                      f"invalid={q.get('invalid_row_count','?')} "
                      f"qrate={q.get('quarantine_rate_pct','?')}%")
            print(f"  │")
        print(f"\n  Grain UNIQUE constraints:")
        print(f"    fct_trip       → (cut, id_viaje) — AS-OF: dim_stop via date_start_sk")
        print(f"    fct_trip_leg   → (cut, id_viaje, leg_seq) — AS-OF: stop+service via date_board_sk")
        print(f"    fct_validation → (cut, id_etapa) — AS-OF: stop+service via date_board_sk")
        print(f"    fct_boardings  → (cut, month_date_sk, stop_code, mode_code, tipo_dia, time_30m_sk)")
        print()

    def run(self) -> int:
        """Ejecuta la carga completa.  Devuelve exit code (0=OK, 1=error)."""
        t0 = time.perf_counter()
        parts = self._discover()

        if self.dry_run:
            self._print_dry_run(parts)
            return 0

        if not parts:
            log.error("Sin particiones: abortando.")
            return 1

        # Abrir DB
        conn = _open_db(self.db_path, overwrite=self.overwrite)
        duck_con = _duckdb_conn()

        diag_records: list[dict] = []

        try:
            # ── [A] Dims globales (una vez para toda la carga) ──────────────
            log.info("=== [A] Cargando dimensiones globales ===")
            cut_cache    = _load_dim_cut(conn, parts)
            time_cache   = _load_dim_time_30m(conn)
            mode_cache   = _load_dim_mode(conn)
            date_cache   = _load_dim_date(conn, duck_con, parts)

            fare_cache   = _load_dim_simple(
                conn, duck_con, parts, "dim_fare_period", "fare_period_code",
                [("viajes_trip", "periodo_inicio_viaje"),
                 ("viajes_trip", "periodo_fin_viaje"),
                 ("viajes_leg",  "fare_period_alight_code"),
                 ("etapas_validation", "periodoSubida"),
                 ("etapas_validation", "periodoBajada")],
            )
            purpose_cache = _load_dim_simple(
                conn, duck_con, parts, "dim_purpose", "purpose_code",
                [("viajes_trip", "proposito")],
            )
            op_ct_cache = _load_dim_operator_contract(conn, duck_con, parts)

            # ── [B] SCD2 dims ────────────────────────────────────────────────
            log.info("=== [B] Cargando dim_stop SCD2 ===")
            stop_candidates = _collect_stop_candidates(duck_con, parts)
            _apply_scd2(conn, "dim_stop", "stop_code", "stop_sk",
                        ["comuna", "zona"],
                        stop_candidates)
            cnt_stop = conn.execute("SELECT COUNT(*) FROM dim_stop").fetchone()[0]
            log.info("dim_stop: %d filas total (incluye versiones SCD2)", cnt_stop)

            log.info("=== [C] Cargando dim_service SCD2 ===")
            svc_candidates = _collect_service_candidates(duck_con, parts)
            _apply_scd2(conn, "dim_service", "service_code", "service_sk",
                        ["mode_code"],
                        svc_candidates)
            cnt_svc = conn.execute("SELECT COUNT(*) FROM dim_service").fetchone()[0]
            log.info("dim_service: %d filas total", cnt_svc)

            # ── [C] Construir caches integradas ─────────────────────────────
            caches = DimCaches()
            caches.cut     = cut_cache
            caches.date    = date_cache
            caches.time    = time_cache
            caches.mode    = mode_cache
            caches.fare    = fare_cache
            caches.purpose = purpose_cache
            caches.op_ct   = op_ct_cache

            # ── [D] Facts por (dataset, cut) ─────────────────────────────────
            log.info("=== [D] Cargando facts por cut ===")
            groups = _group_by_cut(parts)

            for (ds, cut), cut_parts in sorted(groups.items()):
                log.info("  → dataset=%s  cut=%s", ds, cut)
                t_cut = time.perf_counter()

                # Inicializar diag record
                drec: dict = {
                    "dataset": ds, "cut": cut,
                    "silver_rows": {},
                    "status": "OK",
                    "quality_pass": True,
                    "facts": {
                        "fct_trip": {"inserted": 0, "ignored": 0, "dup_count": 0, "miss_rates": {}},
                        "fct_trip_leg": {"inserted": 0, "ignored": 0, "dup_count": 0, "miss_rates": {}},
                        "fct_validation": {"inserted": 0, "ignored": 0, "dup_count": 0, "miss_rates": {}},
                        "fct_boardings_30m": {"inserted": 0, "ignored": 0, "dup_count": 0, "miss_rates": {}},
                    },
                    "quarantine_top": [],
                    "elapsed_s": 0.0,
                }

                # Conteos silver y duplicados
                grain_by_type = {
                    "viajes_trip":        ["cut", "id_viaje"],
                    "viajes_leg":         ["cut", "id_viaje", "leg_seq"],
                    "etapas_validation":  ["cut", "id_etapa"],
                    "subidas_30m":        ["cut", "stop_code", "mode_code", "tipo_dia", "time_30m_sk"],
                }
                for p in cut_parts:
                    nrows = _count_parquet_rows(duck_con, str(p["path"]))
                    drec["silver_rows"][p["parquet_type"]] = nrows
                    grain = grain_by_type.get(p["parquet_type"], [])
                    if grain:
                        dup_cnt, dup_keys = _detect_duplicates(duck_con, str(p["path"]), grain)
                        # Mapear parquet_type → fact name
                        fact_map = {"viajes_trip": "fct_trip", "viajes_leg": "fct_trip_leg",
                                    "etapas_validation": "fct_validation", "subidas_30m": "fct_boardings_30m"}
                        fn = fact_map.get(p["parquet_type"])
                        if fn:
                            drec["facts"][fn]["dup_count"] = dup_cnt
                            if dup_cnt and dup_cnt > 0:
                                drec["facts"][fn]["dup_keys_top20"] = dup_keys
                                log.warning("  DUPLICADOS en %s/%s %s: %d grupos",
                                            ds, cut, p["parquet_type"], dup_cnt)

                # quality.json comparación
                qj = _load_quality_json(ds, cut)
                if qj:
                    drec["quality_json"] = {
                        "meta_row_count":    qj.get("meta_row_count"),
                        "read_row_count":    qj.get("read_row_count"),
                        "valid_row_count":   qj.get("valid_row_count"),
                        "invalid_row_count": qj.get("invalid_row_count"),
                        "count_assertion":   qj.get("count_assertion"),
                    }
                    if qj.get("count_assertion") != "PASS":
                        drec["quality_pass"] = False

                # Cuarentena top reasons
                drec["quarantine_top"] = _quarantine_top_reasons(duck_con, ds, cut)

                # Cargar facts dentro de una transacción por cut
                try:
                    conn.execute("BEGIN")
                    ins, ign = _load_fct_trip(conn, duck_con, cut_parts, caches, ds, cut, drec)
                    drec["facts"]["fct_trip"]["inserted"] = ins
                    drec["facts"]["fct_trip"]["ignored"]  = ign

                    ins, ign = _load_fct_trip_leg(conn, duck_con, cut_parts, caches, ds, cut, drec)
                    drec["facts"]["fct_trip_leg"]["inserted"] = ins
                    drec["facts"]["fct_trip_leg"]["ignored"]  = ign

                    ins, ign = _load_fct_validation(conn, duck_con, cut_parts, caches, ds, cut, drec)
                    drec["facts"]["fct_validation"]["inserted"] = ins
                    drec["facts"]["fct_validation"]["ignored"]  = ign

                    ins, ign = _load_fct_boardings_30m(conn, duck_con, cut_parts, caches, ds, cut, drec)
                    drec["facts"]["fct_boardings_30m"]["inserted"] = ins
                    drec["facts"]["fct_boardings_30m"]["ignored"]  = ign

                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    drec["status"] = "FAILED"
                    drec["error"]  = str(exc)
                    log.exception("  ERROR en cut %s/%s: %s", ds, cut, exc)

                drec["elapsed_s"] = round(time.perf_counter() - t_cut, 2)
                total_ins = sum(f["inserted"] for f in drec["facts"].values())
                total_ign = sum(f["ignored"]  for f in drec["facts"].values())
                log.info("  ✓ %s/%s → inserted=%d ignored=%d elapsed=%.1fs",
                         ds, cut, total_ins, total_ign, drec["elapsed_s"])
                diag_records.append(drec)

        finally:
            # ── [E] Diagnóstico ───────────────────────────────────────────────
            _write_diagnostics(diag_records)
            conn.close()
            duck_con.close()

        elapsed = round(time.perf_counter() - t0, 1)
        any_fail = any(r["status"] == "FAILED" for r in diag_records)
        log.info("=== Carga completa en %.1fs — %s ===", elapsed,
                 "FAILED (ver reporte)" if any_fail else "OK")
        return 1 if any_fail else 0


# =============================================================================
# XI.  CLI
# =============================================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.sqlite.load_sqlite",
        description="Carga Silver Parquet → Gold SQLite con diagnóstico.",
    )
    p.add_argument("--db",      default="gold_sqlite.db",
                   help="Ruta al archivo SQLite destino (default: gold_sqlite.db)")
    p.add_argument("--dataset", default="all",
                   choices=["all", "viajes", "etapas", "subidas_30m"],
                   help="Dataset a cargar (default: all)")
    p.add_argument("--cut",     default=None,
                   help="Filtro de cut (ej: 2025-04-21). Omitir para todos.")
    p.add_argument("--overwrite", action="store_true",
                   help="Eliminar DB existente antes de cargar (reset total).")
    p.add_argument("--dry-run", action="store_true",
                   help="Solo imprime el plan; no carga datos.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Nivel de logging (default: INFO)")
    return p


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = _PROJECT_ROOT / args.db if not Path(args.db).is_absolute() else Path(args.db)
    loader  = SqliteLoader(
        db_path   = db_path,
        dataset   = args.dataset,
        cut       = args.cut,
        overwrite = args.overwrite,
        dry_run   = args.dry_run,
    )
    sys.exit(loader.run())


if __name__ == "__main__":
    main()

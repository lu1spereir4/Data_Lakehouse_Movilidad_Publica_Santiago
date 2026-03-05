"""
load_gold.py  —  Loader Python para la capa Gold DTPM (SQL Server DW Kimball).

Flujo por partición:
  1. Ejecutar DDL (idempotente — solo crea si no existe).
  2. Cargar dims estáticas una sola vez: dim_date, dim_time_30m, dim_mode.
  3. Para cada partición Silver encontrada:
     a. Truncar staging y hacer bulk load de Parquets.
     b. Upsert dim_cut (desde quality.json).
     c. Upsert dims simples: dim_fare_period, dim_purpose, dim_operator_contract.
     d. SCD2 upsert: dim_stop, dim_service.
     e. MERGE facts desde staging JOIN dims (set-based, idempotente por grain).

Ejecución:
    python -m src.gold.load_gold --cut 2025-04-21
    python -m src.gold.load_gold --dataset etapas
    python -m src.gold.load_gold --dataset all
    python -m src.gold.load_gold --dataset all --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyodbc

from src.gold.sql_helpers import (
    DDL_PATH,
    begin_tx,
    build_lookup_dict,
    bulk_insert,
    commit_tx,
    exec_scalar,
    execute_sql,
    execute_sql_file,
    execute_sql_scalar,
    fetch_df,
    get_connection,
    rollback_tx,
    setup_logging,
    upsert_lookup_dim,
)

log = logging.getLogger(__name__)

_PROJECT_ROOT   = Path(__file__).resolve().parents[2]
_LAKE_ROOT      = _PROJECT_ROOT / "lake" / "processed"
LOADER_VERSION  = "2.0.0"  # bump al cambiar lógica de carga

SUPPORTED_DATASETS = ("viajes", "etapas", "subidas_30m")

# ─────────────────────────────────────────────────────────────
# Descubrimiento de particiones Silver
# ─────────────────────────────────────────────────────────────

@dataclass
class SilverPartition:
    dataset: str
    cut:     str            # e.g. '2025-04-21', '2025-04'
    year:    int
    month:   int
    parquet_files: dict[str, Path] = field(default_factory=dict)  # {type: path}
    quality: dict[str, Any]        = field(default_factory=dict)


def _find_quality_json(base_dir: Path) -> dict[str, Any]:
    q = base_dir / "quality.json"
    if q.exists():
        with open(q, encoding="utf-8") as f:
            return json.load(f)
    return {}


def discover_partitions(
    cut_filter: str | None = None,
    dataset_filter: str | None = None,
) -> list[SilverPartition]:
    """
    Escanea lake/processed/dtpm/ y devuelve todas las SilverPartitions disponibles.
    Filtra por dataset y/o cut si se proporcionan.
    """
    dtpm_root = _LAKE_ROOT / "dtpm"
    partitions: list[SilverPartition] = []

    datasets = (
        [dataset_filter]
        if dataset_filter and dataset_filter != "all"
        else list(SUPPORTED_DATASETS)
    )

    for ds in datasets:
        ds_dir = dtpm_root / f"dataset={ds}"
        if not ds_dir.exists():
            log.warning("Directorio no encontrado: %s", ds_dir)
            continue

        for year_dir in sorted(ds_dir.glob("year=*")):
            year = int(year_dir.name.split("=")[1])
            for month_dir in sorted(year_dir.glob("month=*")):
                month = int(month_dir.name.split("=")[1])
                for cut_dir in sorted(month_dir.glob("cut=*")):
                    cut_id = cut_dir.name.split("=")[1]

                    # Filtro por cut
                    if cut_filter and cut_filter != "all" and cut_filter not in cut_id:
                        continue

                    # Recopilar parquets disponibles
                    pq_files: dict[str, Path] = {}
                    for pq in cut_dir.glob("*.parquet"):
                        pq_files[pq.stem] = pq  # stem = nombre sin .parquet

                    if not pq_files:
                        log.warning("Sin parquets en %s — skip.", cut_dir)
                        continue

                    # Quality JSON (desde _quality/)
                    quality_dir = (
                        _LAKE_ROOT / "_quality"
                        / f"dataset={ds}"
                        / f"year={year}"
                        / f"month={month:02d}"
                        / f"cut={cut_id}"
                    )
                    quality = _find_quality_json(quality_dir)

                    partitions.append(
                        SilverPartition(
                            dataset=ds,
                            cut=cut_id,
                            year=year,
                            month=month,
                            parquet_files=pq_files,
                            quality=quality,
                        )
                    )

    log.info("Particiones Silver descubiertas: %d", len(partitions))
    return partitions


# ─────────────────────────────────────────────────────────────
# Helpers SCD2  —  type-safe, batch-optimized
# ─────────────────────────────────────────────────────────────

# Type hints per column for each SCD2 dimension.
# Supported hints: "int", "str", "bit", "date", "float", "hash"
_SCD2_SCHEMA: dict[str, dict[str, str]] = {
    "dw.dim_stop": {
        "stop_code":     "str",
        "stop_name":     "str",
        "stop_type":     "str",
        "comuna":        "str",
        "zone_code":     "str",
        "x_utm":         "int",   # SQL INT — must NOT be float
        "y_utm":         "int",   # SQL INT — must NOT be float
        "row_hash":      "hash",
        "valid_from":    "date",
        "valid_to":      "date",
        "is_current":    "bit",
    },
    "dw.dim_service": {
        "service_code":  "str",
        "service_name":  "str",
        "mode_code":     "str",
        "row_hash":      "hash",
        "valid_from":    "date",
        "valid_to":      "date",
        "is_current":    "bit",
    },
}


def _sanitize_val(value: Any, hint: str) -> Any:
    """
    Coerce `value` to the Python type expected by pyodbc for `hint`.

    Rules:
      "int"  → int or None (NaN/Inf → None; float whole-num → int; str digits → int)
      "str"  → str or None (empty → None; non-str → str; NaN → None)
      "bit"  → 1 or 0 or None
      "date" → date or None
      "hash" → str (passthrough — already a valid sha256 hex string)
      "float"→ float or None (NaN/Inf → None)
    """
    # ── null-like ─────────────────────────────────────────────
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    # pandas NA
    try:
        import pandas as pd  # noqa: PLC0415
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    # ── per-hint coercion ─────────────────────────────────────
    if hint == "int":
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            # float with fractional part — drop fractional (floor) and warn
            log.warning("_sanitize_val: float with fraction for int column: %r — truncating", value)
            return int(value)
        try:
            return int(str(value).strip().split(".")[0])
        except (ValueError, TypeError):
            log.warning("_sanitize_val: cannot cast %r to int — using None", value)
            return None

    if hint == "str":
        if isinstance(value, str):
            s = value.strip()
            return s if s else None
        # numpy/pandas string types
        s = str(value).strip()
        return s if s else None

    if hint == "bit":
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            return 1 if value else 0
        s = str(value).strip().lower()
        return 1 if s in ("1", "true", "yes", "t") else 0

    if hint == "date":
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None

    if hint == "float":
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # "hash" or unknown — return as-is
    return value


def _sanitize_row(
    row_dict: dict[str, Any],
    dim_table: str,
    cols: list[str],
) -> list[Any]:
    """
    Returns a list of sanitized values for `cols` using the schema map.
    Falls back to passthrough for columns not in the schema.
    """
    schema = _SCD2_SCHEMA.get(dim_table, {})
    result = []
    for col in cols:
        raw = row_dict.get(col)
        hint = schema.get(col, "str")   # default to str for unknown cols
        result.append(_sanitize_val(raw, hint))
    return result


def _row_hash(row: dict[str, Any], attr_cols: list[str]) -> str:
    """
    SHA-256 hex (64 chars) de los atributos SCD2 normalizados.

    Normalización: UPPER + STRIP por valor; NULL → cadena vacía.
    Separador '||' entre valores.
    UPPER+STRIP garantiza hash estable ante variaciones de case/espacios
    (equivale a UPPER(RTRIM(LTRIM(...))) en T-SQL).
    """
    parts = [str(row.get(c) or "").strip().upper() for c in attr_cols]
    raw = "||".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def scd2_upsert(
    conn: pyodbc.Connection,
    dim_table: str,          # 'dw.dim_stop'
    bk_col: str,             # 'stop_code'
    attr_cols: list[str],    # columnas que determinan cambio → row_hash
    new_df: pd.DataFrame,    # DataFrame con BK + attr_cols (DISTINCT)
    event_date: date,        # fecha del corte Silver analizado
) -> dict[str, int]:
    """
    Implementa SCD Tipo 2 con batch inserts/updates para minimizar roundtrips.

      - Nuevos BKs → INSERT (valid_from=event_date, valid_to=NULL, is_current=1).
      - BKs existentes con hash distinto:
          1) UPDATE fila actual (valid_to=event_date-1, is_current=0).
          2) INSERT nueva fila.
      - BKs existentes con mismo hash → skip.

    Performance:
      - 1 SELECT para cargar estado actual.
      - 1 executemany para UPDATEs de expiración.
      - 1 executemany para INSERTs de filas nuevas.
      Total: 3 round-trips independientemente de N filas.

    Devuelve counts {'inserted': n, 'expired': n, 'unchanged': n}.
    """
    if new_df.empty:
        return {"inserted": 0, "expired": 0, "unchanged": 0}

    # ── 1. Pre-cargar estado actual en dict (1 SELECT) ────────
    current = fetch_df(
        conn,
        f"SELECT [{bk_col}], row_hash, valid_from FROM {dim_table} WHERE is_current = 1",
    )
    current_dict: dict[str, tuple[str, date]] = {}
    for _, row in current.iterrows():
        current_dict[str(row[bk_col])] = (str(row["row_hash"]), row["valid_from"])

    counts      = {"inserted": 0, "expired": 0, "unchanged": 0}
    expire_date = event_date - timedelta(days=1)

    # Build INSERT template once
    all_cols = [bk_col] + attr_cols + ["row_hash", "valid_from", "valid_to", "is_current"]
    col_str  = ", ".join(f"[{c}]" for c in all_cols)
    val_str  = ", ".join("?" * len(all_cols))
    insert_sql = f"INSERT INTO {dim_table} ({col_str}) VALUES ({val_str})"

    expire_sql = (
        f"UPDATE {dim_table} "
        f"SET is_current = 0, valid_to = ? "
        f"WHERE [{bk_col}] = ? AND is_current = 1"
    )

    # Collect batches
    insert_params: list[tuple] = []
    expire_params: list[tuple] = []

    for _, row in new_df.iterrows():
        bk_val = row.get(bk_col)
        if not bk_val or str(bk_val).strip() == "":
            continue
        bk_val = str(bk_val).strip()

        row_dict = row.to_dict()
        new_hash = _row_hash(row_dict, attr_cols)

        if bk_val not in current_dict:
            # New BK → queue INSERT
            vals = _sanitize_row(
                {**row_dict, "row_hash": new_hash, "valid_from": event_date,
                 "valid_to": None, "is_current": 1},
                dim_table, all_cols,
            )
            insert_params.append(tuple(vals))
            counts["inserted"] += 1

        else:
            existing_hash, existing_from = current_dict[bk_val]

            if existing_hash == new_hash:
                counts["unchanged"] += 1
                continue

            if event_date <= existing_from:
                log.warning(
                    "SCD2 %s: event_date=%s <= valid_from existente=%s para BK=%s — skip.",
                    dim_table, event_date, existing_from, bk_val,
                )
                counts["unchanged"] += 1
                continue

            # Changed attribute → queue EXPIRE + INSERT
            expire_params.append((expire_date, bk_val))
            vals = _sanitize_row(
                {**row_dict, "row_hash": new_hash, "valid_from": event_date,
                 "valid_to": None, "is_current": 1},
                dim_table, all_cols,
            )
            insert_params.append(tuple(vals))
            counts["inserted"] += 1
            counts["expired"] += 1

    # ── 2. Batch UPDATEs (expiry) ─────────────────────────────
    if expire_params:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(expire_sql, expire_params)
        cursor.close()

    # ── 3. Batch INSERTs ──────────────────────────────────────
    if insert_params:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, insert_params)
        cursor.close()

    conn.commit()
    log.info(
        "SCD2 %s | inserted=%d expired=%d unchanged=%d",
        dim_table, counts["inserted"], counts["expired"], counts["unchanged"],
    )
    return counts


# ─────────────────────────────────────────────────────────────
# Loader principal
# ─────────────────────────────────────────────────────────────

class GoldLoader:
    """
    Orquesta la carga completa de la capa Gold para una o más particiones Silver.
    """

    def __init__(
        self,
        conn: pyodbc.Connection,
        dry_run: bool = False,
        overwrite_staging: bool = True,
    ) -> None:
        self.conn              = conn
        self.dry_run           = dry_run
        self.overwrite_staging = overwrite_staging  # si False: no truncar staging (re-run dims/facts)
        self._duckdb = duckdb.connect(":memory:")
        self._duckdb.execute(f"SET memory_limit='4GB'")
        self._duckdb.execute(f"SET threads TO {__import__('os').cpu_count() or 4}")

    # ── 1. DDL ────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Ejecuta ddl_gold.sql (idempotente)."""
        log.info("Ejecutando DDL: %s", DDL_PATH)
        if self.dry_run:
            log.info("[DRY-RUN] skip DDL")
            return
        execute_sql_file(self.conn, DDL_PATH)

    # ── 2. Dimensiones estáticas (cargadas una sola vez) ──────

    def load_static_dims(self) -> None:
        """Carga dim_time_30m y dim_mode si están vacías."""
        if self.dry_run:
            log.info("[DRY-RUN] skip static dims")
            return
        self._load_dim_time_30m()
        self._load_dim_mode()

    def _load_dim_time_30m(self) -> None:
        count = execute_sql_scalar(self.conn, "SELECT COUNT(*) FROM dw.dim_time_30m")
        if count and count >= 48:
            log.info("dim_time_30m ya cargada (%d filas) — skip.", count)
            return
        rows = []
        for sk in range(48):
            h      = sk // 2
            m      = 30 if sk % 2 else 0
            start  = f"{h:02d}:{m:02d}:00"
            end_h  = (sk + 1) // 2
            end_m  = 30 if (sk + 1) % 2 else 0
            end    = f"{end_h % 24:02d}:{end_m:02d}:00"
            label  = f"{h:02d}:{m:02d}"
            rows.append((sk, start, end, h, m, label))
        df = pd.DataFrame(rows, columns=["time_30m_sk", "start_time", "end_time", "hour", "minute", "label"])
        bulk_insert(self.conn, "dw.dim_time_30m", df, truncate_first=False)

    def _load_dim_mode(self) -> None:
        modes = [
            ("BUS",       "Bus RED"),
            ("METRO",     "Metro de Santiago"),
            ("METROTREN", "Metrotren"),
            ("ZP",        "Zona Paga / Estación"),
            ("UNKNOWN",   "Modo desconocido"),
        ]
        for code, desc in modes:
            execute_sql(
                self.conn,
                """
                IF NOT EXISTS (SELECT 1 FROM dw.dim_mode WHERE mode_code = ?)
                    INSERT INTO dw.dim_mode (mode_code, mode_desc) VALUES (?, ?)
                """,
                (code, code, desc),
                commit=False,
            )
        self.conn.commit()
        log.info("dim_mode: upsert de %d modos.", len(modes))

    def _ensure_dim_date(self, date_sks: list[int]) -> None:
        """
        Asegura que todas las fechas necesarias existen en dim_date.
        Genera el rango desde min→max date_sk y hace INSERT de las faltantes.
        """
        if not date_sks:
            return
        valid_sks = [s for s in date_sks if s and isinstance(s, (int, float)) and s > 19000101]
        if not valid_sks:
            return

        min_sk = int(min(valid_sks))
        max_sk = int(max(valid_sks))

        def sk_to_date(sk: int) -> date:
            y, rem = divmod(sk, 10000)
            m, d   = divmod(rem, 100)
            return date(y, m, d)

        d_min = sk_to_date(min_sk)
        d_max = sk_to_date(max_sk)

        # Obtener fechas ya existentes en la dim
        existing = fetch_df(self.conn, f"SELECT date_sk FROM dw.dim_date WHERE date_sk BETWEEN {min_sk} AND {max_sk}")
        existing_set = set(existing["date_sk"].astype(int).tolist()) if not existing.empty else set()

        ES_NAMES  = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
        MES_NAMES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio",
                     "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

        rows = []
        current = d_min
        while current <= d_max:
            sk = current.year * 10000 + current.month * 100 + current.day
            if sk not in existing_set:
                dow       = current.weekday()  # 0=Mon, 6=Sun
                is_wknd   = 1 if dow >= 5 else 0
                tipo_dia  = "DOMINGO" if dow == 6 else ("SABADO" if dow == 5 else "LABORAL")
                rows.append((
                    sk,
                    current,
                    current.year,
                    current.month,
                    current.day,
                    current.isocalendar()[1],   # iso_week
                    ES_NAMES[dow],
                    MES_NAMES[current.month],
                    is_wknd,
                    f"{current.year}-{current.month:02d}",
                    tipo_dia,
                ))
            current += timedelta(days=1)

        if rows:
            df = pd.DataFrame(rows, columns=[
                "date_sk","full_date","year","month","day",
                "iso_week","day_of_week","month_name","is_weekend","year_month","tipo_dia",
            ])
            bulk_insert(self.conn, "dw.dim_date", df)
            log.info("dim_date: %d filas nuevas generadas.", len(rows))
        else:
            log.info("dim_date: sin nuevas fechas en rango [%d, %d].", min_sk, max_sk)

    # ── 3. Staging ────────────────────────────────────────────

    def load_staging(self, partition: SilverPartition) -> int:
        """Trunca staging (si overwrite_staging=True) y hace bulk load.

        Returns:
            Total de filas cargadas en staging (0 para dry-run).
        """
        if self.dry_run:
            log.info("[DRY-RUN] staging para %s/%s", partition.dataset, partition.cut)
            return 0

        if not self.overwrite_staging:
            log.info(
                "--no-overwrite-staging: staging no truncado; asumiendo datos previos para %s/%s.",
                partition.dataset, partition.cut,
            )

        if partition.dataset == "viajes":
            return self._load_stg_viajes(partition)
        elif partition.dataset == "etapas":
            return self._load_stg_etapas(partition)
        elif partition.dataset == "subidas_30m":
            return self._load_stg_subidas(partition)
        else:
            log.warning("Dataset desconocido: %s — skip staging.", partition.dataset)
            return 0

    def _read_parquet(self, path: Path) -> pd.DataFrame:
        """Lee un Parquet con DuckDB y devuelve DataFrame (NaN→None ya hecho)."""
        p = str(path).replace("\\", "/")
        df = self._duckdb.execute(f"SELECT * FROM read_parquet('{p}')").fetchdf()
        return df.astype(object).where(pd.notna(df), None)

    def _load_stg_viajes(self, part: SilverPartition) -> int:
        total = 0
        # viajes_trip
        if "viajes_trip" in part.parquet_files:
            df = self._read_parquet(part.parquet_files["viajes_trip"])
            # Normalizar tipos: cut→DATE, y month/year a int
            df = self._normalize_trip_df(df)
            stg_cols = [
                "cut","year","month","id_viaje","id_tarjeta","tipo_dia","proposito","contrato",
                "factor_expansion","n_etapas","distancia_eucl","distancia_ruta",
                "tiempo_inicio_viaje","tiempo_fin_viaje",
                "date_start_sk","time_start_30m_sk","date_end_sk","time_end_30m_sk",
                "paradero_inicio_viaje","paradero_fin_viaje",
                "comuna_inicio_viaje","comuna_fin_viaje",
                "zona_inicio_viaje","zona_fin_viaje",
                "periodo_inicio_viaje","periodo_fin_viaje","tviaje_min",
            ]
            df = df[[c for c in stg_cols if c in df.columns]]
            total += bulk_insert(self.conn, "staging.stg_viajes_trip", df,
                                 truncate_first=self.overwrite_staging)

        # viajes_leg
        if "viajes_leg" in part.parquet_files:
            df = self._read_parquet(part.parquet_files["viajes_leg"])
            df = self._normalize_trip_df(df)
            stg_cols = [
                "cut","year","month","id_viaje","id_tarjeta","leg_seq","mode_code",
                "service_code","operator_code","board_stop_code","alight_stop_code",
                "ts_board","ts_alight",
                "date_board_sk","time_board_30m_sk","date_alight_sk","time_alight_30m_sk",
                "fare_period_alight_code","zone_board","zone_alight",
                "tv_leg_min","tc_transfer_min","te_wait_min",
            ]
            df = df[[c for c in stg_cols if c in df.columns]]
            total += bulk_insert(self.conn, "staging.stg_viajes_leg", df,
                                 truncate_first=self.overwrite_staging)
        return total

    def _load_stg_etapas(self, part: SilverPartition) -> int:
        pq_key = [k for k in part.parquet_files if "etapas" in k or "validation" in k]
        if not pq_key:
            log.warning("No se encontró parquet de etapas en %s/%s", part.dataset, part.cut)
            return 0
        df = self._read_parquet(part.parquet_files[pq_key[0]])
        df = df.rename(columns={"month": "_month_raw", "year": "_year_raw"})
        df["year"]  = part.year
        df["month"] = part.month
        df["cut"]   = part.cut
        # Mapear time_board/alight a TINYINT-safe
        for col in ["time_board_30m_sk", "time_alight_30m_sk"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(object).where(pd.notna(df[col]), None)
        stg_cols = [
            "cut","year","month","id_etapa","operador","contrato","tipo_dia","tipo_transporte",
            "fExpansionServicioPeriodoTS","tiene_bajada","tiempo_subida","tiempo_bajada","tiempo_etapa",
            "date_board_sk","time_board_30m_sk","date_alight_sk","time_alight_30m_sk",
            "x_subida","y_subida","x_bajada","y_bajada",
            "dist_ruta_paraderos","dist_eucl_paraderos",
            "servicio_subida","servicio_bajada","parada_subida","parada_bajada",
            "comuna_subida","comuna_bajada","zona_subida","zona_bajada",
            "tEsperaMediaIntervalo","periodoSubida","periodoBajada",
        ]
        df = df[[c for c in stg_cols if c in df.columns]]
        return bulk_insert(self.conn, "staging.stg_etapas_validation", df,
                           truncate_first=self.overwrite_staging, chunk_size=25_000)

    def _load_stg_subidas(self, part: SilverPartition) -> int:
        if "subidas_30m" not in part.parquet_files:
            return 0
        df = self._read_parquet(part.parquet_files["subidas_30m"])
        df["cut"]   = part.cut
        df["year"]  = part.year
        df["month"] = part.month
        stg_cols = ["cut","year","month","tipo_dia","mode_code","stop_code","comuna","time_30m_sk","subidas_promedio"]
        df = df[[c for c in stg_cols if c in df.columns]]
        return bulk_insert(self.conn, "staging.stg_subidas_30m", df,
                           truncate_first=self.overwrite_staging)

    @staticmethod
    def _normalize_trip_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza types comunes en viajes_trip / viajes_leg para staging."""
        if "year" in df.columns:
            df["year"] = pd.to_numeric(df["year"], errors="coerce")
        if "month" in df.columns:
            df["month"] = pd.to_numeric(df["month"], errors="coerce")
        # time_30m_sk columns: BIGINT en parquet → TINYINT en staging (0-47)
        for col in df.columns:
            if "30m_sk" in col and col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(object).where(pd.notna(df[col]), None)
        return df

    # ── 4. Upsert dim_cut ─────────────────────────────────────

    def upsert_dim_cut(self, partition: SilverPartition) -> None:
        if self.dry_run:
            return
        q = partition.quality
        extracted_at = None
        if q.get("generated_at"):
            try:
                extracted_at = datetime.fromisoformat(q["generated_at"].replace("Z",""))
            except Exception:
                pass

        execute_sql(
            self.conn,
            """
            IF NOT EXISTS (
                SELECT 1 FROM dw.dim_cut WHERE dataset_name = ? AND cut_id = ?
            )
            INSERT INTO dw.dim_cut (dataset_name, cut_id, year, month, extracted_at, row_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                partition.dataset, partition.cut,
                partition.dataset, partition.cut,
                partition.year, partition.month,
                extracted_at,
                q.get("read_row_count"),
            ),
        )
        log.info("dim_cut: upsert dataset=%s cut=%s", partition.dataset, partition.cut)

    # ── 5. Dims simples (sin SCD2) ────────────────────────────

    def upsert_simple_dims(self, dataset: str) -> None:
        """Upserta dim_fare_period, dim_purpose y dim_operator_contract desde staging."""
        if self.dry_run:
            return

        if dataset in ("viajes", "etapas"):
            # Fare periods
            fare_periods: list[str] = []
            if dataset == "viajes":
                for col in ("periodo_inicio_viaje", "periodo_fin_viaje"):
                    r = fetch_df(self.conn, f"SELECT DISTINCT [{col}] FROM staging.stg_viajes_trip WHERE [{col}] IS NOT NULL")
                    fare_periods += r.iloc[:, 0].tolist()
            elif dataset == "etapas":
                for col in ("periodoSubida", "periodoBajada"):
                    r = fetch_df(self.conn, f"SELECT DISTINCT [{col}] FROM staging.stg_etapas_validation WHERE [{col}] IS NOT NULL")
                    fare_periods += r.iloc[:, 0].tolist()
            upsert_lookup_dim(self.conn, "dw.dim_fare_period", "fare_period_name", fare_periods)

        if dataset == "viajes":
            # Propósitos
            r = fetch_df(self.conn, "SELECT DISTINCT proposito FROM staging.stg_viajes_trip WHERE proposito IS NOT NULL")
            upsert_lookup_dim(self.conn, "dw.dim_purpose", "purpose_name", r["proposito"].tolist())

            # Operadores/contratos
            r = fetch_df(self.conn, "SELECT DISTINCT contrato FROM staging.stg_viajes_trip WHERE contrato IS NOT NULL")
            for _, row in r.iterrows():
                code = str(row["contrato"]).strip()
                if code:
                    execute_sql(
                        self.conn,
                        """
                        IF NOT EXISTS (SELECT 1 FROM dw.dim_operator_contract WHERE contract_code = ?)
                            INSERT INTO dw.dim_operator_contract (contract_code) VALUES (?)
                        """,
                        (code, code),
                        commit=False,
                    )
            self.conn.commit()

        if dataset == "etapas":
            # Operadores desde etapas (columna operador)
            r = fetch_df(self.conn, "SELECT DISTINCT operador, contrato FROM staging.stg_etapas_validation WHERE operador IS NOT NULL OR contrato IS NOT NULL")
            for _, row in r.iterrows():
                op  = str(row.get("operador") or "").strip() or None
                con = str(row.get("contrato") or "").strip() or None
                bk  = con or op
                if bk:
                    execute_sql(
                        self.conn,
                        """
                        IF NOT EXISTS (SELECT 1 FROM dw.dim_operator_contract WHERE contract_code = ?)
                            INSERT INTO dw.dim_operator_contract (contract_code, operator_code) VALUES (?, ?)
                        """,
                        (bk, bk, op),
                        commit=False,
                    )
            self.conn.commit()

    # ── 6. SCD2 dims (dim_stop, dim_service) ─────────────────

    def upsert_dim_stop(self, dataset: str, event_date: date) -> None:
        """
        Agrega stops nuevos/cambiados en dim_stop via SCD2.
        Fuentes:
          - viajes: board_stop_code/alight_stop_code de stg_viajes_leg
          - etapas: parada_subida/parada_bajada de stg_etapas_validation (con coords)
          - subidas_30m: stop_code de stg_subidas_30m
        """
        if self.dry_run:
            return

        stops_dfs: list[pd.DataFrame] = []

        if dataset == "viajes":
            for col, com_col, zone_col in [
                ("board_stop_code",  None, "zone_board"),
                ("alight_stop_code", None, "zone_alight"),
            ]:
                r = fetch_df(
                    self.conn,
                    f"""
                    SELECT DISTINCT
                        [{col}] AS stop_code,
                        NULL AS comuna,
                        CAST([{zone_col}] AS VARCHAR(20)) AS zone_code,
                        NULL AS x_utm,
                        NULL AS y_utm
                    FROM staging.stg_viajes_leg
                    WHERE [{col}] IS NOT NULL
                    """,
                )
                stops_dfs.append(r)

        elif dataset == "etapas":
            for stop_col, com_col, zone_col, x_col, y_col in [
                ("parada_subida", "comuna_subida", "zona_subida", "x_subida", "y_subida"),
                ("parada_bajada", "comuna_bajada", "zona_bajada", "x_bajada", "y_bajada"),
            ]:
                r = fetch_df(
                    self.conn,
                    f"""
                    SELECT DISTINCT
                        [{stop_col}] AS stop_code,
                        [{com_col}] AS comuna,
                        CAST([{zone_col}] AS VARCHAR(20)) AS zone_code,
                        [{x_col}] AS x_utm,
                        [{y_col}] AS y_utm
                    FROM staging.stg_etapas_validation
                    WHERE [{stop_col}] IS NOT NULL
                    """,
                )
                stops_dfs.append(r)

        elif dataset == "subidas_30m":
            r = fetch_df(
                self.conn,
                """
                SELECT DISTINCT
                    stop_code,
                    MAX(comuna) AS comuna,
                    NULL AS zone_code,
                    NULL AS x_utm,
                    NULL AS y_utm
                FROM staging.stg_subidas_30m
                WHERE stop_code IS NOT NULL
                GROUP BY stop_code
                """,
            )
            stops_dfs.append(r)

        if not stops_dfs:
            return

        all_stops = pd.concat(stops_dfs, ignore_index=True)
        # Agregar: para un mismo stop_code, tomar atributos más completos
        all_stops = (
            all_stops
            .groupby("stop_code", as_index=False)
            .agg({"comuna": "first", "zone_code": "first", "x_utm": "first", "y_utm": "first"})
        )
        all_stops = all_stops.astype(object).where(pd.notna(all_stops), None)

        scd2_upsert(
            self.conn,
            dim_table="dw.dim_stop",
            bk_col="stop_code",
            attr_cols=["stop_name", "stop_type", "comuna", "zone_code", "x_utm", "y_utm"],
            new_df=all_stops,
            event_date=event_date,
        )

    def upsert_dim_service(self, dataset: str, event_date: date) -> None:
        """SCD2 upsert para dim_service."""
        if self.dry_run:
            return

        if dataset == "viajes":
            r = fetch_df(
                self.conn,
                """
                SELECT DISTINCT service_code, mode_code
                FROM staging.stg_viajes_leg
                WHERE service_code IS NOT NULL
                """,
            )
        elif dataset == "etapas":
            r = fetch_df(
                self.conn,
                """
                SELECT s AS service_code, t AS mode_code FROM (
                    SELECT DISTINCT servicio_subida AS s, tipo_transporte AS t FROM staging.stg_etapas_validation WHERE servicio_subida IS NOT NULL
                    UNION
                    SELECT DISTINCT servicio_bajada, tipo_transporte FROM staging.stg_etapas_validation WHERE servicio_bajada IS NOT NULL
                ) x
                """,
            )
        else:
            return  # subidas_30m no tiene service_code

        if r.empty:
            return

        svc_df = (
            r.groupby("service_code", as_index=False)
            .agg({"mode_code": "first"})
        )
        # stop_name no disponible en Silver → NULL
        svc_df["service_name"] = None
        svc_df = svc_df.astype(object).where(pd.notna(svc_df), None)

        scd2_upsert(
            self.conn,
            dim_table="dw.dim_service",
            bk_col="service_code",
            attr_cols=["service_name", "mode_code"],
            new_df=svc_df,
            event_date=event_date,
        )

    # ── 7. Merge Facts ────────────────────────────────────────

    def _get_cut_sk(self, dataset: str, cut_id: str) -> int | None:
        return execute_sql_scalar(
            self.conn,
            "SELECT cut_sk FROM dw.dim_cut WHERE dataset_name = ? AND cut_id = ?",
            (dataset, cut_id),
        )

    def merge_fct_trip(self, partition: SilverPartition) -> int:
        """
        MERGE set-based staging.stg_viajes_trip → dw.fct_trip.
        Grain real: (cut_sk, id_tarjeta, id_viaje).
        id_viaje es un contador por tarjeta/día (1..27), NO un ID global.
        Filtra efectivo (id_tarjeta IS NULL) — sin BK único, se excluyen de facts.
        Idempotente vía UX_fct_trip_grain (filtrado WHERE id_tarjeta IS NOT NULL).
        Devuelve (filas_insertadas, filas_efectivo_excluidas).
        """
        if self.dry_run:
            log.info(
                "[DRY-RUN] merge_fct_trip %s/%s\n"
                "  MERGE dw.fct_trip ON (cut_sk, id_tarjeta, id_viaje) — grain correcto\n"
                "  Efectivo excluido | AS-OF join dim_stop vía valid_from/valid_to",
                partition.dataset, partition.cut,
            )
            return 0, 0

        cut_sk = self._get_cut_sk(partition.dataset, partition.cut)
        if cut_sk is None:
            log.error("No se encontró cut_sk para %s/%s — abortar merge_fct_trip", partition.dataset, partition.cut)
            return 0, 0

        # ── Preflight: diagnostico de grain antes del MERGE ────────────────
        total_rows   = execute_sql_scalar(self.conn, "SELECT COUNT(*) FROM staging.stg_viajes_trip") or 0
        cash_rows    = execute_sql_scalar(
            self.conn, "SELECT COUNT(*) FROM staging.stg_viajes_trip WHERE id_tarjeta IS NULL",
        ) or 0
        tarjeta_rows = total_rows - cash_rows
        dist_grain   = execute_sql_scalar(
            self.conn,
            "SELECT COUNT(*) FROM (SELECT DISTINCT id_tarjeta, id_viaje"
            " FROM staging.stg_viajes_trip WHERE id_tarjeta IS NOT NULL) g",
        ) or 0
        log.info(
            "preflight fct_trip | total=%d  con_tarjeta=%d  efectivo(excluidos)=%d"
            "  distinct_grain(id_tarjeta,id_viaje)=%d%s",
            total_rows, tarjeta_rows, cash_rows, dist_grain,
            "  [DEDUP rn=1 activo]" if tarjeta_rows > dist_grain else "",
        )

        sql = f"""
        -- Grain real: (cut_sk, id_tarjeta, id_viaje).
        -- id_viaje es contador por tarjeta/día (1..27), NO es un ID global.
        -- Viajes en efectivo (id_tarjeta IS NULL) se excluyen: no tienen BK único.
        -- Si Silver produce duplicados sobre el grain, conservamos el más reciente.
        WITH src_dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id_tarjeta, id_viaje
                    ORDER BY tiempo_inicio_viaje DESC
                ) AS _rn
            FROM staging.stg_viajes_trip
            WHERE id_tarjeta IS NOT NULL         -- excluir efectivo
        ),
        -- CTE 2: precomputa event_dt (DATE) desde date_start_sk (YYYYMMDD INT).
        -- DATEFROMPARTS evita conversión varchar; null-safe con CASE.
        src_prep AS (
            SELECT
                s.date_start_sk,
                s.time_start_30m_sk,
                s.date_end_sk,
                s.time_end_30m_sk,
                s.paradero_inicio_viaje,
                s.paradero_fin_viaje,
                s.periodo_inicio_viaje,
                s.periodo_fin_viaje,
                s.contrato,
                s.proposito,
                s.id_viaje,
                s.id_tarjeta,
                s.tipo_dia,
                s.zona_inicio_viaje,
                s.zona_fin_viaje,
                s.n_etapas,
                s.tviaje_min,
                s.distancia_eucl,
                s.distancia_ruta,
                s.factor_expansion,
                CASE WHEN s.date_start_sk IS NOT NULL
                     THEN DATEFROMPARTS(
                            s.date_start_sk / 10000,
                            (s.date_start_sk % 10000) / 100,
                             s.date_start_sk % 100)
                     ELSE NULL
                END AS event_dt
            FROM src_dedup s
            WHERE s._rn = 1
        )
        MERGE dw.fct_trip AS tgt
        USING (
            SELECT
                s.date_start_sk,
                CAST(s.time_start_30m_sk AS TINYINT)        AS time_start_30m_sk,
                s.date_end_sk,
                CAST(s.time_end_30m_sk   AS TINYINT)        AS time_end_30m_sk,
                origin_s.stop_sk                            AS origin_stop_sk,
                dest_s.stop_sk                              AS dest_stop_sk,
                fp_start.fare_period_sk                     AS fare_period_start_sk,
                fp_end.fare_period_sk                       AS fare_period_end_sk,
                opr.operator_sk,
                pur.purpose_sk,
                {cut_sk}                                    AS cut_sk,
                s.id_viaje,
                s.id_tarjeta,
                s.tipo_dia,
                CAST(s.zona_inicio_viaje AS VARCHAR(20))    AS zone_origin_txt,
                CAST(s.zona_fin_viaje    AS VARCHAR(20))    AS zone_dest_txt,
                CAST(s.n_etapas AS TINYINT)                 AS n_etapas,
                s.tviaje_min,
                s.distancia_eucl                            AS distancia_eucl_m,
                s.distancia_ruta                            AS distancia_ruta_m,
                s.factor_expansion
            FROM src_prep s
            -- AS-OF join: versión de stop válida el día de inicio del viaje
            LEFT JOIN dw.dim_stop origin_s
                   ON origin_s.stop_code  = s.paradero_inicio_viaje
                  AND s.event_dt          IS NOT NULL
                  AND origin_s.valid_from <= s.event_dt
                  AND (origin_s.valid_to  IS NULL OR s.event_dt <= origin_s.valid_to)
            LEFT JOIN dw.dim_stop dest_s
                   ON dest_s.stop_code    = s.paradero_fin_viaje
                  AND s.event_dt          IS NOT NULL
                  AND dest_s.valid_from   <= s.event_dt
                  AND (dest_s.valid_to    IS NULL OR s.event_dt <= dest_s.valid_to)
            LEFT JOIN dw.dim_fare_period fp_start ON fp_start.fare_period_name = s.periodo_inicio_viaje
            LEFT JOIN dw.dim_fare_period fp_end   ON fp_end.fare_period_name   = s.periodo_fin_viaje
            LEFT JOIN dw.dim_operator_contract opr ON opr.contract_code = s.contrato
            LEFT JOIN dw.dim_purpose pur           ON pur.purpose_name  = s.proposito
        ) AS src
        -- Grain correcto: id_viaje es contador por tarjeta/día (1..27), no global.
        ON  tgt.cut_sk     = src.cut_sk
        AND tgt.id_tarjeta = src.id_tarjeta
        AND tgt.id_viaje   = src.id_viaje
        WHEN NOT MATCHED THEN INSERT (
            date_start_sk, time_start_30m_sk, date_end_sk, time_end_30m_sk,
            origin_stop_sk, dest_stop_sk,
            fare_period_start_sk, fare_period_end_sk,
            operator_sk, purpose_sk, cut_sk,
            id_viaje, id_tarjeta, tipo_dia,
            zone_origin_txt, zone_dest_txt,
            n_etapas, tviaje_min, distancia_eucl_m, distancia_ruta_m, factor_expansion
        ) VALUES (
            src.date_start_sk, src.time_start_30m_sk, src.date_end_sk, src.time_end_30m_sk,
            src.origin_stop_sk, src.dest_stop_sk,
            src.fare_period_start_sk, src.fare_period_end_sk,
            src.operator_sk, src.purpose_sk, src.cut_sk,
            src.id_viaje, src.id_tarjeta, src.tipo_dia,
            src.zone_origin_txt, src.zone_dest_txt,
            src.n_etapas, src.tviaje_min, src.distancia_eucl_m, src.distancia_ruta_m, src.factor_expansion
        );
        """
        cursor = execute_sql(self.conn, sql, commit=True)
        n = cursor.rowcount
        cursor.close()
        log.info(
            "merge_fct_trip: %d insertadas (cut_sk=%d) | %d efectivo excluidos",
            n, cut_sk, cash_rows,
        )
        return n, cash_rows

    def merge_fct_trip_leg(self, partition: SilverPartition) -> int:
        """
        MERGE staging.stg_viajes_leg → dw.fct_trip_leg.
        Grain real: (cut_sk, id_tarjeta, id_viaje, leg_seq).
        Filtra efectivo y une a fct_trip por grain correcto (cut_sk, id_tarjeta, id_viaje).
        Devuelve (filas_insertadas, filas_efectivo_excluidas).
        """
        if self.dry_run:
            log.info(
                "[DRY-RUN] merge_fct_trip_leg %s/%s\n"
                "  MERGE dw.fct_trip_leg ON (cut_sk, id_tarjeta, id_viaje, leg_seq) — grain correcto\n"
                "  Efectivo excluido | AS-OF join dim_stop y dim_service vía valid_from/valid_to",
                partition.dataset, partition.cut,
            )
            return 0, 0

        cut_sk = self._get_cut_sk(partition.dataset, partition.cut)
        if cut_sk is None:
            return 0, 0

        # ── Preflight: diagnostico de grain antes del MERGE ────────────────
        total_leg  = execute_sql_scalar(self.conn, "SELECT COUNT(*) FROM staging.stg_viajes_leg") or 0
        cash_leg   = execute_sql_scalar(
            self.conn, "SELECT COUNT(*) FROM staging.stg_viajes_leg WHERE id_tarjeta IS NULL",
        ) or 0
        tarjeta_leg = total_leg - cash_leg
        dist_leg   = execute_sql_scalar(
            self.conn,
            "SELECT COUNT(*) FROM (SELECT DISTINCT id_tarjeta, id_viaje, leg_seq"
            " FROM staging.stg_viajes_leg WHERE id_tarjeta IS NOT NULL) g",
        ) or 0
        log.info(
            "preflight fct_trip_leg | total=%d  con_tarjeta=%d  efectivo(excluidos)=%d"
            "  distinct_grain(id_tarjeta,id_viaje,leg_seq)=%d%s",
            total_leg, tarjeta_leg, cash_leg, dist_leg,
            "  [DEDUP rn=1 activo]" if tarjeta_leg > dist_leg else "",
        )

        sql = f"""
        -- Grain real: (cut_sk, id_tarjeta, id_viaje, leg_seq).
        -- Efectivo (id_tarjeta IS NULL) excluido — sin BK único.
        -- Si Silver produce duplicados, conservamos el embarque más reciente.
        WITH src_dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id_tarjeta, id_viaje, leg_seq
                    ORDER BY ts_board DESC
                ) AS _rn
            FROM staging.stg_viajes_leg
            WHERE id_tarjeta IS NOT NULL         -- excluir efectivo
        ),
        -- CTE 2: date_board_sk → event_dt DATE para as-of joins SCD2
        src_prep AS (
            SELECT
                s.id_viaje,
                s.id_tarjeta,
                s.leg_seq,
                s.date_board_sk,
                s.time_board_30m_sk,
                s.date_alight_sk,
                s.time_alight_30m_sk,
                s.board_stop_code,
                s.alight_stop_code,
                s.mode_code,
                s.service_code,
                s.operator_code,
                s.fare_period_alight_code,
                s.zone_board,
                s.zone_alight,
                s.ts_board,
                s.ts_alight,
                s.tv_leg_min,
                s.tc_transfer_min,
                s.te_wait_min,
                CASE WHEN s.date_board_sk IS NOT NULL
                     THEN DATEFROMPARTS(
                            s.date_board_sk / 10000,
                            (s.date_board_sk % 10000) / 100,
                             s.date_board_sk % 100)
                     ELSE NULL
                END AS event_dt
            FROM src_dedup s
            WHERE s._rn = 1
        )
        MERGE dw.fct_trip_leg AS tgt
        USING (
            SELECT
                ft.trip_sk,
                s.id_viaje,
                CAST(s.leg_seq AS TINYINT)                      AS leg_seq,
                {cut_sk}                                        AS cut_sk,
                s.date_board_sk,
                CAST(s.time_board_30m_sk  AS TINYINT)           AS time_board_30m_sk,
                s.date_alight_sk,
                CAST(s.time_alight_30m_sk AS TINYINT)           AS time_alight_30m_sk,
                board_s.stop_sk                                 AS board_stop_sk,
                alight_s.stop_sk                                AS alight_stop_sk,
                dm.mode_sk,
                svc.service_sk,
                opr.operator_sk,
                fp.fare_period_sk                               AS fare_period_alight_sk,
                s.id_tarjeta,
                CAST(s.zone_board  AS VARCHAR(20))              AS zone_board_txt,
                CAST(s.zone_alight AS VARCHAR(20))              AS zone_alight_txt,
                s.ts_board,
                s.ts_alight,
                s.tv_leg_min,
                s.tc_transfer_min,
                s.te_wait_min
            FROM src_prep s
            -- FK a fct_trip: grain correcto incluye id_tarjeta
            LEFT JOIN dw.fct_trip            ft
                   ON ft.id_tarjeta = s.id_tarjeta AND ft.id_viaje = s.id_viaje AND ft.cut_sk = {cut_sk}
            -- AS-OF stop: versión válida al día del embarque
            LEFT JOIN dw.dim_stop board_s
                   ON board_s.stop_code  = s.board_stop_code
                  AND s.event_dt         IS NOT NULL
                  AND board_s.valid_from <= s.event_dt
                  AND (board_s.valid_to  IS NULL OR s.event_dt <= board_s.valid_to)
            LEFT JOIN dw.dim_stop alight_s
                   ON alight_s.stop_code  = s.alight_stop_code
                  AND s.event_dt          IS NOT NULL
                  AND alight_s.valid_from <= s.event_dt
                  AND (alight_s.valid_to  IS NULL OR s.event_dt <= alight_s.valid_to)
            LEFT JOIN dw.dim_mode dm
                   ON dm.mode_code = s.mode_code
            -- AS-OF service: versión válida al día del embarque
            LEFT JOIN dw.dim_service svc
                   ON svc.service_code  = s.service_code
                  AND s.event_dt        IS NOT NULL
                  AND svc.valid_from   <= s.event_dt
                  AND (svc.valid_to    IS NULL OR s.event_dt <= svc.valid_to)
            LEFT JOIN dw.dim_operator_contract opr
                   ON opr.contract_code = s.operator_code
            LEFT JOIN dw.dim_fare_period fp
                   ON fp.fare_period_name = s.fare_period_alight_code
        ) AS src
        -- Grain correcto: id_viaje es contador por tarjeta/día, no global.
        ON  tgt.cut_sk     = src.cut_sk
        AND tgt.id_tarjeta = src.id_tarjeta
        AND tgt.id_viaje   = src.id_viaje
        AND tgt.leg_seq    = src.leg_seq
        WHEN NOT MATCHED THEN INSERT (
            trip_sk, id_viaje, leg_seq, cut_sk,
            date_board_sk, time_board_30m_sk, date_alight_sk, time_alight_30m_sk,
            board_stop_sk, alight_stop_sk, mode_sk, service_sk, operator_sk, fare_period_alight_sk,
            id_tarjeta, zone_board_txt, zone_alight_txt, ts_board, ts_alight,
            tv_leg_min, tc_transfer_min, te_wait_min
        ) VALUES (
            src.trip_sk, src.id_viaje, src.leg_seq, src.cut_sk,
            src.date_board_sk, src.time_board_30m_sk, src.date_alight_sk, src.time_alight_30m_sk,
            src.board_stop_sk, src.alight_stop_sk, src.mode_sk, src.service_sk, src.operator_sk, src.fare_period_alight_sk,
            src.id_tarjeta, src.zone_board_txt, src.zone_alight_txt, src.ts_board, src.ts_alight,
            src.tv_leg_min, src.tc_transfer_min, src.te_wait_min
        );
        """
        cursor = execute_sql(self.conn, sql, commit=True)
        n = cursor.rowcount
        cursor.close()
        log.info(
            "merge_fct_trip_leg: %d insertadas (cut_sk=%d) | %d efectivo excluidos",
            n, cut_sk, cash_leg,
        )
        return n, cash_leg

    def merge_fct_validation(self, partition: SilverPartition) -> int:
        """
        MERGE staging.stg_etapas_validation → dw.fct_validation.
        Grain: (id_etapa, cut_sk).

        As-of join:
          event_date = date_board_sk (embarque de la etapa).
          Resuelve board/alight stop_sk y service board/alight SK con versión SCD2
          válida el día de la etapa.
        Devuelve filas insertadas.
        """
        if self.dry_run:
            log.info(
                "[DRY-RUN] merge_fct_validation %s/%s\n"
                "  MERGE dw.fct_validation ON (id_etapa, cut_sk)\n"
                "  AS-OF join dim_stop y dim_service vía valid_from/valid_to",
                partition.dataset, partition.cut,
            )
            return 0

        cut_sk = self._get_cut_sk(partition.dataset, partition.cut)
        if cut_sk is None:
            return 0

        sql = f"""
        -- CTE 1: dedup por grain (id_etapa) antes de cualquier join.
        WITH src_dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id_etapa ORDER BY (SELECT NULL)) AS _rn
            FROM staging.stg_etapas_validation
        ),
        -- CTE 2: date_board_sk → event_dt DATE para as-of join SCD2
        src_prep AS (
            SELECT
                s.id_etapa,
                s.date_board_sk,
                s.time_board_30m_sk,
                s.date_alight_sk,
                s.time_alight_30m_sk,
                s.parada_subida,
                s.parada_bajada,
                s.tipo_transporte,
                s.servicio_subida,
                s.servicio_bajada,
                s.contrato,
                s.operador,
                s.periodoSubida,
                s.periodoBajada,
                s.tipo_dia,
                s.tiene_bajada,
                s.tiempo_bajada,
                s.tiempo_etapa,
                s.tEsperaMediaIntervalo,
                s.dist_ruta_paraderos,
                s.dist_eucl_paraderos,
                s.x_subida, s.y_subida, s.x_bajada, s.y_bajada,
                s.fExpansionServicioPeriodoTS,
                CASE WHEN s.date_board_sk IS NOT NULL
                     THEN DATEFROMPARTS(
                            s.date_board_sk / 10000,
                            (s.date_board_sk % 10000) / 100,
                             s.date_board_sk % 100)
                     ELSE NULL
                END AS event_dt
            FROM src_dedup s
            WHERE s._rn = 1
        )
        MERGE dw.fct_validation AS tgt
        USING (
            SELECT
                s.id_etapa,
                {cut_sk}                                        AS cut_sk,
                s.date_board_sk,
                CAST(s.time_board_30m_sk  AS TINYINT)          AS time_board_30m_sk,
                s.date_alight_sk,
                CAST(s.time_alight_30m_sk AS TINYINT)          AS time_alight_30m_sk,
                board_s.stop_sk                                 AS board_stop_sk,
                alight_s.stop_sk                               AS alight_stop_sk,
                dm.mode_sk,
                svc_b.service_sk                               AS service_board_sk,
                svc_a.service_sk                               AS service_alight_sk,
                opr.operator_sk,
                fp_b.fare_period_sk                            AS fare_period_board_sk,
                fp_a.fare_period_sk                            AS fare_period_alight_sk,
                s.tipo_dia,
                s.tiene_bajada,
                s.tiempo_bajada,
                s.tiempo_etapa                                 AS tiempo_etapa_sec,
                s.tEsperaMediaIntervalo                        AS t_espera_media_min,
                s.dist_ruta_paraderos                          AS dist_ruta_m,
                s.dist_eucl_paraderos                          AS dist_eucl_m,
                s.x_subida, s.y_subida, s.x_bajada, s.y_bajada,
                s.fExpansionServicioPeriodoTS                  AS fexp_servicio
            FROM src_prep s
            -- AS-OF stop: versión válida el día del embarque de la etapa
            LEFT JOIN dw.dim_stop board_s
                   ON board_s.stop_code  = s.parada_subida
                  AND s.event_dt         IS NOT NULL
                  AND board_s.valid_from <= s.event_dt
                  AND (board_s.valid_to  IS NULL OR s.event_dt <= board_s.valid_to)
            LEFT JOIN dw.dim_stop alight_s
                   ON alight_s.stop_code  = s.parada_bajada
                  AND s.event_dt          IS NOT NULL
                  AND alight_s.valid_from <= s.event_dt
                  AND (alight_s.valid_to  IS NULL OR s.event_dt <= alight_s.valid_to)
            LEFT JOIN dw.dim_mode dm
                   ON dm.mode_code = s.tipo_transporte
            -- AS-OF service (subida y bajada)
            LEFT JOIN dw.dim_service svc_b
                   ON svc_b.service_code = s.servicio_subida
                  AND s.event_dt         IS NOT NULL
                  AND svc_b.valid_from  <= s.event_dt
                  AND (svc_b.valid_to   IS NULL OR s.event_dt <= svc_b.valid_to)
            LEFT JOIN dw.dim_service svc_a
                   ON svc_a.service_code = s.servicio_bajada
                  AND s.event_dt         IS NOT NULL
                  AND svc_a.valid_from  <= s.event_dt
                  AND (svc_a.valid_to   IS NULL OR s.event_dt <= svc_a.valid_to)
            LEFT JOIN dw.dim_operator_contract opr
                   ON opr.contract_code = COALESCE(s.contrato, s.operador)
            LEFT JOIN dw.dim_fare_period fp_b ON fp_b.fare_period_name = s.periodoSubida
            LEFT JOIN dw.dim_fare_period fp_a ON fp_a.fare_period_name = s.periodoBajada
        ) AS src
        ON tgt.id_etapa = src.id_etapa AND tgt.cut_sk = src.cut_sk
        WHEN NOT MATCHED THEN INSERT (
            id_etapa, cut_sk,
            date_board_sk, time_board_30m_sk, date_alight_sk, time_alight_30m_sk,
            board_stop_sk, alight_stop_sk, mode_sk,
            service_board_sk, service_alight_sk,
            operator_sk, fare_period_board_sk, fare_period_alight_sk,
            tipo_dia, tiene_bajada, tiempo_bajada, tiempo_etapa_sec,
            t_espera_media_min, dist_ruta_m, dist_eucl_m,
            x_subida, y_subida, x_bajada, y_bajada, fexp_servicio
        ) VALUES (
            src.id_etapa, src.cut_sk,
            src.date_board_sk, src.time_board_30m_sk, src.date_alight_sk, src.time_alight_30m_sk,
            src.board_stop_sk, src.alight_stop_sk, src.mode_sk,
            src.service_board_sk, src.service_alight_sk,
            src.operator_sk, src.fare_period_board_sk, src.fare_period_alight_sk,
            src.tipo_dia, src.tiene_bajada, src.tiempo_bajada, src.tiempo_etapa_sec,
            src.t_espera_media_min, src.dist_ruta_m, src.dist_eucl_m,
            src.x_subida, src.y_subida, src.x_bajada, src.y_bajada, src.fexp_servicio
        );
        """
        cursor = execute_sql(self.conn, sql, commit=True)
        n = cursor.rowcount
        cursor.close()
        log.info("merge_fct_validation: %d filas insertadas (cut_sk=%d)", n, cut_sk)
        return n

    def merge_fct_boardings_30m(self, partition: SilverPartition) -> int:
        """
        MERGE staging.stg_subidas_30m → dw.fct_boardings_30m.
        Grain: (month_date_sk, time_30m_sk, stop_sk, mode_sk, tipo_dia, cut_sk).

        As-of join:
          event_date = primer día del mes del corte (YYYYMM01 → DATE).
          Resuelve stop_sk con la versión SCD2 de dim_stop válida ese día.
          Filas donde stop_sk o mode_sk no se resuelven quedan excluidas
          (filtro WHERE en USING — el grano las necesita para ser no-nulo).
        Devuelve filas insertadas.
        """
        if self.dry_run:
            log.info(
                "[DRY-RUN] merge_fct_boardings_30m %s/%s\n"
                "  MERGE dw.fct_boardings_30m ON (month_date_sk, time_30m_sk, stop_sk, mode_sk, tipo_dia, cut_sk)\n"
                "  AS-OF join dim_stop vía valid_from/valid_to",
                partition.dataset, partition.cut,
            )
            return 0

        cut_sk = self._get_cut_sk(partition.dataset, partition.cut)
        if cut_sk is None:
            return 0

        # month_date_sk = YYYYMM01 (grain de subidas_30m)
        month_date_sk = partition.year * 10000 + partition.month * 100 + 1
        # event_dt = DATE del primer día del mes
        event_dt = f"DATEFROMPARTS({partition.year}, {partition.month}, 1)"

        sql = f"""
        MERGE dw.fct_boardings_30m AS tgt
        USING (
            SELECT
                {month_date_sk}                                 AS month_date_sk,
                CAST(s.time_30m_sk AS TINYINT)                  AS time_30m_sk,
                ds.stop_sk,
                dm.mode_sk,
                {cut_sk}                                        AS cut_sk,
                s.tipo_dia,
                s.comuna                                        AS comuna_txt,
                s.subidas_promedio
            FROM (
                -- dedup por grain natural de subidas_30m (stop_code, time_30m_sk, mode_code, tipo_dia)
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY stop_code, time_30m_sk, mode_code, tipo_dia
                    ORDER BY (SELECT NULL)
                ) AS _rn
                FROM staging.stg_subidas_30m
            ) s
            WHERE s._rn = 1
            -- AS-OF stop: versión de dim_stop válida al primer día del mes del corte
            LEFT JOIN dw.dim_stop ds
                   ON ds.stop_code  = s.stop_code
                  AND ds.valid_from <= {event_dt}
                  AND (ds.valid_to  IS NULL OR {event_dt} <= ds.valid_to)
            LEFT JOIN dw.dim_mode dm ON dm.mode_code = s.mode_code
            -- Excluir filas donde no se puede resolver stop_sk o mode_sk
            -- (ambos forman parte del grain y no pueden ser NULL en la fact)
            WHERE ds.stop_sk IS NOT NULL AND dm.mode_sk IS NOT NULL
        ) AS src
        ON  tgt.month_date_sk = src.month_date_sk
        AND tgt.time_30m_sk   = src.time_30m_sk
        AND tgt.stop_sk       = src.stop_sk
        AND tgt.mode_sk       = src.mode_sk
        AND tgt.tipo_dia      = src.tipo_dia
        AND tgt.cut_sk        = src.cut_sk
        WHEN NOT MATCHED THEN INSERT (
            month_date_sk, time_30m_sk, stop_sk, mode_sk, cut_sk,
            tipo_dia, comuna_txt, subidas_promedio
        ) VALUES (
            src.month_date_sk, src.time_30m_sk, src.stop_sk, src.mode_sk, src.cut_sk,
            src.tipo_dia, src.comuna_txt, src.subidas_promedio
        );
        """
        cursor = execute_sql(self.conn, sql, commit=True)
        n = cursor.rowcount
        cursor.close()
        log.info("merge_fct_boardings_30m: %d filas insertadas (cut_sk=%d)", n, cut_sk)
        return n

    # ── 8. Colectar date_sks desde staging para dim_date ─────

    def _collect_date_sks_from_staging(self, dataset: str) -> list[int]:
        """
        Recoge MIN/MAX date_sks desde staging para generar el rango en dim_date.
        Usa MIN/MAX en vez de DISTINCT para evitar SELECT DISTINCT masivos.
        """
        sks: list[int] = []
        try:
            if dataset == "viajes":
                for tbl, cols in [
                    ("staging.stg_viajes_trip", ["date_start_sk", "date_end_sk"]),
                    ("staging.stg_viajes_leg",  ["date_board_sk", "date_alight_sk"]),
                ]:
                    for col in cols:
                        r = fetch_df(self.conn,
                            f"SELECT MIN([{col}]), MAX([{col}]) FROM {tbl} WHERE [{col}] IS NOT NULL")
                        if not r.empty and r.iloc[0, 0] is not None:
                            sks += [int(r.iloc[0, 0]), int(r.iloc[0, 1])]
            elif dataset == "etapas":
                for col in ("date_board_sk", "date_alight_sk"):
                    r = fetch_df(self.conn,
                        f"SELECT MIN([{col}]), MAX([{col}]) "
                        f"FROM staging.stg_etapas_validation WHERE [{col}] IS NOT NULL")
                    if not r.empty and r.iloc[0, 0] is not None:
                        sks += [int(r.iloc[0, 0]), int(r.iloc[0, 1])]
            # subidas_30m: no tiene date_sk con grano diario, se maneja por month_date_sk
        except Exception as e:
            log.warning("Error recolectando date_sks: %s", e)
        return [s for s in sks if s]

    # ── 9. etl_run_log helpers ────────────────────────────────

    def _run_log_start(self, dataset: str, cut: str) -> int | None:
        """Inserta un registro 'RUNNING' en dw.etl_run_log. Devuelve run_id."""
        try:
            # Must fetchone() BEFORE commit; committing invalidates the OUTPUT cursor.
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO dw.etl_run_log (dataset, cut, status, loader_version)
                OUTPUT INSERTED.run_id
                VALUES (?, ?, 'RUNNING', ?)
                """,
                (dataset, cut, LOADER_VERSION),
            )
            row = cursor.fetchone()
            self.conn.commit()
            cursor.close()
            return int(row[0]) if row else None
        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            log.warning("etl_run_log INSERT fallo (no critico): %s", exc)
            return None

    def _run_log_finish(
        self,
        run_id: int | None,
        status: str,
        rows_staged: int,
        rows_inserted: int,
        error_message: str | None = None,
        ignored_cash_rows: int | None = None,
    ) -> None:
        """Actualiza el registro de etl_run_log con resultado final."""
        if run_id is None:
            return
        try:
            execute_sql(
                self.conn,
                """
                UPDATE dw.etl_run_log
                SET finished_at       = SYSUTCDATETIME(),
                    status            = ?,
                    rows_staged       = ?,
                    rows_inserted     = ?,
                    error_message     = ?,
                    ignored_cash_rows = ?
                WHERE run_id = ?
                """,
                (status, rows_staged, rows_inserted, error_message, ignored_cash_rows, run_id),
                commit=True,
            )
        except Exception as exc:
            log.warning("etl_run_log UPDATE falló (no crítico): %s", exc)

    # ── 10. Validaciones pre-run ──────────────────────────────

    def _validate_staging_non_empty(self, dataset: str) -> bool:
        """
        Verifica que el staging tenga filas después de la carga.
        Si está vacío muestra WARNING pero NO aborta (podría ser un
        corte válidamente vacío tras filtros Silver).
        """
        tables = {
            "viajes":      ["staging.stg_viajes_trip", "staging.stg_viajes_leg"],
            "etapas":      ["staging.stg_etapas_validation"],
            "subidas_30m": ["staging.stg_subidas_30m"],
        }
        for tbl in tables.get(dataset, []):
            try:
                n = execute_sql_scalar(self.conn, f"SELECT COUNT(*) FROM {tbl}")
                if not n or n == 0:
                    log.warning(
                        "VALIDACIÓN: %s está vacía tras la carga de staging para dataset=%s. "
                        "Continúa, pero verifica el Parquet fuente.",
                        tbl, dataset,
                    )
                else:
                    log.info("VALIDACIÓN: %s tiene %d filas en staging.", tbl, n)
            except Exception as exc:
                log.warning("VALIDACIÓN: error leyendo %s — %s", tbl, exc)
        return True

    # ── 11. run() principal ───────────────────────────────────

    def run(self, partitions: list[SilverPartition]) -> int:
        """
        Procesa todas las particiones entregadas.
        Retorna número de particiones que fallaron.

        Flujo por partición:
          0. etl_run_log INSERT (status=RUNNING)
          a. Staging (bulk load desde Parquet)
          b. dim_cut upsert
          c. dim_date ensure
          d. Dims simples upsert
          e. SCD2 dims (dim_stop, dim_service)
          f. Facts MERGE (as-of join SCD2)
          z. etl_run_log UPDATE (status=OK|FAILED)
        """
        self.ensure_schema()
        self.load_static_dims()

        failed  = 0
        total   = len(partitions)

        for i, part in enumerate(partitions, 1):
            t_total = time.monotonic()
            log.info(
                "[%d/%d] ▶ dataset=%s  cut=%s  year=%d  month=%d",
                i, total, part.dataset, part.cut, part.year, part.month,
            )

            run_id: int | None = None
            rows_staged       = 0
            rows_inserted     = 0
            ignored_cash_rows = 0

            try:
                # 0. Determinar event_date para SCD2
                try:
                    event_date = date.fromisoformat(part.cut[:10])
                except ValueError:
                    event_date = date(part.year, part.month, 1)

                # 0. etl_run_log
                run_id = self._run_log_start(part.dataset, part.cut)

                # ── a. Staging ──────────────────────────────────────
                t0 = time.monotonic()
                rows_staged = self.load_staging(part)
                log.info("  [a] staging: %d filas en %.1fs", rows_staged, time.monotonic() - t0)

                # Validación post-staging (warning si vacío, no aborta)
                if not self.dry_run:
                    self._validate_staging_non_empty(part.dataset)

                # ── b. dim_cut ─────────────────────────────────────
                t0 = time.monotonic()
                self.upsert_dim_cut(part)
                log.info("  [b] dim_cut: %.1fs", time.monotonic() - t0)

                if not self.dry_run:
                    # ── c. dim_date ────────────────────────────────
                    t0 = time.monotonic()
                    if part.dataset == "subidas_30m":
                        month_sk = part.year * 10000 + part.month * 100 + 1
                        self._ensure_dim_date([month_sk])
                    else:
                        date_sks = self._collect_date_sks_from_staging(part.dataset)
                        self._ensure_dim_date(date_sks)
                    log.info("  [c] dim_date: %.1fs", time.monotonic() - t0)

                    # ── d. Dims simples ────────────────────────────
                    t0 = time.monotonic()
                    self.upsert_simple_dims(part.dataset)
                    log.info("  [d] dims simples: %.1fs", time.monotonic() - t0)

                    # ── e. SCD2 dims ───────────────────────────────
                    t0 = time.monotonic()
                    self.upsert_dim_stop(part.dataset, event_date)
                    self.upsert_dim_service(part.dataset, event_date)
                    log.info("  [e] SCD2 dims: %.1fs", time.monotonic() - t0)

                # ── f. Facts ───────────────────────────────────────
                t0 = time.monotonic()
                if part.dataset == "viajes":
                    n1, cash1 = self.merge_fct_trip(part)
                    n2, _     = self.merge_fct_trip_leg(part)
                    rows_inserted     = n1 + n2
                    ignored_cash_rows = cash1
                elif part.dataset == "etapas":
                    rows_inserted = self.merge_fct_validation(part)
                elif part.dataset == "subidas_30m":
                    rows_inserted = self.merge_fct_boardings_30m(part)
                log.info("  [f] facts MERGE: %d filas en %.1fs", rows_inserted, time.monotonic() - t0)

                elapsed = time.monotonic() - t_total
                log.info(
                    "  ✔ DONE  dataset=%s  cut=%s  staged=%d  inserted=%d  total=%.1fs",
                    part.dataset, part.cut, rows_staged, rows_inserted, elapsed,
                )
                self._run_log_finish(run_id, "OK", rows_staged, rows_inserted, ignored_cash_rows=ignored_cash_rows)

            except Exception as exc:  # noqa: BLE001
                elapsed = time.monotonic() - t_total
                log.exception(
                    "  ✘ FAIL  dataset=%s  cut=%s  elapsed=%.1fs — %s",
                    part.dataset, part.cut, elapsed, exc,
                )
                self._run_log_finish(run_id, "FAILED", rows_staged, rows_inserted, str(exc)[:2000], ignored_cash_rows=ignored_cash_rows)
                failed += 1

        log.info(
            "Gold load finalizado | total=%d  failed=%d  ok=%d",
            total, failed, total - failed,
        )
        return failed


# ─────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.gold.load_gold",
        description="Carga la capa Gold (SQL Server DW) desde Silver Parquet.",
    )
    p.add_argument(
        "--dataset",
        default="all",
        choices=list(SUPPORTED_DATASETS) + ["all"],
        help="Dataset a cargar. 'all' procesa los tres. (default: all)",
    )
    p.add_argument(
        "--cut",
        default=None,
        help="Cut específico a cargar, ej. '2025-04-21'. Combina con --dataset.",
    )
    p.add_argument(
        "--overwrite-staging",
        dest="overwrite_staging",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Truncar staging antes de cargar cada partición (default: sí). "
            "Usar --no-overwrite-staging para re-ejecutar solo dims/facts "
            "asumiendo que staging ya está cargado."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Imprime el plan (particiones, queries) sin escribir nada en SQL Server.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging. (default: INFO)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    setup_logging(args.log_level)

    partitions = discover_partitions(
        cut_filter=args.cut,
        dataset_filter=args.dataset,
    )

    if not partitions:
        log.warning("No se encontraron particiones Silver para los filtros dados.")
        return 0

    log.info(
        "Particiones a procesar: %d | dry_run=%s | dataset=%s | cut=%s | overwrite_staging=%s",
        len(partitions), args.dry_run, args.dataset, args.cut, args.overwrite_staging,
    )

    if args.dry_run:
        for p in partitions:
            log.info(
                "  [PLAN] %s / %s | archivos=%s | overwrite_staging=%s",
                p.dataset, p.cut, list(p.parquet_files.keys()), args.overwrite_staging,
            )
            # Mostrar queries que se ejecutarían (MERGE summary)
            log.info(
                "  [PLAN]   -> MERGE dw.fct_%s ON grain (%s, cut_sk) -- AS-OF join dim_stop/dim_service",
                {"viajes": "trip + trip_leg", "etapas": "validation", "subidas_30m": "boardings_30m"}.get(p.dataset, "?"),
                {"viajes": "id_viaje", "etapas": "id_etapa", "subidas_30m": "month_date_sk+time_30m_sk+stop_sk+mode_sk+tipo_dia"}.get(p.dataset, "?"),
            )
        return 0

    conn = get_connection()
    try:
        loader = GoldLoader(
            conn=conn,
            dry_run=args.dry_run,
            overwrite_staging=args.overwrite_staging,
        )
        failed = loader.run(partitions)
    finally:
        conn.close()
        log.info("Conexión SQL Server cerrada.")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

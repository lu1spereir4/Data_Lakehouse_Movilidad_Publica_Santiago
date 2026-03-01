"""
transforms.py — Lógica DuckDB para la capa Silver DTPM.

Responsabilidades:
  - Leer CSV RAW con DuckDB (sin pandas, sin ignore_errors)
  - All-VARCHAR read con columns= explícito derivado de _meta.json
  - Transformar / normalizar / mapear códigos
  - Exportar Parquet con COPY … ZSTD (escritura atómica tmp→rename)
  - Generar quality.json con read_row_count, assertion y DuckDB version
  - Generar valid.parquet + invalid.parquet (quarantine) con reason_code
  - Validar muestra con Pydantic v2 (configurable warn/fail rate)
  - Soporte --overwrite (limpia dirs antes de escribir)

Todos los "grandes" queries se ejecutan en DuckDB puro.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from src.silver.catalog import PartitionInfo
from src.silver.contracts import (
    EtapasValidationRow,
    PYDANTIC_FAIL_RATE,
    PYDANTIC_WARN_RATE,
    Subidas30mRow,
    ViajesLegRow,
    ViajesTripRow,
)

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Configuración de mappings de códigos
# ─────────────────────────────────────────────────────────────
TIPODIA_MAP: dict[int, str] = {
    0: "LABORAL",
    1: "SABADO",
    2: "DOMINGO",
}

MODE_MAP: dict[int, str] = {
    1: "BUS",
    2: "METRO",
    3: "METROTREN",
    4: "ZP",
}

SAMPLE_ROWS = 10_000  # filas para validación Pydantic

# ─────────────────────────────────────────────────────────────
# Helpers SQL reutilizables
# ─────────────────────────────────────────────────────────────

def _tipodia_case(col: str) -> str:
    """Genera CASE SQL para mapear tipodia int -> string."""
    branches = "\n        ".join(
        f"WHEN {k} THEN '{v}'" for k, v in TIPODIA_MAP.items()
    )
    return f"CASE {col}\n        {branches}\n        ELSE 'UNKNOWN'\n    END"


def _mode_case(col: str) -> str:
    """Genera CASE SQL para mapear tipo_transporte int -> mode_code."""
    branches = "\n        ".join(
        f"WHEN {k} THEN '{v}'" for k, v in MODE_MAP.items()
    )
    return f"CASE {col}\n        {branches}\n        ELSE 'UNKNOWN'\n    END"


def _ts_to_date_sk(col: str) -> str:
    """Convierte timestamp a int YYYYMMDD."""
    return f"CAST(strftime({col}, '%Y%m%d') AS INTEGER)"


def _ts_to_time_30m_sk(col: str) -> str:
    """Convierte timestamp a time_30m_sk (0-47)."""
    return (
        f"(DATEPART('hour', {col}) * 2 + "
        f"CASE WHEN DATEPART('minute', {col}) >= 30 THEN 1 ELSE 0 END)"
    )


def _excel_fraction_to_time_30m_sk(col: str) -> str:
    """
    Media_hora en subidas_30m es fracción del día estilo Excel (0..1)
    -> time_30m_sk (0..47).
    """
    return f"CAST(FLOOR({col} * 48) AS INTEGER)"


def _excel_fraction_to_time(col: str) -> str:
    """Fracción del día -> TIME HH:MM:00."""
    return (
        f"MAKE_TIME("
        f"  CAST(FLOOR({col} * 24) AS INTEGER),"
        f"  CAST(FLOOR(({col} * 24 - FLOOR({col} * 24)) * 60) AS INTEGER),"
        f"  0"
        f")"
    )


def _build_varchar_read(csv_path: Path, col_spec: str) -> str:
    """
    Genera cláusula read_csv con all-VARCHAR explícito.

    col_spec es la cadena devuelta por PartitionInfo.columns_sql_spec(),
    e.g.: {'col1': 'VARCHAR', 'col2': 'VARCHAR', ...}

    No usamos ignore_errors; cualquier error de parseo lanzará excepción.
    """
    p = str(csv_path).replace("\\", "/")
    return (
        f"read_csv('{p}', "
        f"delim='|', header=True, encoding='utf-8', "
        f"nullstr=['-'], "
        f"columns={col_spec})"
    )


# ─────────────────────────────────────────────────────────────
# Escritura atómica: tmp → rename
# ─────────────────────────────────────────────────────────────

def _write_parquet_atomic(
    con: duckdb.DuckDBPyConnection, query: str, dest: Path
) -> None:
    """
    Escribe el resultado de `query` como Parquet ZSTD en `dest`.
    Usa patrón atómico: escribe a un temporal, luego shutil.move().
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f"._tmp_{uuid4().hex}_{dest.name}"
    tmp_str = str(tmp).replace("\\", "/")
    sql = f"COPY ({query}) TO '{tmp_str}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    log.debug("COPY (tmp) -> %s", tmp)
    try:
        con.execute(sql)
        shutil.move(str(tmp), str(dest))
        log.debug("Atomic rename -> %s", dest)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _duckdb_con() -> duckdb.DuckDBPyConnection:
    """Conexión in-process con configuración óptima."""
    con = duckdb.connect(database=":memory:")
    threads = os.cpu_count() or 4
    con.execute(f"SET threads TO {threads}")
    con.execute("SET memory_limit = '6GB'")
    return con


# ─────────────────────────────────────────────────────────────
# Idempotencia: limpiar dirs existentes
# ─────────────────────────────────────────────────────────────

def _clear_partition_dirs(partition: PartitionInfo) -> None:
    """Elimina silver + quality + quarantine dirs si existen (--overwrite)."""
    for d in [
        partition.silver_output_dir(),
        partition.quality_output_dir(),
        partition.quarantine_output_dir(),
    ]:
        if d.exists():
            shutil.rmtree(d)
            log.info("Removed (overwrite) %s", d)


# ─────────────────────────────────────────────────────────────
# Audit helpers: DuckDB version + git hash
# ─────────────────────────────────────────────────────────────

def _git_hash() -> str:
    """Devuelve el short hash del HEAD actual (o 'unknown')."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ─────────────────────────────────────────────────────────────
# Quality helpers
# ─────────────────────────────────────────────────────────────

def _write_quality(stats: dict[str, Any], dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / "quality.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, ensure_ascii=False, indent=2)
    log.info("Quality report -> %s", path)


# ─────────────────────────────────────────────────────────────
# Pydantic sample validation
# ─────────────────────────────────────────────────────────────

def _validate_sample(
    con: duckdb.DuckDBPyConnection,
    view: str,
    model_cls: type,
    n: int = SAMPLE_ROWS,
    warn_rate: float = PYDANTIC_WARN_RATE,
    fail_rate: float = PYDANTIC_FAIL_RATE,
) -> dict[str, Any]:
    """
    Toma una muestra de n filas del view y las valida con Pydantic v2.
    Devuelve estadísticas de validación.
    Lanza RuntimeError si error_rate > fail_rate.
    """
    rows = con.execute(
        f"SELECT * FROM {view} USING SAMPLE {n} ROWS"
    ).fetchdf()

    # pandas representa NULLs de columnas float como NaN.
    # `df.where(cond, None)` NO convierte a Python None en columnas float
    # porque pandas las mantiene como float y NaN no se puede reemplazar por None
    # en ese dtype.  Solución: convertir primero a dtype=object, luego reemplazar.
    import pandas as pd
    rows = rows.astype(object).where(pd.notna(rows), None)

    reason_counts: dict[str, int] = {}
    error_count = 0
    for rec in rows.to_dict("records"):
        try:
            model_cls(**rec)
        except Exception as exc:  # noqa: BLE001
            error_count += 1
            # Extract first word as reason code (MISSING_ID, BAD_TIME_SLOT, …)
            msg = str(exc)
            code = msg.split(":")[0].strip().split()[0] if msg else "PARSE_ERROR"
            reason_counts[code] = reason_counts.get(code, 0) + 1

    sample_size = len(rows)
    error_rate = error_count / sample_size if sample_size else 0

    result: dict[str, Any] = {
        "sample_size": sample_size,
        "error_count": error_count,
        "error_rate_pct": round(error_rate * 100, 2),
        "reason_distribution": reason_counts,
        "warn_rate_threshold_pct": round(warn_rate * 100, 2),
        "fail_rate_threshold_pct": round(fail_rate * 100, 2),
    }

    if error_rate > fail_rate:
        msg = (
            f"Pydantic FAIL: error_rate={error_rate:.2%} > fail_rate={fail_rate:.2%} "
            f"on view '{view}' model={model_cls.__name__}"
        )
        log.error(msg)
        raise RuntimeError(msg)
    elif error_rate > warn_rate:
        log.warning(
            "Pydantic WARN: error_rate=%.2f%% (> warn %.2f%%) on view %s",
            error_rate * 100, warn_rate * 100, view,
        )
    else:
        log.info(
            "Pydantic OK: %d rows, error_rate=%.2f%% on view %s",
            sample_size, error_rate * 100, view,
        )
    return result


# ─────────────────────────────────────────────────────────────
# ██  VIAJES  ██
# ─────────────────────────────────────────────────────────────

def transform_viajes(partition: PartitionInfo, overwrite: bool = False) -> dict[str, Any]:
    """
    Procesa una partición de viajes y genera:
      - viajes_trip.parquet    (1 fila = 1 viaje)
      - viajes_leg.parquet     (1 fila = 1 etapa, leg_seq 1..4)
      - _quality/quality.json
      - _quarantine/invalid.parquet + valid.parquet (audit)
    """
    if overwrite:
        _clear_partition_dirs(partition)

    csv_path = partition.csv_file
    log.info("=== viajes transform | cut=%s | csv=%s", partition.cut, csv_path)
    t0 = time.monotonic()

    con = _duckdb_con()

    # ── 1. Vista RAW (all-VARCHAR, sin ignore_errors) ──────────
    src = _build_varchar_read(csv_path, partition.columns_sql_spec())
    con.execute(f"CREATE OR REPLACE VIEW raw_viajes AS SELECT * FROM {src}")

    # Conteo de filas leídas — DEBE coincidir con meta_row_count
    read_row_count: int = con.execute("SELECT COUNT(*) FROM raw_viajes").fetchone()[0]  # type: ignore[index]
    meta_count = partition.meta_row_count()
    if meta_count and read_row_count != meta_count:
        log.warning(
            "viajes cut=%s: read_row_count=%d != meta_row_count=%d",
            partition.cut, read_row_count, meta_count,
        )

    # ── 2. Vista enriquecida ──────────────────────────────────
    cut = partition.cut
    year = partition.year
    month = partition.month

    # Columnas de leg: 4 sets de campos
    # Generamos los selects dinámicamente para mantener el código limpio.
    tipodia_sql = _tipodia_case("CAST(TRY_CAST(tipodia AS INTEGER) AS INTEGER)")

    con.execute(f"""
    CREATE OR REPLACE VIEW enriched_viajes AS
    SELECT
        '{cut}'  AS cut,
        {year}   AS year,
        {month}  AS month,

        -- IDs
        id_viaje,
        id_tarjeta,

        -- Tipo dia
        {tipodia_sql} AS tipo_dia,

        -- Proposito y contrato
        UPPER(TRIM(proposito))  AS proposito,
        TRIM(contrato)          AS contrato,

        -- Métricas
        TRY_CAST(factor_expansion AS DOUBLE)  AS factor_expansion,
        TRY_CAST(n_etapas         AS INTEGER) AS n_etapas,
        TRY_CAST(distancia_eucl   AS DOUBLE)  AS distancia_eucl,
        TRY_CAST(distancia_ruta   AS DOUBLE)  AS distancia_ruta,

        -- Tiempo inicio viaje
        TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP) AS tiempo_inicio_viaje,
        TRY_CAST(tiempo_fin_viaje    AS TIMESTAMP) AS tiempo_fin_viaje,

        -- Keys temporales inicio
        CASE
            WHEN TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_date_sk("TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP)")}
        END AS date_start_sk,
        CASE
            WHEN TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_time_30m_sk("TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP)")}
        END AS time_start_30m_sk,

        -- Keys temporales fin
        CASE
            WHEN TRY_CAST(tiempo_fin_viaje AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_date_sk("TRY_CAST(tiempo_fin_viaje AS TIMESTAMP)")}
        END AS date_end_sk,
        CASE
            WHEN TRY_CAST(tiempo_fin_viaje AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_time_30m_sk("TRY_CAST(tiempo_fin_viaje AS TIMESTAMP)")}
        END AS time_end_30m_sk,

        -- Paraderos / comunas / zonas
        UPPER(TRIM(paradero_inicio_viaje)) AS paradero_inicio_viaje,
        UPPER(TRIM(paradero_fin_viaje))    AS paradero_fin_viaje,
        UPPER(TRIM(comuna_inicio_viaje))   AS comuna_inicio_viaje,
        UPPER(TRIM(comuna_fin_viaje))      AS comuna_fin_viaje,
        TRY_CAST(zona_inicio_viaje AS INTEGER) AS zona_inicio_viaje,
        TRY_CAST(zona_fin_viaje  AS INTEGER)   AS zona_fin_viaje,

        -- Periodos
        UPPER(TRIM(periodo_inicio_viaje)) AS periodo_inicio_viaje,
        UPPER(TRIM(periodo_fin_viaje))    AS periodo_fin_viaje,

        -- Duración del viaje (usar tviaje2 porque tviaje puede venir vacío)
        TRY_CAST(tviaje2 AS DOUBLE) AS tviaje_min,

        -- Campos leg (mantenidos para viajes_leg)
        -- Tipo transporte 1..4
        {_mode_case("TRY_CAST(tipo_transporte_1 AS INTEGER)")} AS mode_code_1,
        {_mode_case("TRY_CAST(tipo_transporte_2 AS INTEGER)")} AS mode_code_2,
        {_mode_case("TRY_CAST(tipo_transporte_3 AS INTEGER)")} AS mode_code_3,
        {_mode_case("TRY_CAST(tipo_transporte_4 AS INTEGER)")} AS mode_code_4,

        UPPER(TRIM(srv_1)) AS service_code_1,
        UPPER(TRIM(srv_2)) AS service_code_2,
        UPPER(TRIM(srv_3)) AS service_code_3,
        UPPER(TRIM(srv_4)) AS service_code_4,

        TRIM(op_1era_etapa) AS operator_code_1,
        TRIM(op_2da_etapa)  AS operator_code_2,
        TRIM(op_3era_etapa) AS operator_code_3,
        TRIM(op_4ta_etapa)  AS operator_code_4,

        UPPER(TRIM(paradero_subida_1)) AS board_stop_1,
        UPPER(TRIM(paradero_subida_2)) AS board_stop_2,
        UPPER(TRIM(paradero_subida_3)) AS board_stop_3,
        UPPER(TRIM(paradero_subida_4)) AS board_stop_4,

        UPPER(TRIM(paradero_bajada_1)) AS alight_stop_1,
        UPPER(TRIM(paradero_bajada_2)) AS alight_stop_2,
        UPPER(TRIM(paradero_bajada_3)) AS alight_stop_3,
        UPPER(TRIM(paradero_bajada_4)) AS alight_stop_4,

        TRY_CAST(tiempo_subida_1 AS TIMESTAMP) AS ts_board_1,
        TRY_CAST(tiempo_subida_2 AS TIMESTAMP) AS ts_board_2,
        TRY_CAST(tiempo_subida_3 AS TIMESTAMP) AS ts_board_3,
        TRY_CAST(tiempo_subida_4 AS TIMESTAMP) AS ts_board_4,

        TRY_CAST(tiempo_bajada_1 AS TIMESTAMP) AS ts_alight_1,
        TRY_CAST(tiempo_bajada_2 AS TIMESTAMP) AS ts_alight_2,
        TRY_CAST(tiempo_bajada_3 AS TIMESTAMP) AS ts_alight_3,
        TRY_CAST(tiempo_bajada_4 AS TIMESTAMP) AS ts_alight_4,

        TRY_CAST(zona_subida_1 AS INTEGER) AS zone_board_1,
        TRY_CAST(zona_subida_2 AS INTEGER) AS zone_board_2,
        TRY_CAST(zona_subida_3 AS INTEGER) AS zone_board_3,
        TRY_CAST(zona_subida_4 AS INTEGER) AS zone_board_4,

        TRY_CAST(zona_bajada_1 AS INTEGER) AS zone_alight_1,
        TRY_CAST(zona_bajada_2 AS INTEGER) AS zone_alight_2,
        TRY_CAST(zona_bajada_3 AS INTEGER) AS zone_alight_3,
        TRY_CAST(zona_bajada_4 AS INTEGER) AS zone_alight_4,

        UPPER(TRIM(periodo_bajada_1)) AS fare_period_alight_1,
        UPPER(TRIM(periodo_bajada_2)) AS fare_period_alight_2,
        UPPER(TRIM(periodo_bajada_3)) AS fare_period_alight_3,
        UPPER(TRIM(periodo_bajada_4)) AS fare_period_alight_4,

        TRY_CAST(tv1 AS DOUBLE) AS tv_leg_1,
        TRY_CAST(tv2 AS DOUBLE) AS tv_leg_2,
        TRY_CAST(tv3 AS DOUBLE) AS tv_leg_3,
        TRY_CAST(tv4 AS DOUBLE) AS tv_leg_4,

        TRY_CAST(tc1 AS DOUBLE) AS tc_transfer_1,
        TRY_CAST(tc2 AS DOUBLE) AS tc_transfer_2,
        TRY_CAST(tc3 AS DOUBLE) AS tc_transfer_3,

        TRY_CAST(te1 AS DOUBLE) AS te_wait_1,
        TRY_CAST(te2 AS DOUBLE) AS te_wait_2,
        TRY_CAST(te3 AS DOUBLE) AS te_wait_3

    FROM raw_viajes
    """)

    # ── 3. Quality rules (quarantine) ─────────────────────────
    con.execute("""
    CREATE OR REPLACE VIEW viajes_quality AS
    SELECT *,
        CASE
            WHEN id_viaje IS NULL OR TRIM(id_viaje) = ''
                THEN 'MISSING_ID'
            WHEN tiempo_inicio_viaje IS NULL
                THEN 'MISSING_TIMESTAMP'
            WHEN n_etapas IS NOT NULL AND (n_etapas < 1 OR n_etapas > 4)
                THEN 'BAD_RANGE_N_ETAPAS'
            WHEN tviaje_min IS NOT NULL AND tviaje_min < 0
                THEN 'NEG_DISTANCE'
            WHEN distancia_eucl IS NOT NULL AND distancia_eucl < 0
                THEN 'NEG_DISTANCE'
            WHEN distancia_ruta IS NOT NULL AND distancia_ruta < 0
                THEN 'NEG_DISTANCE'
            WHEN time_start_30m_sk IS NOT NULL
             AND (time_start_30m_sk < 0 OR time_start_30m_sk > 47)
                THEN 'BAD_TIME_SLOT'
            ELSE NULL
        END AS _reason_code
    FROM enriched_viajes
    """)

    valid_q = """
        SELECT * EXCLUDE (_reason_code)
        FROM viajes_quality
        WHERE _reason_code IS NULL
    """
    invalid_q = """
        SELECT *, _reason_code AS reason_code
        FROM viajes_quality
        WHERE _reason_code IS NOT NULL
    """

    # Define only the trip columns (excluir columnas de legs)
    trip_cols = """
        cut, year, month,
        id_viaje, id_tarjeta,
        tipo_dia, proposito, contrato,
        factor_expansion, n_etapas,
        distancia_eucl, distancia_ruta,
        tiempo_inicio_viaje, tiempo_fin_viaje,
        date_start_sk, time_start_30m_sk,
        date_end_sk, time_end_30m_sk,
        paradero_inicio_viaje, paradero_fin_viaje,
        comuna_inicio_viaje, comuna_fin_viaje,
        zona_inicio_viaje, zona_fin_viaje,
        periodo_inicio_viaje, periodo_fin_viaje,
        tviaje_min
    """

    trip_valid_query = f"SELECT {trip_cols} FROM viajes_quality WHERE _reason_code IS NULL"

    # ── 4. Escribir viajes_trip.parquet (atómico) ─────────────
    out_trip = partition.silver_output_dir() / "viajes_trip.parquet"
    _write_parquet_atomic(con, trip_valid_query, out_trip)
    log.info("viajes_trip.parquet written -> %s", out_trip)

    # ── 5. Construir viajes_leg (UNPIVOT manual) ───────────────
    # Generamos UNION de las 4 legs; sólo incluimos si tiene al menos 1 campo útil
    leg_unions = []
    for i in range(1, 5):
        tc = f"tc_transfer_{i}" if i <= 3 else "NULL"
        te = f"te_wait_{i}" if i <= 3 else "NULL"
        leg_unions.append(f"""
        SELECT
            cut, year, month,
            id_viaje, id_tarjeta,
            {i} AS leg_seq,
            CASE WHEN mode_code_{i} = 'UNKNOWN' THEN NULL ELSE mode_code_{i} END AS mode_code,
            service_code_{i}  AS service_code,
            operator_code_{i} AS operator_code,
            board_stop_{i}    AS board_stop_code,
            alight_stop_{i}   AS alight_stop_code,
            ts_board_{i}      AS ts_board,
            ts_alight_{i}     AS ts_alight,
            CASE WHEN ts_board_{i} IS NOT NULL
                 THEN {_ts_to_date_sk(f"ts_board_{i}")} END AS date_board_sk,
            CASE WHEN ts_board_{i} IS NOT NULL
                 THEN {_ts_to_time_30m_sk(f"ts_board_{i}")} END AS time_board_30m_sk,
            CASE WHEN ts_alight_{i} IS NOT NULL
                 THEN {_ts_to_date_sk(f"ts_alight_{i}")} END AS date_alight_sk,
            CASE WHEN ts_alight_{i} IS NOT NULL
                 THEN {_ts_to_time_30m_sk(f"ts_alight_{i}")} END AS time_alight_30m_sk,
            fare_period_alight_{i} AS fare_period_alight_code,
            zone_board_{i}    AS zone_board,
            zone_alight_{i}   AS zone_alight,
            tv_leg_{i}        AS tv_leg_min,
            {tc}              AS tc_transfer_min,
            {te}              AS te_wait_min
        FROM viajes_quality
        WHERE _reason_code IS NULL
          AND (
              mode_code_{i} IS NOT NULL
              OR service_code_{i} IS NOT NULL
              OR board_stop_{i} IS NOT NULL
              OR ts_board_{i} IS NOT NULL
          )
        """)

    leg_query = " UNION ALL ".join(leg_unions)

    out_leg = partition.silver_output_dir() / "viajes_leg.parquet"
    _write_parquet_atomic(con, leg_query, out_leg)
    log.info("viajes_leg.parquet written -> %s", out_leg)

    # ── 6. Quarantine ─────────────────────────────────────────
    invalid_trip_q = f"""
        SELECT {trip_cols}, _reason_code AS reason_code
        FROM viajes_quality
        WHERE _reason_code IS NOT NULL
    """
    quarantine_dir = partition.quarantine_output_dir()
    _write_parquet_atomic(con, invalid_trip_q, quarantine_dir / "invalid.parquet")
    log.info("Quarantine invalid -> %s", quarantine_dir / "invalid.parquet")

    # valid.parquet en quarantine — para auditoría de conteo
    _write_parquet_atomic(con, trip_valid_query, quarantine_dir / "valid.parquet")
    log.info("Quarantine valid -> %s", quarantine_dir / "valid.parquet")

    # ── 7. Pydantic sample validation ─────────────────────────
    con.execute(f"""
        CREATE OR REPLACE VIEW trip_for_pydantic AS
        SELECT {trip_cols} FROM viajes_quality WHERE _reason_code IS NULL
    """)
    pydantic_stats = _validate_sample(con, "trip_for_pydantic", ViajesTripRow)

    # ── 8. Count assertion & quality report ───────────────────
    total_valid = con.execute(
        "SELECT COUNT(*) FROM viajes_quality WHERE _reason_code IS NULL"
    ).fetchone()[0]  # type: ignore[index]
    total_invalid = con.execute(
        "SELECT COUNT(*) FROM viajes_quality WHERE _reason_code IS NOT NULL"
    ).fetchone()[0]  # type: ignore[index]

    assert read_row_count == total_valid + total_invalid, (
        f"viajes cut={partition.cut}: read_row_count={read_row_count} "
        f"!= valid({total_valid}) + invalid({total_invalid})"
    )

    reason_dist = con.execute("""
        SELECT _reason_code, COUNT(*) AS cnt
        FROM viajes_quality
        WHERE _reason_code IS NOT NULL
        GROUP BY _reason_code
        ORDER BY cnt DESC
    """).fetchdf().to_dict("records")

    stats: dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__,
        "git_hash": _git_hash(),
        "dataset": "viajes",
        "cut": partition.cut,
        "year": partition.year,
        "month": partition.month,
        "meta_row_count": meta_count,
        "read_row_count": read_row_count,
        "valid_row_count": total_valid,
        "invalid_row_count": total_invalid,
        "count_assertion": "PASS",
        "quarantine_rate_pct": round(
            total_invalid / read_row_count * 100, 4
        ) if read_row_count else 0,
        "quarantine_reason_distribution": reason_dist,
        "pydantic_sample_validation": pydantic_stats,
        "output_files": [
            str(out_trip.relative_to(out_trip.parents[6])),
            str(out_leg.relative_to(out_leg.parents[6])),
        ],
    }
    _write_quality(stats, partition.quality_output_dir())

    elapsed = time.monotonic() - t0
    log.info("viajes | cut=%s | elapsed=%.1fs", partition.cut, elapsed)
    con.close()
    return stats


# ─────────────────────────────────────────────────────────────
# ██  ETAPAS  ██
# ─────────────────────────────────────────────────────────────

def transform_etapas(partition: PartitionInfo, overwrite: bool = False) -> dict[str, Any]:
    """
    Procesa una partición de etapas y genera:
      - etapas_validation.parquet
      - _quality/quality.json
      - _quarantine/invalid.parquet + valid.parquet (audit)
    """
    if overwrite:
        _clear_partition_dirs(partition)

    csv_path = partition.csv_file
    log.info("=== etapas transform | cut=%s | csv=%s", partition.cut, csv_path)
    t0 = time.monotonic()
    con = _duckdb_con()

    cut = partition.cut
    year = partition.year
    month = partition.month

    src = _build_varchar_read(csv_path, partition.columns_sql_spec())
    con.execute(f"CREATE OR REPLACE VIEW raw_etapas AS SELECT * FROM {src}")

    read_row_count: int = con.execute("SELECT COUNT(*) FROM raw_etapas").fetchone()[0]  # type: ignore[index]
    meta_count = partition.meta_row_count()
    if meta_count and read_row_count != meta_count:
        log.warning(
            "etapas cut=%s: read_row_count=%d != meta_row_count=%d",
            partition.cut, read_row_count, meta_count,
        )

    # Determinar modo: tipo_transporte puede venir como int o como texto
    # Usamos TRY_CAST a int y después mode_case; si ya es texto uppercase lo conservamos
    mode_sql = f"""
        CASE
            WHEN TRY_CAST(tipo_transporte AS INTEGER) IS NOT NULL
            THEN {_mode_case("TRY_CAST(tipo_transporte AS INTEGER)")}
            ELSE UPPER(TRIM(tipo_transporte))
        END
    """

    tipodia_sql = f"""
        CASE
            WHEN TRY_CAST(tipo_dia AS INTEGER) IS NOT NULL
            THEN {_tipodia_case("TRY_CAST(tipo_dia AS INTEGER)")}
            ELSE UPPER(TRIM(tipo_dia))
        END
    """

    con.execute(f"""
    CREATE OR REPLACE VIEW enriched_etapas AS
    SELECT
        '{cut}'  AS cut,
        {year}   AS year,
        {month}  AS month,

        id_etapa,
        TRIM(operador)  AS operador,
        TRIM(contrato)  AS contrato,

        {tipodia_sql} AS tipo_dia,
        {mode_sql}    AS tipo_transporte,

        TRY_CAST(fExpansionServicioPeriodoTS AS DOUBLE) AS fExpansionServicioPeriodoTS,

        -- tiene_bajada: 0/1 -> boolean
        CASE
            WHEN TRY_CAST(tiene_bajada AS INTEGER) = 1 THEN TRUE
            WHEN TRY_CAST(tiene_bajada AS INTEGER) = 0 THEN FALSE
            ELSE NULL
        END AS tiene_bajada,

        TRY_CAST(tiempo_subida AS TIMESTAMP) AS tiempo_subida,
        TRY_CAST(tiempo_bajada AS TIMESTAMP) AS tiempo_bajada,
        TRY_CAST(tiempo_etapa  AS INTEGER)   AS tiempo_etapa,

        -- Keys temporales de subida
        CASE
            WHEN TRY_CAST(tiempo_subida AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_date_sk("TRY_CAST(tiempo_subida AS TIMESTAMP)")}
        END AS date_board_sk,
        CASE
            WHEN TRY_CAST(tiempo_subida AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_time_30m_sk("TRY_CAST(tiempo_subida AS TIMESTAMP)")}
        END AS time_board_30m_sk,

        -- Keys temporales de bajada
        CASE
            WHEN TRY_CAST(tiempo_bajada AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_date_sk("TRY_CAST(tiempo_bajada AS TIMESTAMP)")}
        END AS date_alight_sk,
        CASE
            WHEN TRY_CAST(tiempo_bajada AS TIMESTAMP) IS NOT NULL
            THEN {_ts_to_time_30m_sk("TRY_CAST(tiempo_bajada AS TIMESTAMP)")}
        END AS time_alight_30m_sk,

        -- Coordenadas UTM
        TRY_CAST(x_subida AS INTEGER) AS x_subida,
        TRY_CAST(y_subida AS INTEGER) AS y_subida,
        TRY_CAST(x_bajada AS INTEGER) AS x_bajada,
        TRY_CAST(y_bajada AS INTEGER) AS y_bajada,

        -- Distancias
        TRY_CAST(dist_ruta_paraderos AS INTEGER) AS dist_ruta_paraderos,
        TRY_CAST(dist_eucl_paraderos AS INTEGER) AS dist_eucl_paraderos,

        -- Servicios / paradas / comunas
        UPPER(TRIM(servicio_subida)) AS servicio_subida,
        UPPER(TRIM(servicio_bajada)) AS servicio_bajada,
        UPPER(TRIM(parada_subida))   AS parada_subida,
        UPPER(TRIM(parada_bajada))   AS parada_bajada,
        UPPER(TRIM(comuna_subida))   AS comuna_subida,
        UPPER(TRIM(comuna_bajada))   AS comuna_bajada,
        TRY_CAST(zona_subida AS INTEGER) AS zona_subida,
        TRY_CAST(zona_bajada AS INTEGER) AS zona_bajada,

        TRY_CAST(tEsperaMediaIntervalo AS DOUBLE) AS tEsperaMediaIntervalo,
        UPPER(TRIM(periodoSubida)) AS periodoSubida,
        UPPER(TRIM(periodoBajada)) AS periodoBajada

    FROM raw_etapas
    """)

    # Quality rules
    con.execute("""
    CREATE OR REPLACE VIEW etapas_quality AS
    SELECT *,
        CASE
            WHEN id_etapa IS NULL OR TRIM(id_etapa) = ''
                THEN 'MISSING_ID'
            WHEN tiempo_subida IS NULL
                THEN 'MISSING_TIMESTAMP'
            WHEN tiene_bajada IS NULL
                THEN 'BAD_BOOLEAN'
            WHEN time_board_30m_sk IS NOT NULL
             AND (time_board_30m_sk < 0 OR time_board_30m_sk > 47)
                THEN 'BAD_TIME_SLOT'
            WHEN x_subida IS NOT NULL
             AND (x_subida < 250000 OR x_subida > 450000)
                THEN 'BAD_UTM_X'
            WHEN y_subida IS NOT NULL
             AND (y_subida < 6200000 OR y_subida > 6400000)
                THEN 'BAD_UTM_Y'
            WHEN x_bajada IS NOT NULL
             AND (x_bajada < 250000 OR x_bajada > 450000)
                THEN 'BAD_UTM_X'
            WHEN y_bajada IS NOT NULL
             AND (y_bajada < 6200000 OR y_bajada > 6400000)
                THEN 'BAD_UTM_Y'
            WHEN dist_ruta_paraderos IS NOT NULL AND dist_ruta_paraderos < 0
                THEN 'NEG_DISTANCE'
            WHEN dist_eucl_paraderos IS NOT NULL AND dist_eucl_paraderos < 0
                THEN 'NEG_DISTANCE'
            ELSE NULL
        END AS _reason_code
    FROM enriched_etapas
    """)

    out_valid = partition.silver_output_dir() / "etapas_validation.parquet"
    _write_parquet_atomic(
        con,
        "SELECT * EXCLUDE (_reason_code) FROM etapas_quality WHERE _reason_code IS NULL",
        out_valid,
    )
    log.info("etapas_validation.parquet -> %s", out_valid)

    quarantine_dir = partition.quarantine_output_dir()
    _write_parquet_atomic(
        con,
        "SELECT *, _reason_code AS reason_code FROM etapas_quality WHERE _reason_code IS NOT NULL",
        quarantine_dir / "invalid.parquet",
    )
    _write_parquet_atomic(
        con,
        "SELECT * EXCLUDE (_reason_code) FROM etapas_quality WHERE _reason_code IS NULL",
        quarantine_dir / "valid.parquet",
    )

    # Pydantic
    con.execute("""
        CREATE OR REPLACE VIEW etapas_for_pydantic AS
        SELECT * EXCLUDE (_reason_code) FROM etapas_quality WHERE _reason_code IS NULL
    """)
    pydantic_stats = _validate_sample(con, "etapas_for_pydantic", EtapasValidationRow)

    # Count assertion
    total_valid = con.execute(
        "SELECT COUNT(*) FROM etapas_quality WHERE _reason_code IS NULL"
    ).fetchone()[0]  # type: ignore[index]
    total_invalid = con.execute(
        "SELECT COUNT(*) FROM etapas_quality WHERE _reason_code IS NOT NULL"
    ).fetchone()[0]  # type: ignore[index]

    assert read_row_count == total_valid + total_invalid, (
        f"etapas cut={partition.cut}: read_row_count={read_row_count} "
        f"!= valid({total_valid}) + invalid({total_invalid})"
    )

    reason_dist = con.execute("""
        SELECT _reason_code, COUNT(*) AS cnt
        FROM etapas_quality WHERE _reason_code IS NOT NULL
        GROUP BY _reason_code ORDER BY cnt DESC
    """).fetchdf().to_dict("records")

    stats: dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__,
        "git_hash": _git_hash(),
        "dataset": "etapas",
        "cut": partition.cut,
        "year": partition.year,
        "month": partition.month,
        "meta_row_count": meta_count,
        "read_row_count": read_row_count,
        "valid_row_count": total_valid,
        "invalid_row_count": total_invalid,
        "count_assertion": "PASS",
        "quarantine_rate_pct": round(
            total_invalid / read_row_count * 100, 4
        ) if read_row_count else 0,
        "quarantine_reason_distribution": reason_dist,
        "pydantic_sample_validation": pydantic_stats,
        "output_files": [str(out_valid)],
    }
    _write_quality(stats, partition.quality_output_dir())

    elapsed = time.monotonic() - t0
    log.info("etapas | cut=%s | elapsed=%.1fs", partition.cut, elapsed)
    con.close()
    return stats


# ─────────────────────────────────────────────────────────────
# ██  SUBIDAS_30M  ██
# ─────────────────────────────────────────────────────────────

def transform_subidas_30m(partition: PartitionInfo, overwrite: bool = False) -> dict[str, Any]:
    """
    Procesa una partición de subidas_30m y genera:
      - subidas_30m.parquet
      - _quality/quality.json
      - _quarantine/invalid.parquet + valid.parquet (audit)
    """
    if overwrite:
        _clear_partition_dirs(partition)

    csv_path = partition.csv_file
    log.info("=== subidas_30m transform | cut=%s | csv=%s", partition.cut, csv_path)
    t0 = time.monotonic()
    con = _duckdb_con()

    cut = partition.cut
    year = partition.year
    month = partition.month

    src = _build_varchar_read(csv_path, partition.columns_sql_spec())
    con.execute(f"CREATE OR REPLACE VIEW raw_subidas AS SELECT * FROM {src}")

    read_row_count: int = con.execute("SELECT COUNT(*) FROM raw_subidas").fetchone()[0]  # type: ignore[index]
    meta_count = partition.meta_row_count()
    if meta_count and read_row_count != meta_count:
        log.warning(
            "subidas_30m cut=%s: read_row_count=%d != meta_row_count=%d",
            partition.cut, read_row_count, meta_count,
        )

    # Media_hora es fracción del día (float Excel) -> TIME + time_30m_sk
    media_hora_col = "TRY_CAST(Media_hora AS DOUBLE)"

    con.execute(f"""
    CREATE OR REPLACE VIEW enriched_subidas AS
    SELECT
        '{cut}'  AS cut,
        {year}   AS year,
        {month}  AS month,

        UPPER(TRIM(Tipo_dia))  AS tipo_dia,
        UPPER(TRIM(Modo))      AS mode_code,
        TRIM(Paradero)         AS stop_code,
        UPPER(TRIM(Comuna))    AS comuna,

        -- Convertir fracción del día a TIME
        {_excel_fraction_to_time(media_hora_col)} AS media_hora_time,

        -- time_30m_sk
        {_excel_fraction_to_time_30m_sk(media_hora_col)} AS time_30m_sk,

        TRY_CAST(Subidas_Promedio AS DOUBLE) AS subidas_promedio

    FROM raw_subidas
    WHERE {media_hora_col} IS NOT NULL
    """)

    con.execute("""
    CREATE OR REPLACE VIEW subidas_quality AS
    SELECT *,
        CASE
            WHEN stop_code IS NULL OR TRIM(stop_code) = ''
                THEN 'MISSING_ID'
            WHEN time_30m_sk IS NULL OR time_30m_sk < 0 OR time_30m_sk > 47
                THEN 'BAD_TIME_SLOT'
            WHEN subidas_promedio IS NULL OR subidas_promedio < 0
                THEN 'NEG_DISTANCE'
            ELSE NULL
        END AS _reason_code
    FROM enriched_subidas
    """)

    out_valid = partition.silver_output_dir() / "subidas_30m.parquet"
    _write_parquet_atomic(
        con,
        "SELECT * EXCLUDE (_reason_code) FROM subidas_quality WHERE _reason_code IS NULL",
        out_valid,
    )
    log.info("subidas_30m.parquet -> %s", out_valid)

    quarantine_dir = partition.quarantine_output_dir()
    _write_parquet_atomic(
        con,
        "SELECT *, _reason_code AS reason_code FROM subidas_quality WHERE _reason_code IS NOT NULL",
        quarantine_dir / "invalid.parquet",
    )
    _write_parquet_atomic(
        con,
        "SELECT * EXCLUDE (_reason_code) FROM subidas_quality WHERE _reason_code IS NULL",
        quarantine_dir / "valid.parquet",
    )

    # Pydantic
    con.execute("""
        CREATE OR REPLACE VIEW subidas_for_pydantic AS
        SELECT * EXCLUDE (_reason_code) FROM subidas_quality WHERE _reason_code IS NULL
    """)
    pydantic_stats = _validate_sample(con, "subidas_for_pydantic", Subidas30mRow)

    # Count assertion
    total_valid = con.execute(
        "SELECT COUNT(*) FROM subidas_quality WHERE _reason_code IS NULL"
    ).fetchone()[0]  # type: ignore[index]
    total_invalid = con.execute(
        "SELECT COUNT(*) FROM subidas_quality WHERE _reason_code IS NOT NULL"
    ).fetchone()[0]  # type: ignore[index]

    assert read_row_count == total_valid + total_invalid, (
        f"subidas_30m cut={partition.cut}: read_row_count={read_row_count} "
        f"!= valid({total_valid}) + invalid({total_invalid})"
    )

    reason_dist = con.execute("""
        SELECT _reason_code, COUNT(*) AS cnt
        FROM subidas_quality WHERE _reason_code IS NOT NULL
        GROUP BY _reason_code ORDER BY cnt DESC
    """).fetchdf().to_dict("records")

    stats: dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__,
        "git_hash": _git_hash(),
        "dataset": "subidas_30m",
        "cut": partition.cut,
        "year": partition.year,
        "month": partition.month,
        "meta_row_count": meta_count,
        "read_row_count": read_row_count,
        "valid_row_count": total_valid,
        "invalid_row_count": total_invalid,
        "count_assertion": "PASS",
        "quarantine_rate_pct": round(
            total_invalid / read_row_count * 100, 4
        ) if read_row_count else 0,
        "quarantine_reason_distribution": reason_dist,
        "pydantic_sample_validation": pydantic_stats,
        "output_files": [str(out_valid)],
    }
    _write_quality(stats, partition.quality_output_dir())

    elapsed = time.monotonic() - t0
    log.info("subidas_30m | cut=%s | elapsed=%.1fs", partition.cut, elapsed)
    con.close()
    return stats


# ─────────────────────────────────────────────────────────────
# Dispatcher
# ─────────────────────────────────────────────────────────────

TRANSFORM_REGISTRY: dict[str, Any] = {
    "viajes": transform_viajes,
    "etapas": transform_etapas,
    "subidas_30m": transform_subidas_30m,
}
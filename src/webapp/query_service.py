from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from pyproj import Transformer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_ROOT = PROJECT_ROOT / "lake" / "processed" / "dtpm"

ALLOWED_DAY_TYPES = {"LABORAL", "SABADO", "DOMINGO"}
ALLOWED_MODES = {"BUS", "METRO", "METROTREN", "ZP"}


@dataclass(frozen=True)
class QueryFilters:
    cut_from: str | None = None
    cut_to: str | None = None
    tipo_dia: list[str] | None = None
    mode: list[str] | None = None
    hour_from: int | None = None
    hour_to: int | None = None


def _to_month_cut(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    if len(v) >= 7:
        return v[:7]
    return None


def _subidas_filters(filters: QueryFilters) -> QueryFilters:
    return QueryFilters(
        cut_from=_to_month_cut(filters.cut_from),
        cut_to=_to_month_cut(filters.cut_to),
        tipo_dia=filters.tipo_dia,
        mode=filters.mode,
        hour_from=filters.hour_from,
        hour_to=filters.hour_to,
    )


def _normalize_hour(value: int | None) -> int | None:
    if value is None:
        return None
    return max(0, min(23, int(value)))


def _normalize_list(values: list[str] | None, allowed: set[str]) -> list[str]:
    if not values:
        return []
    normalized = [v.strip().upper() for v in values if v and v.strip()]
    return [v for v in normalized if v in allowed]


def _parquet_glob(dataset: str, filename: str) -> str:
    p = PROCESSED_ROOT / f"dataset={dataset}" / "year=*" / "month=*" / "cut=*" / filename
    return str(p).replace("\\", "/")


def _build_predicates(
    filters: QueryFilters,
    *,
    cut_col: str = "cut",
    day_col: str | None = None,
    mode_col: str | None = None,
    hour_col: str | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if filters.cut_from:
        clauses.append(f"{cut_col} >= ?")
        params.append(filters.cut_from)
    if filters.cut_to:
        clauses.append(f"{cut_col} <= ?")
        params.append(filters.cut_to)

    day_values = _normalize_list(filters.tipo_dia, ALLOWED_DAY_TYPES)
    if day_col and day_values:
        placeholders = ", ".join(["?"] * len(day_values))
        clauses.append(f"{day_col} IN ({placeholders})")
        params.extend(day_values)

    mode_values = _normalize_list(filters.mode, ALLOWED_MODES)
    if mode_col and mode_values:
        placeholders = ", ".join(["?"] * len(mode_values))
        clauses.append(f"{mode_col} IN ({placeholders})")
        params.extend(mode_values)

    hour_from = _normalize_hour(filters.hour_from)
    hour_to = _normalize_hour(filters.hour_to)
    if hour_col and hour_from is not None:
        clauses.append(f"CAST(FLOOR({hour_col} / 2) AS INTEGER) >= ?")
        params.append(hour_from)
    if hour_col and hour_to is not None:
        clauses.append(f"CAST(FLOOR({hour_col} / 2) AS INTEGER) <= ?")
        params.append(hour_to)

    if not clauses:
        return "", []

    return "WHERE " + " AND ".join(clauses), params


def _fetch_rows(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    with duckdb.connect(database=":memory:") as con:
        return con.execute(sql, params).fetchdf().to_dict("records")


def ensure_data_ready() -> bool:
    viajes_glob = _parquet_glob("viajes", "viajes_trip.parquet")
    etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
    subidas_glob = _parquet_glob("subidas_30m", "subidas_30m.parquet")

    sql = f"""
    WITH counts AS (
      SELECT
        (SELECT COUNT(*) FROM glob('{viajes_glob}')) AS viajes_files,
        (SELECT COUNT(*) FROM glob('{etapas_glob}')) AS etapas_files,
        (SELECT COUNT(*) FROM glob('{subidas_glob}')) AS subidas_files
    )
    SELECT CASE
      WHEN viajes_files > 0 AND etapas_files > 0 AND subidas_files > 0 THEN TRUE
      ELSE FALSE
    END AS ready
    FROM counts
    """
    rows = _fetch_rows(sql, [])
    return bool(rows and rows[0].get("ready"))


def ensure_map_points_ready() -> bool:
        subidas_glob = _parquet_glob("subidas_30m", "subidas_30m.parquet")
        etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
        sql = f"""
        SELECT CASE
            WHEN (SELECT COUNT(*) FROM glob('{subidas_glob}')) > 0
             AND (SELECT COUNT(*) FROM glob('{etapas_glob}')) > 0
            THEN TRUE ELSE FALSE
        END AS ready
        """
        rows = _fetch_rows(sql, [])
        return bool(rows and rows[0].get("ready"))


def query_map_points(filters: QueryFilters, limit: int = 400) -> list[dict[str, Any]]:
        subidas_glob = _parquet_glob("subidas_30m", "subidas_30m.parquet")
        etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
        subidas_where, subidas_params = _build_predicates(
                _subidas_filters(filters),
                cut_col="s.cut",
                day_col="s.tipo_dia",
                mode_col="s.mode_code",
                hour_col="s.time_30m_sk",
        )

        sql = f"""
        WITH stop_coords AS (
            SELECT stop_code, x_utm, y_utm
            FROM (
                SELECT
                    stop_code,
                    x_utm,
                    y_utm,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY stop_code
                        ORDER BY COUNT(*) DESC, x_utm, y_utm
                    ) AS rn
                FROM (
                    SELECT
                        parada_subida AS stop_code,
                        CAST(x_subida AS DOUBLE) AS x_utm,
                        CAST(y_subida AS DOUBLE) AS y_utm
                    FROM read_parquet('{etapas_glob}')
                    WHERE parada_subida IS NOT NULL
                        AND TRIM(parada_subida) <> ''
                        AND x_subida BETWEEN 200000 AND 500000
                        AND y_subida BETWEEN 6200000 AND 6350000

                    UNION ALL

                    SELECT
                        parada_bajada AS stop_code,
                        CAST(x_bajada AS DOUBLE) AS x_utm,
                        CAST(y_bajada AS DOUBLE) AS y_utm
                    FROM read_parquet('{etapas_glob}')
                    WHERE parada_bajada IS NOT NULL
                        AND TRIM(parada_bajada) <> ''
                        AND x_bajada BETWEEN 200000 AND 500000
                        AND y_bajada BETWEEN 6200000 AND 6350000
                ) coords
                GROUP BY stop_code, x_utm, y_utm
            ) t
            WHERE rn = 1
        ),
        boardings AS (
            SELECT
                s.cut AS service_date,
                CAST(FLOOR(s.time_30m_sk / 2) AS INTEGER) AS hour_of_day,
                s.tipo_dia,
                s.mode_code,
                s.stop_code,
                ANY_VALUE(s.comuna) AS comuna,
                ROUND(SUM(s.subidas_promedio), 2) AS etapas_estimadas,
                COUNT(*) AS etapas_observadas
            FROM read_parquet('{subidas_glob}') s
            {subidas_where}
            GROUP BY 1,2,3,4,5
        )
        SELECT
            b.service_date,
            b.hour_of_day,
            b.tipo_dia,
            b.mode_code,
            b.stop_code,
            b.comuna,
            c.x_utm,
            c.y_utm,
            b.etapas_estimadas,
            b.etapas_observadas
        FROM boardings b
        JOIN stop_coords c
            ON c.stop_code = b.stop_code
        ORDER BY b.etapas_estimadas DESC
        LIMIT ?
        """

        rows = _fetch_rows(sql, subidas_params + [limit])
        transformer = Transformer.from_crs("EPSG:32719", "EPSG:4326", always_xy=True)

        points: list[dict[str, Any]] = []
        for row in rows:
                try:
                        lon, lat = transformer.transform(float(row["x_utm"]), float(row["y_utm"]))
                except (TypeError, ValueError):
                        continue
                points.append(
                        {
                                "service_date": row["service_date"],
                                "hour_of_day": int(row["hour_of_day"]),
                                "tipo_dia": row["tipo_dia"],
                                "mode_code": row["mode_code"],
                                "stop_code": row["stop_code"],
                                "comuna": row["comuna"],
                                "lat": round(float(lat), 6),
                                "lon": round(float(lon), 6),
                                "etapas_estimadas": float(row["etapas_estimadas"]),
                                "etapas_observadas": int(row["etapas_observadas"]),
                        }
                )
        return points


def query_overview(filters: QueryFilters) -> list[dict[str, Any]]:
    viajes_glob = _parquet_glob("viajes", "viajes_trip.parquet")
    etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
    subidas_glob = _parquet_glob("subidas_30m", "subidas_30m.parquet")

    viajes_where, viajes_params = _build_predicates(filters, cut_col="cut")
    etapas_where, etapas_params = _build_predicates(
        filters,
        cut_col="cut",
        day_col="tipo_dia",
        mode_col="tipo_transporte",
        hour_col="time_board_30m_sk",
    )
    subidas_where, subidas_params = _build_predicates(
        _subidas_filters(filters),
        cut_col="cut",
        day_col="tipo_dia",
        mode_col="mode_code",
        hour_col="time_30m_sk",
    )

    sql = f"""
    SELECT
      (SELECT COUNT(*) FROM read_parquet('{viajes_glob}') {viajes_where}) AS viajes_observados,
      (SELECT COALESCE(ROUND(SUM(factor_expansion), 2), 0) FROM read_parquet('{viajes_glob}') {viajes_where}) AS viajes_estimados,
      (SELECT COUNT(*) FROM read_parquet('{etapas_glob}') {etapas_where}) AS etapas_observadas,
      (SELECT COALESCE(ROUND(SUM(fExpansionServicioPeriodoTS), 2), 0) FROM read_parquet('{etapas_glob}') {etapas_where}) AS etapas_estimadas,
      (SELECT COALESCE(ROUND(SUM(subidas_promedio), 2), 0) FROM read_parquet('{subidas_glob}') {subidas_where}) AS subidas_promedio_total
    """
    params = viajes_params + viajes_params + etapas_params + etapas_params + subidas_params
    return _fetch_rows(sql, params)


def query_demand_by_day_type(filters: QueryFilters) -> list[dict[str, Any]]:
    etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
    where_clause, params = _build_predicates(
        filters,
        cut_col="cut",
        mode_col="tipo_transporte",
        hour_col="time_board_30m_sk",
    )
    sql = f"""
    SELECT
      tipo_dia,
      COUNT(*) AS etapas_observadas,
      ROUND(SUM(fExpansionServicioPeriodoTS), 2) AS etapas_estimadas
    FROM read_parquet('{etapas_glob}')
    {where_clause}
    GROUP BY tipo_dia
    ORDER BY etapas_estimadas DESC
    """
    return _fetch_rows(sql, params)


def query_demand_by_mode(filters: QueryFilters) -> list[dict[str, Any]]:
    etapas_glob = _parquet_glob("etapas", "etapas_validation.parquet")
    where_clause, params = _build_predicates(
        filters,
        cut_col="cut",
        day_col="tipo_dia",
        mode_col="tipo_transporte",
        hour_col="time_board_30m_sk",
    )
    sql = f"""
    SELECT
      tipo_transporte AS mode_code,
      COUNT(*) AS etapas_observadas,
      ROUND(SUM(fExpansionServicioPeriodoTS), 2) AS etapas_estimadas
    FROM read_parquet('{etapas_glob}')
    {where_clause}
    GROUP BY tipo_transporte
    ORDER BY etapas_estimadas DESC
    """
    return _fetch_rows(sql, params)


def query_top_boardings(filters: QueryFilters, limit: int = 20) -> list[dict[str, Any]]:
    subidas_glob = _parquet_glob("subidas_30m", "subidas_30m.parquet")
    where_clause, params = _build_predicates(
        _subidas_filters(filters),
        cut_col="cut",
        day_col="tipo_dia",
        mode_col="mode_code",
        hour_col="time_30m_sk",
    )
    sql = f"""
    SELECT
      stop_code,
      comuna,
      mode_code,
      ROUND(SUM(subidas_promedio), 2) AS subidas_promedio_total
    FROM read_parquet('{subidas_glob}')
    {where_clause}
    GROUP BY stop_code, comuna, mode_code
    ORDER BY subidas_promedio_total DESC
    LIMIT ?
    """
    return _fetch_rows(sql, params + [limit])

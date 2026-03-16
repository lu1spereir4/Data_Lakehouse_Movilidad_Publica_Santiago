from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[1]
ETAPAS_GLOB = (
    ROOT
    / "lake"
    / "processed"
    / "dtpm"
    / "dataset=etapas"
    / "year=*"
    / "month=*"
    / "cut=*"
    / "etapas_validation.parquet"
)
OUT_PATH = ROOT / "web" / "static" / "data" / "map_points.json"


def build_points(limit: int = 3000) -> dict:
    etapas_glob = str(ETAPAS_GLOB).replace("\\", "/")
    sql = f"""
    SELECT
      strftime(strptime(CAST(date_board_sk AS VARCHAR), '%Y%m%d'), '%Y-%m-%d') AS service_date,
            CAST(FLOOR(time_board_30m_sk / 2) AS INTEGER) AS hour_of_day,
      tipo_dia,
      tipo_transporte AS mode_code,
      parada_subida AS stop_code,
      comuna_subida AS comuna,
      AVG(CAST(x_subida AS DOUBLE)) AS x_utm,
      AVG(CAST(y_subida AS DOUBLE)) AS y_utm,
      ROUND(SUM(fExpansionServicioPeriodoTS), 2) AS etapas_estimadas,
      COUNT(*) AS etapas_observadas
    FROM read_parquet('{etapas_glob}')
    WHERE parada_subida IS NOT NULL
      AND TRIM(parada_subida) <> ''
      AND x_subida BETWEEN 200000 AND 500000
      AND y_subida BETWEEN 6200000 AND 6350000
      AND date_board_sk IS NOT NULL
            AND time_board_30m_sk IS NOT NULL
        GROUP BY 1,2,3,4,5,6
    ORDER BY etapas_estimadas DESC
    LIMIT ?
    """

    with duckdb.connect(database=":memory:") as con:
        rows = con.execute(sql, [limit]).fetchdf().to_dict("records")

    transformer = Transformer.from_crs("EPSG:32719", "EPSG:4326", always_xy=True)

    points = []
    for row in rows:
        lon, lat = transformer.transform(float(row["x_utm"]), float(row["y_utm"]))
        points.append(
            {
                "service_date": row["service_date"],
                "hour_of_day": int(row["hour_of_day"]),
                "tipo_dia": row["tipo_dia"],
                "mode_code": row["mode_code"],
                "stop_code": row["stop_code"],
                "comuna": row["comuna"],
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "etapas_estimadas": float(row["etapas_estimadas"]),
                "etapas_observadas": int(row["etapas_observadas"]),
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "crs_input": "EPSG:32719",
        "crs_output": "EPSG:4326",
        "point_count": len(points),
        "points": points,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build transformed geo points for web map")
    parser.add_argument("--limit", type=int, default=3000, help="Max aggregated points")
    args = parser.parse_args()

    payload = build_points(limit=args.limit)
    print(f"Map points generated: {payload['point_count']} -> {OUT_PATH}")


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
set -euo pipefail

if [[ "${SKIP_PIPELINE:-0}" != "1" ]]; then
  echo "[bootstrap] Building demo RAW + Silver"
  python build_lake_demo.py --overwrite
  python build_catalog.py
  python -m src.silver.transform_silver --dataset all --overwrite
else
  echo "[bootstrap] SKIP_PIPELINE=1 -> using existing lake data"
fi

if [[ "${SKIP_MAP_CACHE:-0}" != "1" ]]; then
  MAP_POINTS_LIMIT="${MAP_POINTS_LIMIT:-5000}"
  echo "[bootstrap] Building transformed map points cache (limit=${MAP_POINTS_LIMIT})"
  if ! python scripts/build_map_points.py --limit "${MAP_POINTS_LIMIT}"; then
    echo "[bootstrap] Warning: map points cache build failed. Continuing with API startup."
  fi
else
  echo "[bootstrap] SKIP_MAP_CACHE=1 -> skipping map points cache build"
fi

echo "[serve] Starting Query API on port 8000"
uvicorn src.webapp.main:app --host 0.0.0.0 --port 8000

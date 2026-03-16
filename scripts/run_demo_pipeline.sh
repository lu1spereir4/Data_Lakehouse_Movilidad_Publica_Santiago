#!/usr/bin/env bash
set -euo pipefail

echo "[1/4] Building demo raw lake"
python build_lake_demo.py --overwrite

echo "[2/4] Building lake catalog"
python build_catalog.py

echo "[3/4] Running silver transforms"
python -m src.silver.transform_silver --dataset all --overwrite

echo "[4/4] Running silver smoke tests"
python -m pytest src/silver/tests_smoke.py -q || true

echo "Demo pipeline finished."

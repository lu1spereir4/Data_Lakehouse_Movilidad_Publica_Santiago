from __future__ import annotations

import csv
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "lake" / "raw" / "dtpm"
DEMO_ROOT = ROOT / "data" / "demo"

VIAJES_ROWS_PER_DAY = 450
ETAPAS_ROWS_PER_DAY = 500
ETAPAS_TOTAL_ROWS = 3500
SUBIDAS_ROWS_PER_DAYTYPE = 300
RNG_SEED = 20260416


def _ensure_clean_demo_root() -> None:
    if DEMO_ROOT.exists():
        for p in DEMO_ROOT.rglob("*"):
            if p.is_file():
                p.unlink()
        for p in sorted(DEMO_ROOT.rglob("*"), reverse=True):
            if p.is_dir():
                p.rmdir()
    DEMO_ROOT.mkdir(parents=True, exist_ok=True)


def _head_csv(path: Path, target_rows: int) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="|")
        rows: list[dict[str, str]] = []
        for i, row in enumerate(reader, start=1):
            rows.append(row)
            if i >= target_rows:
                break
        return reader.fieldnames or [], rows


def _write_csv_dicts(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="|", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _build_viajes_sample(rng: random.Random) -> dict[str, int]:
    out_counts: dict[str, int] = {}
    viajes_dirs = sorted((RAW_ROOT / "dataset=viajes" / "year=2025" / "month=04").glob("cut=*"))

    for cut_dir in viajes_dirs:
        cut = cut_dir.name.replace("cut=", "")
        src = cut_dir / "viajes.csv"
        if not src.exists():
            continue

        fieldnames, sampled = _head_csv(src, VIAJES_ROWS_PER_DAY)

        out_file = DEMO_ROOT / "viajes" / f"{cut}.viajes.csv"
        _write_csv_dicts(out_file, fieldnames, sampled)
        out_counts[cut] = len(sampled)

    return out_counts


def _build_etapas_sample(rng: random.Random) -> dict[str, int]:
    src = RAW_ROOT / "dataset=etapas" / "year=2025" / "month=04" / "cut=2025-04-21_2025-04-27" / "etapas.csv"
    fieldnames, sampled = _head_csv(src, ETAPAS_TOTAL_ROWS)
    out_file = DEMO_ROOT / "etapas" / "2025-04-21_2025-04-27.etapas.csv"
    _write_csv_dicts(out_file, fieldnames, sampled)
    return {"2025-04-21_2025-04-27": len(sampled)}


def _build_subidas_sample(rng: random.Random) -> dict[str, int]:
    src = RAW_ROOT / "dataset=subidas_30m" / "year=2025" / "month=04" / "cut=2025-04" / "subidas_30m.csv"
    _ = rng
    targets = {
        "LABORAL": SUBIDAS_ROWS_PER_DAYTYPE,
        "SABADO": SUBIDAS_ROWS_PER_DAYTYPE,
        "DOMINGO": SUBIDAS_ROWS_PER_DAYTYPE,
    }
    samples: dict[str, list[dict[str, str]]] = {k: [] for k in targets}

    with open(src, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="|")
        fieldnames = reader.fieldnames or []
        for row in reader:
            day_type = (row.get("Tipo_dia") or "").upper().strip()
            if day_type not in targets:
                continue
            sample = samples[day_type]
            target = targets[day_type]
            if len(sample) < target:
                sample.append(row)
            if all(len(samples[k]) >= targets[k] for k in targets):
                break

    merged: list[dict[str, str]] = []
    out_counts: dict[str, int] = {}
    for day_type in ["LABORAL", "SABADO", "DOMINGO"]:
        sampled = samples[day_type]
        merged.extend(sampled)
        out_counts[day_type] = len(sampled)

    out_file = DEMO_ROOT / "subidas_30m" / "2025-04.subidas_30m.csv"
    _write_csv_dicts(out_file, fieldnames, merged)

    return out_counts


def main() -> None:
    rng = random.Random(RNG_SEED)
    _ensure_clean_demo_root()

    viajes_counts = _build_viajes_sample(rng)
    etapas_counts = _build_etapas_sample(rng)
    subidas_counts = _build_subidas_sample(rng)

    manifest = {
        "seed": RNG_SEED,
        "viajes_rows_per_day": VIAJES_ROWS_PER_DAY,
        "etapas_rows_per_day": ETAPAS_ROWS_PER_DAY,
        "etapas_total_rows": ETAPAS_TOTAL_ROWS,
        "subidas_rows_per_daytype": SUBIDAS_ROWS_PER_DAYTYPE,
        "generated_from": str(RAW_ROOT),
        "outputs": {
            "viajes": viajes_counts,
            "etapas": etapas_counts,
            "subidas_30m": subidas_counts,
        },
    }

    manifest_path = DEMO_ROOT / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Demo sample created in: {DEMO_ROOT}")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()

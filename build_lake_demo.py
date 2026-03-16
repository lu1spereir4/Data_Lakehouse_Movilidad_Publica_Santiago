from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEMO_DIR = ROOT / "data" / "demo"
LAKE_RAW = ROOT / "lake" / "raw" / "dtpm"
SEPARATOR = "|"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def count_rows(csv_path: Path) -> int:
    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def read_header(csv_path: Path) -> list[str]:
    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh, delimiter=SEPARATOR)
        return next(reader)


def write_meta(meta_path: Path, meta: dict) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")


def clear_raw_dtpm(overwrite: bool = False) -> None:
    if LAKE_RAW.exists() and any(LAKE_RAW.rglob("*.csv")) and not overwrite:
        raise RuntimeError(
            "lake/raw/dtpm already contains CSV files. "
            "Use --overwrite to replace with demo sample."
        )
    if LAKE_RAW.exists():
        shutil.rmtree(LAKE_RAW)
    LAKE_RAW.mkdir(parents=True, exist_ok=True)


def build_viajes() -> None:
    src_dir = DEMO_DIR / "viajes"
    for csv_file in sorted(src_dir.glob("*.viajes.csv")):
        cut = csv_file.name.replace(".viajes.csv", "")
        year = cut[:4]
        month = cut[5:7]

        partition = LAKE_RAW / "dataset=viajes" / f"year={year}" / f"month={month}" / f"cut={cut}"
        partition.mkdir(parents=True, exist_ok=True)

        dst_csv = partition / "viajes.csv"
        shutil.copy2(csv_file, dst_csv)

        rows = count_rows(dst_csv)
        columns = read_header(dst_csv)

        meta = {
            "dataset": "viajes",
            "source": "DTPM demo sample",
            "cut": cut,
            "year": int(year),
            "month": int(month),
            "separator": SEPARATOR,
            "encoding": "utf-8",
            "columns": columns,
            "column_count": len(columns),
            "row_count": rows,
            "file_size_bytes": dst_csv.stat().st_size,
            "source_file": csv_file.name,
            "extracted_at": now_iso(),
        }
        write_meta(partition / "_meta.json", meta)


def build_etapas() -> None:
    src_dir = DEMO_DIR / "etapas"
    csv_files = sorted(src_dir.glob("*.etapas.csv"))
    if not csv_files:
        return

    if len(csv_files) == 1 and "_" in csv_files[0].name:
        src = csv_files[0]
        cut = src.name.replace(".etapas.csv", "")
        year = cut[:4]
        month = cut[5:7]

        partition = LAKE_RAW / "dataset=etapas" / f"year={year}" / f"month={month}" / f"cut={cut}"
        partition.mkdir(parents=True, exist_ok=True)

        dst_csv = partition / "etapas.csv"
        shutil.copy2(src, dst_csv)

        total_rows = count_rows(dst_csv)
        columns = read_header(dst_csv)
        date_from, date_to = cut.split("_", 1)

        meta = {
            "dataset": "etapas",
            "source": "DTPM demo sample",
            "cut": cut,
            "year": int(year),
            "month": int(month),
            "separator": SEPARATOR,
            "encoding": "utf-8",
            "columns": columns,
            "column_count": len(columns),
            "row_count": total_rows,
            "file_size_bytes": dst_csv.stat().st_size,
            "source_files": [src.name],
            "date_range": {"from": date_from, "to": date_to},
            "extracted_at": now_iso(),
        }
        write_meta(partition / "_meta.json", meta)
        return

    cuts = [f.name.replace(".etapas.csv", "") for f in csv_files]
    cuts.sort()
    cut = f"{cuts[0]}_{cuts[-1]}"
    year = cuts[0][:4]
    month = cuts[0][5:7]

    partition = LAKE_RAW / "dataset=etapas" / f"year={year}" / f"month={month}" / f"cut={cut}"
    partition.mkdir(parents=True, exist_ok=True)

    dst_csv = partition / "etapas.csv"
    header_written = False
    total_rows = 0

    with open(dst_csv, "w", encoding="utf-8", newline="") as out_fh:
        writer = None
        for src in csv_files:
            with open(src, "r", encoding="utf-8", newline="") as in_fh:
                reader = csv.reader(in_fh, delimiter=SEPARATOR)
                header = next(reader)
                if not header_written:
                    writer = csv.writer(out_fh, delimiter=SEPARATOR, lineterminator="\n")
                    writer.writerow(header)
                    header_written = True

                for row in reader:
                    writer.writerow(row)
                    total_rows += 1

    columns = read_header(dst_csv)
    meta = {
        "dataset": "etapas",
        "source": "DTPM demo sample",
        "cut": cut,
        "year": int(year),
        "month": int(month),
        "separator": SEPARATOR,
        "encoding": "utf-8",
        "columns": columns,
        "column_count": len(columns),
        "row_count": total_rows,
        "file_size_bytes": dst_csv.stat().st_size,
        "source_files": [f.name for f in csv_files],
        "date_range": {"from": cuts[0], "to": cuts[-1]},
        "extracted_at": now_iso(),
    }
    write_meta(partition / "_meta.json", meta)


def build_subidas_30m() -> None:
    src_dir = DEMO_DIR / "subidas_30m"
    for csv_file in sorted(src_dir.glob("*.subidas_30m.csv")):
        cut = csv_file.name.replace(".subidas_30m.csv", "")
        year = cut[:4]
        month = cut[5:7]

        partition = LAKE_RAW / "dataset=subidas_30m" / f"year={year}" / f"month={month}" / f"cut={cut}"
        partition.mkdir(parents=True, exist_ok=True)

        dst_csv = partition / "subidas_30m.csv"
        shutil.copy2(csv_file, dst_csv)

        rows = count_rows(dst_csv)
        columns = read_header(dst_csv)

        meta = {
            "dataset": "subidas_30m",
            "source": "DTPM demo sample",
            "cut": cut,
            "year": int(year),
            "month": int(month),
            "separator": SEPARATOR,
            "encoding": "utf-8",
            "columns": columns,
            "column_count": len(columns),
            "row_count": rows,
            "file_size_bytes": dst_csv.stat().st_size,
            "source_file": csv_file.name,
            "source_sheet": "demo_csv",
            "ficha": {"note": "Demo sample converted from public data"},
            "extracted_at": now_iso(),
        }
        write_meta(partition / "_meta.json", meta)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build RAW lake from demo CSV sample")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing lake/raw/dtpm content",
    )
    args = parser.parse_args()

    if not DEMO_DIR.exists():
        raise FileNotFoundError(f"Demo directory not found: {DEMO_DIR}")

    clear_raw_dtpm(overwrite=args.overwrite)
    build_viajes()
    build_etapas()
    build_subidas_30m()

    print(f"Demo raw lake generated in: {LAKE_RAW}")


if __name__ == "__main__":
    main()

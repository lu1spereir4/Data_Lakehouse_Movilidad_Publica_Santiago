"""
build_catalog.py — Genera lake_catalog.json consolidando todos los _meta.json del lake.
Ejecutar desde la raíz del proyecto o desde cualquier lugar.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

LAKE_ROOT  = Path(__file__).parent / "lake"
CATALOG_OUT = LAKE_ROOT / "lake_catalog.json"


def build_catalog() -> dict:
    meta_files = sorted(LAKE_ROOT.rglob("_meta.json"))

    partitions = []
    datasets_index: dict[str, dict] = {}

    total_rows  = 0
    total_bytes = 0

    for meta_path in meta_files:
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)

        rel_partition = str(meta_path.parent.relative_to(LAKE_ROOT)).replace("\\", "/")
        layer = rel_partition.split("/")[0]  # raw / processed / curated

        file_size = meta.get("file_size_bytes", 0)
        rows      = meta.get("row_count", 0)
        dataset   = meta.get("dataset", "unknown")

        total_rows  += rows
        total_bytes += file_size

        partition_entry = {
            "partition_path" : rel_partition,
            "layer"          : layer,
            "dataset"        : dataset,
            "cut"            : meta.get("cut"),
            "year"           : meta.get("year"),
            "month"          : meta.get("month"),
            "row_count"      : rows,
            "file_size_bytes": file_size,
            "file_size_mb"   : round(file_size / 1024**2, 2),
            "column_count"   : meta.get("column_count"),
            "separator"      : meta.get("separator"),
            "encoding"       : meta.get("encoding"),
            "extracted_at"   : meta.get("extracted_at"),
            "meta_file"      : str(meta_path.relative_to(LAKE_ROOT)).replace("\\", "/"),
        }
        partitions.append(partition_entry)

        # Índice por dataset
        if dataset not in datasets_index:
            datasets_index[dataset] = {
                "dataset"      : dataset,
                "source"       : meta.get("source"),
                "layer"        : layer,
                "columns"      : meta.get("columns", []),
                "column_count" : meta.get("column_count"),
                "separator"    : meta.get("separator"),
                "encoding"     : meta.get("encoding"),
                "partitions"   : [],
                "total_rows"   : 0,
                "total_size_bytes": 0,
            }
            # Campos extra específicos
            if "date_range" in meta:
                datasets_index[dataset]["date_range"] = meta["date_range"]
            if "source_sheet" in meta:
                datasets_index[dataset]["source_sheet"] = meta["source_sheet"]
            if "ficha" in meta:
                datasets_index[dataset]["ficha"] = meta["ficha"]

        datasets_index[dataset]["partitions"].append({
            "cut"            : meta.get("cut"),
            "partition_path" : rel_partition,
            "row_count"      : rows,
            "file_size_mb"   : round(file_size / 1024**2, 2),
            "extracted_at"   : meta.get("extracted_at"),
        })
        datasets_index[dataset]["total_rows"]       += rows
        datasets_index[dataset]["total_size_bytes"] += file_size

    # Añadir total_size_mb a cada dataset
    for ds in datasets_index.values():
        ds["total_size_mb"] = round(ds["total_size_bytes"] / 1024**2, 2)

    catalog = {
        "catalog_version" : "1.0",
        "generated_at"    : datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lake_root"       : str(LAKE_ROOT),
        "summary": {
            "total_datasets"  : len(datasets_index),
            "total_partitions": len(partitions),
            "total_rows"      : total_rows,
            "total_size_bytes": total_bytes,
            "total_size_gb"   : round(total_bytes / 1024**3, 3),
        },
        "datasets"    : list(datasets_index.values()),
        "partitions"  : partitions,
    }

    return catalog


if __name__ == "__main__":
    catalog = build_catalog()

    CATALOG_OUT.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    s = catalog["summary"]
    print(f"lake_catalog.json generado en: {CATALOG_OUT}")
    print(f"  Datasets     : {s['total_datasets']}")
    print(f"  Particiones  : {s['total_partitions']}")
    print(f"  Filas totales: {s['total_rows']:,}")
    print(f"  Tamaño total : {s['total_size_gb']} GB")

"""
build_lake.py — Organiza los datos extraídos de DTPM en una estructura de Data Lake.

Estructura de salida:
  lake/
    raw/
      dtpm/
        dataset=viajes/
          year=YYYY/month=MM/cut=YYYY-MM-DD/
            viajes.csv
            _meta.json
        dataset=etapas/
          year=YYYY/month=MM/cut=YYYY-MM-DD_YYYY-MM-DD/
            etapas.csv
            _meta.json
        dataset=subidas_30m/
          year=YYYY/month=MM/cut=YYYY-MM/
            subidas_30m.csv
            _meta.json

Fuentes:
  - data/extracted/Tabla-de-viajes-*/viajes/   → un cut por día
  - data/extracted/Tabla-de-etapas-*/etapas/   → todos los días concatenados, cut = rango
  - data/Subida_Paradero_Estacion_YYYY.MM.xlsb  → convertido a CSV, cut = YYYY-MM
"""

import csv
import gzip
import json
import re
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constantes de rutas
# ---------------------------------------------------------------------------
ROOT          = Path(__file__).parent
DATA_DIR      = ROOT / "data"
EXTRACTED_DIR = DATA_DIR / "extracted"
LAKE_RAW      = ROOT / "lake" / "raw" / "dtpm"

SEPARATOR = "|"  # separador de los CSV de DTPM


# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def count_rows(csv_path: Path) -> int:
    """Cuenta filas de datos (sin encabezado)."""
    with open(csv_path, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1  # -1 por el header


def read_header(csv_path: Path) -> list[str]:
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=SEPARATOR)
        return next(reader)


def write_meta(meta_path: Path, meta: dict) -> None:
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"    ✓ {meta_path.relative_to(ROOT)}")


def copy_csv(src: Path, dst: Path) -> None:
    shutil.copy2(src, dst)
    print(f"    ✓ {dst.relative_to(ROOT)}  ({dst.stat().st_size / 1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Extracción desde zip si no existe la carpeta extracted
# ---------------------------------------------------------------------------

def ensure_extracted() -> None:
    """Extrae los ZIP principales + descomprime .gz si aún no se hizo."""
    if EXTRACTED_DIR.exists() and any(EXTRACTED_DIR.rglob("*.csv")):
        return

    print("[0] Carpeta extracted no encontrada — extrayendo ZIPs...")
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

    for top_zip in DATA_DIR.glob("*.zip"):
        top_out = EXTRACTED_DIR / top_zip.stem
        top_out.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(top_zip) as zf:
            zf.extractall(top_out)
        for gz in top_out.rglob("*.gz"):
            out = gz.with_suffix("")
            with gzip.open(gz, "rb") as fi, open(out, "wb") as fo:
                shutil.copyfileobj(fi, fo)
            gz.unlink()

    print("    Extracción completada.\n")


# ---------------------------------------------------------------------------
# Dataset: viajes — un cut por día
# ---------------------------------------------------------------------------

def build_viajes() -> None:
    print("\n[1] Procesando dataset=viajes (un cut por día)...")

    source_dirs = list(EXTRACTED_DIR.glob("Tabla-de-viajes-*"))
    if not source_dirs:
        print("    ⚠ No se encontró carpeta de viajes en extracted/")
        return

    csv_files = sorted(source_dirs[0].rglob("*.csv"))
    if not csv_files:
        print("    ⚠ No hay CSV de viajes.")
        return

    for csv_file in csv_files:
        # Nombre esperado: 2025-04-21.viajes.csv
        match = re.match(r"(\d{4})-(\d{2})-(\d{2})\.viajes\.csv", csv_file.name)
        if not match:
            print(f"    ⚠ Nombre inesperado: {csv_file.name}, omitiendo.")
            continue

        year, month, day = match.groups()
        cut = f"{year}-{month}-{day}"

        partition = LAKE_RAW / "dataset=viajes" / f"year={year}" / f"month={month}" / f"cut={cut}"
        partition.mkdir(parents=True, exist_ok=True)

        dst_csv = partition / "viajes.csv"
        copy_csv(csv_file, dst_csv)

        rows     = count_rows(dst_csv)
        columns  = read_header(dst_csv)

        meta = {
            "dataset"       : "viajes",
            "source"        : "DTPM - Transantiago / RED Movilidad",
            "cut"           : cut,
            "year"          : int(year),
            "month"         : int(month),
            "separator"     : SEPARATOR,
            "encoding"      : "utf-8",
            "columns"       : columns,
            "column_count"  : len(columns),
            "row_count"     : rows,
            "file_size_bytes": dst_csv.stat().st_size,
            "source_file"   : csv_file.name,
            "extracted_at"  : now_iso(),
        }
        write_meta(partition / "_meta.json", meta)


# ---------------------------------------------------------------------------
# Dataset: etapas — todos los días en un solo CSV, cut = rango
# ---------------------------------------------------------------------------

def build_etapas() -> None:
    print("\n[2] Procesando dataset=etapas (cut de rango, todos los días concatenados)...")

    source_dirs = list(EXTRACTED_DIR.glob("Tabla-de-etapas-*"))
    if not source_dirs:
        print("    ⚠ No se encontró carpeta de etapas en extracted/")
        return

    csv_files = sorted(source_dirs[0].rglob("*.csv"))
    if not csv_files:
        print("    ⚠ No hay CSV de etapas.")
        return

    # Determinar rango de fechas
    dates = []
    for f in csv_files:
        m = re.match(r"(\d{4}-\d{2}-\d{2})\.etapas\.csv", f.name)
        if m:
            dates.append(m.group(1))
    dates.sort()
    cut   = f"{dates[0]}_{dates[-1]}"
    year  = dates[0][:4]
    month = dates[0][5:7]

    partition = LAKE_RAW / "dataset=etapas" / f"year={year}" / f"month={month}" / f"cut={cut}"
    partition.mkdir(parents=True, exist_ok=True)

    dst_csv = partition / "etapas.csv"

    print(f"    Concatenando {len(csv_files)} archivos → {dst_csv.relative_to(ROOT)}")

    header_written = False
    total_rows = 0

    with open(dst_csv, "w", encoding="utf-8", newline="") as out_f:
        for i, src in enumerate(csv_files):
            with open(src, encoding="utf-8", newline="") as in_f:
                lines = in_f.readlines()

            if not header_written:
                out_f.writelines(lines)
                header_written = True
                total_rows += len(lines) - 1
            else:
                out_f.writelines(lines[1:])   # omite encabezado duplicado
                total_rows += len(lines) - 1

            print(f"      [{i+1}/{len(csv_files)}] {src.name}  ({len(lines)-1} filas)")

    columns = read_header(dst_csv)

    meta = {
        "dataset"        : "etapas",
        "source"         : "DTPM - Transantiago / RED Movilidad",
        "cut"            : cut,
        "year"           : int(year),
        "month"          : int(month),
        "separator"      : SEPARATOR,
        "encoding"       : "utf-8",
        "columns"        : columns,
        "column_count"   : len(columns),
        "row_count"      : total_rows,
        "file_size_bytes": dst_csv.stat().st_size,
        "source_files"   : [f.name for f in csv_files],
        "date_range"     : {"from": dates[0], "to": dates[-1]},
        "extracted_at"   : now_iso(),
    }
    write_meta(partition / "_meta.json", meta)


# ---------------------------------------------------------------------------
# Dataset: subidas_30m — convierte el .xlsb mensual a CSV
# ---------------------------------------------------------------------------

def build_subidas_30m() -> None:
    print("\n[3] Procesando dataset=subidas_30m (desde .xlsb)...")

    xlsb_files = sorted(DATA_DIR.glob("Subida_Paradero_Estacion_*.xlsb"))
    if not xlsb_files:
        print("    ⚠ No se encontró ningún archivo .xlsb en data/")
        return

    try:
        from pyxlsb import open_workbook
    except ImportError:
        print("    ⚠ pyxlsb no instalado. Ejecuta:  pip install pyxlsb")
        print("    Saltando dataset=subidas_30m.")
        return

    for xlsb_path in xlsb_files:
        # Nombre esperado: Subida_Paradero_Estacion_2025.04.xlsb
        match = re.search(r"(\d{4})\.(\d{2})\.xlsb$", xlsb_path.name)
        if not match:
            print(f"    ⚠ Nombre inesperado: {xlsb_path.name}, omitiendo.")
            continue

        year, month = match.groups()
        cut = f"{year}-{month}"

        partition = LAKE_RAW / "dataset=subidas_30m" / f"year={year}" / f"month={month}" / f"cut={cut}"
        partition.mkdir(parents=True, exist_ok=True)

        dst_csv = partition / "subidas_30m.csv"
        print(f"    Convirtiendo {xlsb_path.name} → {dst_csv.relative_to(ROOT)}")

        rows_written = 0
        header = None
        ficha = {}

        with open_workbook(str(xlsb_path)) as wb:
            all_sheets = wb.sheets
            print(f"    Hojas disponibles: {all_sheets}")

            # Guardar metadatos de FICHA_DATOS si existe
            ficha_sheet = next((s for s in all_sheets if "FICHA" in s.upper()), None)
            if ficha_sheet:
                with wb.get_sheet(ficha_sheet) as ws:
                    for row in ws.rows():
                        vals = [c.v for c in row]
                        if len(vals) >= 2 and vals[0] is not None:
                            ficha[str(vals[0])] = vals[1]

            # Seleccionar la hoja de datos (la más grande o la que no es FICHA)
            data_sheet = next(
                (s for s in all_sheets if "FICHA" not in s.upper()),
                all_sheets[-1],
            )
            print(f"    Leyendo hoja de datos: {data_sheet}")

            with wb.get_sheet(data_sheet) as ws:
                with open(dst_csv, "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f, delimiter=SEPARATOR)
                    for row in ws.rows():
                        values = [c.v for c in row]
                        # Omitir filas completamente vacías
                        if all(v is None for v in values):
                            continue
                        if header is None:
                            header = [str(v) if v is not None else "" for v in values]
                        writer.writerow(values)
                        rows_written += 1

        print(f"      {rows_written - 1} filas escritas")  # -1 por header

        meta = {
            "dataset"        : "subidas_30m",
            "source"         : "DTPM - Subidas por paradero/estación en ventanas de 30 min",
            "cut"            : cut,
            "year"           : int(year),
            "month"          : int(month),
            "separator"      : SEPARATOR,
            "encoding"       : "utf-8",
            "columns"        : header or [],
            "column_count"   : len(header) if header else 0,
            "row_count"      : rows_written - 1,
            "file_size_bytes": dst_csv.stat().st_size,
            "source_file"    : xlsb_path.name,
            "source_sheet"   : data_sheet,
            "ficha"          : ficha,
            "extracted_at"   : now_iso(),
        }
        write_meta(partition / "_meta.json", meta)


# ---------------------------------------------------------------------------
# Árbol final
# ---------------------------------------------------------------------------

def _print_tree(path: Path, prefix: str = "") -> None:
    entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name))
    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        print(f"{prefix}{connector}{entry.name}")
        if entry.is_dir():
            ext = "    " if i == len(entries) - 1 else "│   "
            _print_tree(entry, prefix + ext)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  DTPM Data Lake Builder — raw layer")
    print("=" * 60)
    print(f"  Raíz del proyecto : {ROOT}")
    print(f"  Salida lake/raw   : {LAKE_RAW}")

    ensure_extracted()
    build_viajes()
    build_etapas()
    build_subidas_30m()

    print("\n" + "=" * 60)
    print("  Estructura final del lake/raw/dtpm/:")
    print("=" * 60)
    if LAKE_RAW.exists():
        _print_tree(LAKE_RAW)
    else:
        print("  (sin datos procesados)")

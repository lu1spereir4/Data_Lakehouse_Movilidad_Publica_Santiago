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

import gzip
import json
import re
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# Constantes de rutas
# ---------------------------------------------------------------------------
ROOT          = Path(__file__).parent
DATA_DIR      = ROOT / "data"
EXTRACTED_DIR = DATA_DIR / "extracted"
LAKE_RAW      = ROOT / "lake" / "raw" / "dtpm"

SEPARATOR = "|"  # separador de los CSV de DTPM

# ---------------------------------------------------------------------------
# Constantes de seguridad para extracción
# ---------------------------------------------------------------------------
MAX_ZIP_UNCOMPRESSED_BYTES = 10 * 1024 ** 3   # 10 GB — aborta si el ZIP supera esto al descomprimir
MAX_GZ_UNCOMPRESSED_BYTES  = 10 * 1024 ** 3   # ídem para .gz individuales
MAX_ZIP_ENTRIES            = 10_000           # límite de entradas por ZIP
ALLOWED_ZIP_EXTENSIONS     = {".csv", ".gz"}  # únicas extensiones permitidas dentro del ZIP


# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def count_rows(csv_path: Path) -> int:
    """Cuenta filas de datos (sin encabezado) usando Polars lazy (sin cargar en memoria)."""
    return (
        pl.scan_csv(csv_path, separator=SEPARATOR, infer_schema_length=0)
        .select(pl.len())
        .collect()
        .item()
    )


def read_header(csv_path: Path) -> list[str]:
    """Lee solo el encabezado del CSV sin cargar el resto en memoria."""
    return pl.read_csv(csv_path, separator=SEPARATOR, infer_schema_length=0, n_rows=0).columns


def write_meta(meta_path: Path, meta: dict) -> None:
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"    ✓ {meta_path.relative_to(ROOT)}")


def copy_csv(src: Path, dst: Path) -> None:
    shutil.copy2(src, dst)
    print(f"    ✓ {dst.relative_to(ROOT)}  ({dst.stat().st_size / 1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Extracción segura desde ZIP y GZ
# ---------------------------------------------------------------------------

def _safe_extract_zip(zf: zipfile.ZipFile, target_dir: Path) -> None:
    """
    Extrae un ZipFile de forma segura:
      - Verifica tamaño total descomprimido antes de tocar el disco (zip bomb).
      - Detecta y rechaza path traversal / zip slip.
      - Omite symlinks.
      - Filtra únicamente extensiones permitidas.
      - Limita el número de entradas.
    """
    members = zf.infolist()

    # 1. Límite de entradas
    if len(members) > MAX_ZIP_ENTRIES:
        raise ValueError(
            f"ZIP contiene {len(members):,} entradas, supera el límite de {MAX_ZIP_ENTRIES:,}."
        )

    # 2. Zip bomb: verificar tamaño total ANTES de extraer
    total_uncompressed = sum(m.file_size for m in members)
    if total_uncompressed > MAX_ZIP_UNCOMPRESSED_BYTES:
        raise ValueError(
            f"Tamaño descomprimido total ({total_uncompressed / 1024**3:.2f} GB) "
            f"supera el límite de {MAX_ZIP_UNCOMPRESSED_BYTES / 1024**3:.0f} GB."
        )

    target_resolved = target_dir.resolve()
    extracted = 0

    for member in members:
        # 3. Omitir directorios
        if member.is_dir():
            continue

        # 4. Detectar symlinks por el atributo Unix (modo 0xA = link)
        unix_mode = (member.external_attr >> 16) & 0xFFFF
        if unix_mode != 0 and (unix_mode & 0xF000) == 0xA000:
            print(f"    ⚠ Omitiendo symlink en ZIP: {member.filename}")
            continue

        # 5. Filtrar extensiones
        if Path(member.filename).suffix.lower() not in ALLOWED_ZIP_EXTENSIONS:
            print(f"    ⚠ Extensión no permitida, omitiendo: {member.filename}")
            continue

        # 6. Path traversal: la ruta resuelta debe estar dentro de target_dir
        dest = (target_dir / member.filename).resolve()
        try:
            dest.relative_to(target_resolved)
        except ValueError:
            raise ValueError(
                f"Path traversal detectado en ZIP: '{member.filename}' → '{dest}'"
            )

        zf.extract(member, target_dir)
        extracted += 1

    print(
        f"      {extracted} archivos extraídos | "
        f"{total_uncompressed / 1024**2:.1f} MB descomprimido"
    )


def _safe_decompress_gz(
    gz_path: Path,
    out_path: Path,
    max_bytes: int = MAX_GZ_UNCOMPRESSED_BYTES,
) -> None:
    """Descomprime un .gz en chunks, abortando si supera max_bytes (gz bomb)."""
    written = 0
    chunk_size = 1024 * 1024  # 1 MB por chunk
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with gzip.open(gz_path, "rb") as fi, open(out_path, "wb") as fo:
            while True:
                data = fi.read(chunk_size)
                if not data:
                    break
                written += len(data)
                if written > max_bytes:
                    raise ValueError(
                        f"GZ bomb detectado: '{gz_path.name}' supera "
                        f"{max_bytes / 1024**3:.0f} GB al descomprimir."
                    )
                fo.write(data)
    except ValueError:
        out_path.unlink(missing_ok=True)
        raise


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
        # Sanitizar el stem: solo caracteres alfanuméricos, guiones y puntos
        safe_stem = re.sub(r"[^\w\-\.]", "_", top_zip.stem)
        top_out = (EXTRACTED_DIR / safe_stem).resolve()
        # Garantizar que la salida esté dentro de EXTRACTED_DIR
        try:
            top_out.relative_to(EXTRACTED_DIR.resolve())
        except ValueError:
            raise ValueError(
                f"Nombre de ZIP peligroso detectado: '{top_zip.name}' → '{top_out}'"
            )
        top_out.mkdir(parents=True, exist_ok=True)

        print(f"    Extrayendo {top_zip.name}...")
        with zipfile.ZipFile(top_zip) as zf:
            _safe_extract_zip(zf, top_out)

        for gz in top_out.rglob("*.gz"):
            out = gz.with_suffix("")
            print(f"      Descomprimiendo {gz.name}...")
            _safe_decompress_gz(gz, out)
            gz.unlink()

    print("    Extracción completada.\n")


# ---------------------------------------------------------------------------
# Dataset: viajes — un cut por día
# ---------------------------------------------------------------------------

def build_viajes() -> None:
    print("\n[1] Procesando dataset=viajes (un cut por día)...")

    source_dirs = sorted(EXTRACTED_DIR.glob("Tabla-de-viajes-*"))
    if not source_dirs:
        print("    ⚠ No se encontró carpeta de viajes en extracted/")
        return
    if len(source_dirs) > 1:
        print(f"    ⚠ Se encontraron {len(source_dirs)} carpetas de viajes; usando la más reciente: {source_dirs[-1].name}")

    csv_files = sorted(source_dirs[-1].rglob("*.csv"))
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

    source_dirs = sorted(EXTRACTED_DIR.glob("Tabla-de-etapas-*"))
    if not source_dirs:
        print("    ⚠ No se encontró carpeta de etapas en extracted/")
        return
    if len(source_dirs) > 1:
        print(f"    ⚠ Se encontraron {len(source_dirs)} carpetas de etapas; usando la más reciente: {source_dirs[-1].name}")

    csv_files = sorted(source_dirs[-1].rglob("*.csv"))
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

    # Scan lazy: ningún archivo se carga entero en memoria
    lazy_frames = []
    for i, src in enumerate(csv_files):
        lf = pl.scan_csv(src, separator=SEPARATOR, infer_schema_length=0)
        row_count = lf.select(pl.len()).collect().item()
        print(f"      [{i+1}/{len(csv_files)}] {src.name}  ({row_count} filas)")
        lazy_frames.append(lf)

    combined = pl.concat(lazy_frames, rechunk=False)

    # sink_csv escribe en modo streaming: nunca materializa el DataFrame completo
    combined.sink_csv(dst_csv, separator=SEPARATOR)

    total_rows = count_rows(dst_csv)
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

        header: list[str] | None = None
        ficha: dict = {}
        data_rows: list[list] = []

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

            # Seleccionar la hoja de datos (la que no es FICHA)
            data_sheet = next(
                (s for s in all_sheets if "FICHA" not in s.upper()),
                all_sheets[-1],
            )
            print(f"    Leyendo hoja de datos: {data_sheet}")

            with wb.get_sheet(data_sheet) as ws:
                for row in ws.rows():
                    values = [c.v for c in row]
                    if all(v is None for v in values):
                        continue
                    if header is None:
                        header = [str(v) if v is not None else "" for v in values]
                        continue  # no agregar el encabezado como fila de datos
                    data_rows.append(values)

        # Escribir con Polars: más eficiente y consistente con el resto del pipeline
        df = pl.DataFrame(
            {col: [row[i] if i < len(row) else None for row in data_rows]
             for i, col in enumerate(header or [])},
            infer_schema_length=0,
        )
        df.write_csv(dst_csv, separator=SEPARATOR)
        rows_written = len(df) + 1  # +1 para compatibilidad con conteo anterior (header incluido)
        print(f"      {len(df)} filas escritas")

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

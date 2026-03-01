"""
Script para extraer archivos ZIP (con posibles ZIPs o GZIPs anidados) en la carpeta
data de movilidad_publicaStgo.

Estructura real detectada:
  data/
    *.zip  (ZIP principal)
      └── *.csv.gz  → se descomprimen a *.csv

También soporta:
  data/
    *.zip  (ZIP principal)
      └── *.zip  (ZIPs internos)
            └── *.csv
"""

import gzip
import shutil
import zipfile
from pathlib import Path


DATA_DIR = Path(__file__).parent / "data"
EXTRACTED_DIR = Path(__file__).parent / "data" / "extracted"


def decompress_gz(gz_path: Path) -> Path:
    """Descomprime un archivo .gz y retorna la ruta del archivo resultante."""
    out_path = gz_path.with_suffix("")  # quita el .gz
    with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    gz_path.unlink()  # elimina el .gz ya procesado
    return out_path


def extract_nested_zips(data_dir: Path, output_dir: Path) -> None:
    """
    Recorre todos los .zip en data_dir y extrae su contenido de forma recursiva:
      - .zip internos → se extraen en subcarpeta con su nombre
      - .csv.gz       → se descomprimen a .csv
      - .csv          → se conservan tal cual
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    top_zips = list(data_dir.glob("*.zip"))
    if not top_zips:
        print("No se encontraron archivos .zip en la carpeta data.")
        return

    for top_zip in top_zips:
        top_name = top_zip.stem
        top_output = output_dir / top_name
        top_output.mkdir(parents=True, exist_ok=True)

        print(f"\n[1] Extrayendo ZIP principal: {top_zip.name}")
        with zipfile.ZipFile(top_zip, "r") as zf:
            zf.extractall(top_output)
            members = zf.namelist()
        print(f"    Contenido ({len(members)} archivos): {members[:5]}{'...' if len(members) > 5 else ''}")

        # --- ZIPs internos ---
        for inner_zip in list(top_output.rglob("*.zip")):
            inner_output = inner_zip.parent / inner_zip.stem
            inner_output.mkdir(parents=True, exist_ok=True)
            print(f"  [2] Extrayendo ZIP interno: {inner_zip.name}")
            with zipfile.ZipFile(inner_zip, "r") as zf:
                zf.extractall(inner_output)
            inner_zip.unlink()

        # --- Archivos .gz (incluyendo .csv.gz) ---
        gz_files = list(top_output.rglob("*.gz"))
        if gz_files:
            print(f"  [2] Descomprimiendo {len(gz_files)} archivo(s) .gz ...")
            for gz_file in gz_files:
                out = decompress_gz(gz_file)
                print(f"      {gz_file.name} → {out.name}")

    print("\nExtracción completada.")
    print(f"Archivos disponibles en: {output_dir}")
    _print_tree(output_dir)


def _print_tree(path: Path, prefix: str = "") -> None:
    """Imprime en consola el árbol de archivos extraídos."""
    entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name))
    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        print(f"{prefix}{connector}{entry.name}")
        if entry.is_dir():
            extension = "    " if i == len(entries) - 1 else "│   "
            _print_tree(entry, prefix + extension)


if __name__ == "__main__":
    print(f"Carpeta de datos  : {DATA_DIR}")
    print(f"Carpeta de salida : {EXTRACTED_DIR}")
    extract_nested_zips(DATA_DIR, EXTRACTED_DIR)

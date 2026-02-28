"""
analyze_columns.py
==================
Analiza las columnas de cada dataset raw, calcula métricas de calidad y
genera CSVs slim con solo las columnas relevantes para preguntas de negocio.

Salida:
  lake/processed/dtpm/
    dataset=viajes/     → viajes_slim.csv  + column_report.json
    dataset=etapas/     → etapas_slim.csv  + column_report.json
    dataset=subidas_30m/→ subidas_30m_slim.csv + column_report.json
  column_analysis.json  → informe consolidado de todas las columnas
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

# ─── Rutas ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
LAKE_RAW    = ROOT / "lake" / "raw"  / "dtpm"
LAKE_PROC   = ROOT / "lake" / "processed" / "dtpm"
ANALYSIS_OUT = ROOT / "lake" / "column_analysis.json"

SEP         = "|"
NULL_VALUES = {"-", "", "null", "NULL", "None", "-1", "nan"}
SAMPLE_ROWS = 50_000     # filas para el análisis estadístico

# ─── Selección de columnas por dataset ────────────────────────────────────────
# Justificación de negocio al lado de cada columna.

BUSINESS_COLUMNS = {

    # ── VIAJES ────────────────────────────────────────────────────────────────
    # Preguntas: ¿cuántos viajes por día/período/modo? ¿Tiempos y distancias?
    #            ¿Flujos origen-destino por comuna/zona? ¿Propósito del viaje?
    "viajes": [
        "tipodia",                   # tipo de día (0=laboral,1=sáb,2=dom)
        "factor_expansion",          # peso estadístico del viaje
        "n_etapas",                  # número de transbordos+1
        "tviaje2",                   # duración total del viaje (min) — campo más completo
        "distancia_eucl",            # distancia en línea recta O-D (m)
        "distancia_ruta",            # distancia recorrida en red (m)
        "tiempo_inicio_viaje",       # datetime inicio → partición temporal
        "mediahora_inicio_viaje_hora",  # ventana 30 min legible (HH:MM:00)
        "periodo_inicio_viaje",      # período tarifario de inicio
        "tipo_transporte_1",         # modo principal (1=bus,2=metro,3=tren)
        "srv_1",                     # servicio/línea principal
        "paradero_inicio_viaje",     # paradero origen
        "paradero_fin_viaje",        # paradero destino
        "comuna_inicio_viaje",       # código comuna origen
        "comuna_fin_viaje",          # código comuna destino
        "zona_inicio_viaje",         # zona tarifaria origen
        "zona_fin_viaje",            # zona tarifaria destino
        "modos",                     # modos usados en el viaje
        "proposito",                 # propósito inferido (TRABAJO/HOGAR/EDUC…)
        "contrato",                  # operador del primer servicio
        "id_tarjeta",                # tarjeta anonimizada (para contar viajeros únicos)
    ],

    # ── ETAPAS ────────────────────────────────────────────────────────────────
    # Preguntas: ¿demanda por paradero/servicio/período? ¿Tiempos de viaje
    #            por etapa? ¿Distribución espacial de subidas/bajadas?
    "etapas": [
        "tipo_dia",                  # tipo de día
        "tipo_transporte",           # modo (BUS/METRO/METROTREN)
        "fExpansionServicioPeriodoTS",  # peso estadístico de la etapa
        "tiene_bajada",              # si se detectó bajada (calidad del dato)
        "tiempo_subida",             # datetime subida → eje temporal
        "tiempo_etapa",              # duración etapa (seg)
        "media_hora_subida",         # ventana 30 min de subida (0-47)
        "periodoSubida",             # período tarifario de subida (nombre)
        "x_subida",                  # coordenada X subida (UTM 19S)
        "y_subida",                  # coordenada Y subida
        "x_bajada",                  # coordenada X bajada
        "y_bajada",                  # coordenada Y bajada
        "dist_ruta_paraderos",       # distancia ruta subida-bajada (m)
        "dist_eucl_paraderos",       # distancia euclidiana subida-bajada (m)
        "servicio_subida",           # línea/servicio al subir
        "parada_subida",             # código paradero subida
        "parada_bajada",             # código paradero bajada
        "comuna_subida",             # comuna subida
        "comuna_bajada",             # comuna bajada
        "zona_subida",               # zona tarifaria subida
        "zona_bajada",               # zona tarifaria bajada
        "tEsperaMediaIntervalo",     # tiempo de espera estimado (min)
        "contrato",                  # operador
        "operador",                  # id numérico del operador
    ],

    # ── SUBIDAS 30M ───────────────────────────────────────────────────────────
    # Preguntas: ¿cuántas subidas promedio por paradero, modo, hora y tipo día?
    # Todas las columnas son relevantes (solo 6 total)
    "subidas_30m": [
        "Tipo_dia",        # LABORAL / SABADO / DOMINGO
        "Modo",            # BUS / METRO / METROTREN
        "Paradero",        # código de paradero o estación
        "Comuna",          # comuna del paradero
        "Media_hora",      # ventana de 30 min
        "Subidas_Promedio", # promedio de subidas
    ],
}

# ─── Utilidades ──────────────────────────────────────────────────────────────

def is_null(val: str) -> bool:
    return val.strip() in NULL_VALUES


def infer_dtype(samples: list[str]) -> str:
    non_null = [v for v in samples if not is_null(v)]
    if not non_null:
        return "empty"
    # Intentar int → float → datetime → str
    try:
        [int(v) for v in non_null[:20]]; return "integer"
    except ValueError: pass
    try:
        [float(v) for v in non_null[:20]]; return "float"
    except ValueError: pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%H:%M:%S"):
        try:
            [datetime.strptime(v, fmt) for v in non_null[:10]]; return "datetime"
        except ValueError: pass
    return "string"


def analyze_columns(csv_path: Path, selected: list[str]) -> dict:
    """
    Lee hasta SAMPLE_ROWS filas y calcula por cada columna:
      - null_rate, unique_count, inferred_dtype, sample_values
      - selected (bool): si está en la selección de negocio
    """
    stats: dict[str, dict] = {}
    total = 0

    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=SEP)
        headers = [h for h in (reader.fieldnames or []) if h]  # quita col vacía final

        for col in headers:
            stats[col] = {"nulls": 0, "values": [], "selected": col in selected}

        for row in reader:
            if total >= SAMPLE_ROWS:
                break
            total += 1
            for col in headers:
                val = row.get(col, "")
                if is_null(val):
                    stats[col]["nulls"] += 1
                elif len(stats[col]["values"]) < 30:
                    stats[col]["values"].append(val)

    report = {}
    for col in headers:
        s = stats[col]
        null_rate = round(s["nulls"] / total, 4) if total else 1.0
        dtype = infer_dtype(s["values"])
        unique = len(set(s["values"]))

        report[col] = {
            "selected"     : s["selected"],
            "null_rate"    : null_rate,
            "null_pct"     : f"{null_rate*100:.1f}%",
            "inferred_dtype": dtype,
            "unique_sample": unique,
            "sample_values": s["values"][:8],
        }

    report["_stats"] = {"rows_sampled": total, "total_columns": len(headers)}
    return report


def write_slim_csv(src: Path, dst: Path, selected: list[str]) -> int:
    """Filtra columnas y escribe CSV slim. Retorna número de filas escritas."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with open(src, encoding="utf-8", newline="") as fin, \
         open(dst, "w",  encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin, delimiter=SEP)
        headers = [h for h in (reader.fieldnames or []) if h]
        # Solo las seleccionadas que realmente existen
        keep = [c for c in selected if c in headers]

        writer = csv.DictWriter(fout, fieldnames=keep, delimiter=SEP,
                                extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            writer.writerow({c: row[c] for c in keep})
            written += 1

    return written


# ─── Pipeline por dataset ─────────────────────────────────────────────────────

DATASET_PATHS = {
    "viajes": (
        LAKE_RAW / "dataset=viajes/year=2025/month=04/cut=2025-04-21/viajes.csv",
        LAKE_PROC / "dataset=viajes/viajes_slim.csv",
    ),
    "etapas": (
        LAKE_RAW / "dataset=etapas/year=2025/month=04/cut=2025-04-21_2025-04-27/etapas.csv",
        LAKE_PROC / "dataset=etapas/etapas_slim.csv",
    ),
    "subidas_30m": (
        LAKE_RAW / "dataset=subidas_30m/year=2025/month=04/cut=2025-04/subidas_30m.csv",
        LAKE_PROC / "dataset=subidas_30m/subidas_30m_slim.csv",
    ),
}


def main() -> None:
    print("=" * 65)
    print("  Column Analyzer + Slim CSV Builder")
    print("=" * 65)

    full_analysis = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sample_rows_used": SAMPLE_ROWS,
        "datasets": {},
    }

    for dataset, (src, dst) in DATASET_PATHS.items():
        selected = BUSINESS_COLUMNS[dataset]
        print(f"\n{'─'*65}")
        print(f"  Dataset: {dataset}")
        print(f"  Fuente : {src.relative_to(ROOT)}")
        print(f"  Destino: {dst.relative_to(ROOT)}")

        if not src.exists():
            print(f"  ⚠ Archivo no encontrado, omitiendo.")
            continue

        # 1. Analizar columnas
        print(f"\n  [1] Analizando columnas (muestra {SAMPLE_ROWS:,} filas)...")
        report = analyze_columns(src, selected)
        meta   = report.pop("_stats")

        # Imprimir resumen en consola
        total_cols    = meta["total_columns"]
        selected_cols = sum(1 for v in report.values() if v["selected"])
        print(f"      Columnas totales   : {total_cols}")
        print(f"      Columnas selected  : {selected_cols}")
        print(f"      Columnas descartadas: {total_cols - selected_cols}")
        print()
        print(f"  {'Columna':<38} {'Selec':<7} {'Nulos':<8} {'Tipo':<12} {'Muestra'}")
        print(f"  {'─'*38} {'─'*6} {'─'*7} {'─'*11} {'─'*20}")

        for col, info in report.items():
            mark   = "✅" if info["selected"] else "  "
            sample = ", ".join(str(v) for v in info["sample_values"][:3])
            print(f"  {col:<38} {mark:<7} {info['null_pct']:<8} {info['inferred_dtype']:<12} {sample[:40]}")

        # Guardar column_report.json junto al slim CSV
        dst.parent.mkdir(parents=True, exist_ok=True)
        report_path = dst.parent / "column_report.json"
        report_path.write_text(
            json.dumps({"dataset": dataset, "sampled_rows": meta["rows_sampled"],
                        "columns": report}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n  [2] column_report.json guardado → {report_path.relative_to(ROOT)}")

        # 2. Escribir CSV slim
        print(f"  [3] Escribiendo CSV slim ({selected_cols} columnas)...")
        rows = write_slim_csv(src, dst, selected)
        size_mb = round(dst.stat().st_size / 1024**2, 1)
        orig_mb = round(src.stat().st_size    / 1024**2, 1)
        saving  = round((1 - dst.stat().st_size / src.stat().st_size) * 100, 1)
        print(f"      Filas escritas : {rows:,}")
        print(f"      Tamaño original: {orig_mb:,} MB")
        print(f"      Tamaño slim    : {size_mb:,} MB  (ahorro {saving}%)")

        full_analysis["datasets"][dataset] = {
            "src"              : str(src.relative_to(ROOT)),
            "dst"              : str(dst.relative_to(ROOT)),
            "total_columns"    : total_cols,
            "selected_columns" : selected_cols,
            "dropped_columns"  : total_cols - selected_cols,
            "row_count"        : rows,
            "original_size_mb" : orig_mb,
            "slim_size_mb"     : size_mb,
            "size_saving_pct"  : saving,
            "columns"          : report,
        }

    # 3. Guardar análisis consolidado
    ANALYSIS_OUT.write_text(
        json.dumps(full_analysis, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n{'='*65}")
    print(f"  Análisis consolidado → {ANALYSIS_OUT.relative_to(ROOT)}")
    print(f"  CSVs slim en         → {LAKE_PROC.relative_to(ROOT)}/")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()

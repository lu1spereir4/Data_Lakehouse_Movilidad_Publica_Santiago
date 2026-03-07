"""
ORQUESTADOR PRINCIPAL DE LA CAPA SILVER: Entry Point CLI robust, este script nos permite
decidir que procesa, como se reportan los errores y asegurar que el sistema sea estable.

Algunas consideraciones en los comandos de los casos de uso:
--dataset all: Permite procesar todo el Lakehouse de una vez.
--overwrite: Activa la idempotencia. Si una partición falló ayer, hoy la borras y la haces de nuevo desde cero sin dejar basura.
--dry-run: Es una red de seguridad. Te dice qué va a pasar sin gastar cómputo ni mover archivos.

transform_silver.py — CLI para ejecutar la capa Silver DTPM. 

Uso:
    # Dataset específico + cut específico
    python -m src.silver.transform_silver --dataset viajes --cut 2025-04-21

    # Todas las particiones de un dataset
    python -m src.silver.transform_silver --dataset etapas

    # Todos los datasets y todas las particiones
    python -m src.silver.transform_silver --dataset all

    # Dry-run (muestra particiones sin procesar)
    python -m src.silver.transform_silver --dataset all --dry-run

    # Sobreescribir particiones ya procesadas
    python -m src.silver.transform_silver --dataset viajes --cut 2025-04-21 --overwrite

    # Ajustar umbrales Pydantic
    python -m src.silver.transform_silver --dataset all --pydantic-warn-rate 0.02 --pydantic-fail-rate 0.10

    # Log más detallado
    python -m src.silver.transform_silver --dataset viajes --cut 2025-04-21 --log-level DEBUG
"""

from __future__ import annotations

import argparse # permite controlar todo desde la terminal, vital para CI/CD (GitHub Actions o AirFlow)
import sys
import time
from typing import Optional

from loguru import logger

from src.silver.catalog import Catalog, PartitionInfo
from src.silver.contracts import PYDANTIC_FAIL_RATE, PYDANTIC_WARN_RATE
from src.silver.transforms import TRANSFORM_REGISTRY

# ─────────────────────────────────────────────────────────────
# Logging estructurado (Loguru)
# ─────────────────────────────────────────────────────────────

def _setup_logging(level: str = "INFO") -> None:
    """Configura loguru: elimina el handler por defecto y añade uno con formato propio."""
    logger.remove()
    logger.add(
        sys.stdout,
        level=level.upper(),
        format="[<green>{time:YYYY-MM-DDTHH:mm:ss}Z</green>] [<level>{level:<8}</level>] {message}",
        colorize=True,
    )


log = logger

# ─────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────

def _resolve_partitions(
    catalog: Catalog,
    dataset: str,
    cut: Optional[str],
) -> list[PartitionInfo]:
    """
    Devuelve la lista de particiones a procesar según los filtros CLI.
    """
    if dataset == "all":
        partitions = catalog.get_partitions()
    else:
        if dataset not in TRANSFORM_REGISTRY:
            raise ValueError(
                f"Dataset '{dataset}' no soportado. "
                f"Opciones: {list(TRANSFORM_REGISTRY.keys())} | all"
            )
        partitions = catalog.get_partitions(dataset=dataset, cut=cut)

    if not partitions:
        log.warning(f"No partitions found for dataset={dataset} cut={cut}")
    return partitions


def _check_csv_exists(partition: PartitionInfo) -> bool:
    try:
        _ = partition.csv_file
        return True
    except FileNotFoundError as exc:
        log.error(f"CSV not found for partition {partition.dataset}/{partition.cut}: {exc}")
        return False


def run(
    dataset: str,
    cut: Optional[str] = None,
    dry_run: bool = False,
    overwrite: bool = False,
    pydantic_warn_rate: float = PYDANTIC_WARN_RATE,
    pydantic_fail_rate: float = PYDANTIC_FAIL_RATE,
) -> int:
    """
    Ejecuta el pipeline Silver para las particiones indicadas.

    Returns:
        Número de particiones que fallaron (0 = éxito total).
    """
    catalog = Catalog()
    partitions = _resolve_partitions(catalog, dataset, cut)

    if not partitions:
        log.warning("Nothing to process.")
        return 0

    log.info(f"Partitions to process: {len(partitions)} | dry_run={dry_run} | overwrite={overwrite}")

    failed = 0
    for i, part in enumerate(partitions, 1):
        log.info(f"[{i}/{len(partitions)}] dataset={part.dataset}  cut={part.cut}  rows={part.row_count:,}")
        if dry_run:
            log.info(f"  [DRY-RUN] csv={part.abs_partition_dir}")
            log.info(f"  [DRY-RUN] out={part.silver_output_dir()}")
            continue

        if not _check_csv_exists(part):
            log.error(f"Skipping partition {part.dataset}/{part.cut} — CSV not found.")
            failed += 1
            continue

        transform_fn = TRANSFORM_REGISTRY.get(part.dataset)
        if transform_fn is None:
            log.warning(f"No transform registered for dataset '{part.dataset}'. Skipping.")
            continue

        t0 = time.monotonic()
        try:
            transform_fn(part, overwrite=overwrite)
            elapsed = time.monotonic() - t0
            log.info(f"✔ DONE  dataset={part.dataset}  cut={part.cut}  elapsed={elapsed:.1f}s")
        except AssertionError as exc:
            elapsed = time.monotonic() - t0
            log.error(f"✘ COUNT ASSERTION FAILED  dataset={part.dataset}  cut={part.cut}  elapsed={elapsed:.1f}s — {exc}")
            failed += 1
        except RuntimeError as exc:
            # Raised by _validate_sample when error_rate > fail_rate
            elapsed = time.monotonic() - t0
            log.error(f"✘ PYDANTIC FAIL RATE EXCEEDED  dataset={part.dataset}  cut={part.cut}  elapsed={elapsed:.1f}s — {exc}")
            failed += 1
        except Exception:  # noqa: BLE001
            elapsed = time.monotonic() - t0
            log.exception(f"✘ FAILED  dataset={part.dataset}  cut={part.cut}  elapsed={elapsed:.1f}s")
            failed += 1

    return failed


# ─────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.silver.transform_silver",
        description="Silver Layer — DTPM Movilidad Santiago (DuckDB + Pydantic v2)",
    )
    p.add_argument(
        "--dataset",
        required=True,
        metavar="DATASET",
        help=(
            "Dataset a procesar: viajes | etapas | subidas_30m | all. "
            "Usar 'all' para procesar todos los datasets."
        ),
    )
    p.add_argument(
        "--cut",
        default=None,
        metavar="CUT",
        help=(
            "Corte específico a procesar (ej: 2025-04-21). "
            "Si se omite, se procesan todos los cortes del dataset."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Muestra las particiones que se procesarían sin ejecutar nada.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help=(
            "Elimina silver/ quality/ quarantine/ antes de procesar. "
            "Garantiza idempotencia total."
        ),
    )
    p.add_argument(
        "--pydantic-warn-rate",
        type=float,
        default=PYDANTIC_WARN_RATE,
        metavar="RATE",
        dest="pydantic_warn_rate",
        help=(
            f"Tasa de error Pydantic que dispara WARNING (default: {PYDANTIC_WARN_RATE:.1f}). "
            "Valor entre 0 y 1."
        ),
    )
    p.add_argument(
        "--pydantic-fail-rate",
        type=float,
        default=PYDANTIC_FAIL_RATE,
        metavar="RATE",
        dest="pydantic_fail_rate",
        help=(
            f"Tasa de error Pydantic que falla el pipeline (default: {PYDANTIC_FAIL_RATE:.1f}). "
            "Valor entre 0 y 1."
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        metavar="LEVEL",
        help="Nivel de logging (default: INFO).",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    _setup_logging(args.log_level)

    log.info(
        f"Silver transform started | dataset={args.dataset} | cut={args.cut} | "
        f"dry_run={args.dry_run} | overwrite={args.overwrite} | "
        f"warn_rate={args.pydantic_warn_rate * 100:.1f}% | fail_rate={args.pydantic_fail_rate * 100:.1f}%"
    )
    global_t0 = time.monotonic()

    try:
        failed = run(
            dataset=args.dataset,
            cut=args.cut,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            pydantic_warn_rate=args.pydantic_warn_rate,
            pydantic_fail_rate=args.pydantic_fail_rate,
        )
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        sys.exit(130)
    except Exception:
        log.exception("Unhandled error in silver transform.")
        sys.exit(1)

    elapsed = time.monotonic() - global_t0
    log.info(f"Silver transform finished | total_elapsed={elapsed:.1f}s | failed={failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

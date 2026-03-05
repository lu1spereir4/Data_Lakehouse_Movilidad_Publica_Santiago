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
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from src.silver.catalog import Catalog, PartitionInfo
from src.silver.contracts import PYDANTIC_FAIL_RATE, PYDANTIC_WARN_RATE
from src.silver.transforms import TRANSFORM_REGISTRY

# ─────────────────────────────────────────────────────────────
# Logging estructurado
# ─────────────────────────────────────────────────────────────

class _StructuredFormatter(logging.Formatter):
    """Formatter JSON-like para logs estructurados."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        level = record.levelname
        msg = super().format(record)
        return f"[{ts}] [{level:<8}] {msg}"


def _setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_StructuredFormatter())
    logging.basicConfig(level=numeric, handlers=[handler], force=True)


log = logging.getLogger(__name__)

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
        log.warning(
            "No partitions found for dataset=%s cut=%s", dataset, cut
        )
    return partitions


def _check_csv_exists(partition: PartitionInfo) -> bool:
    try:
        _ = partition.csv_file
        return True
    except FileNotFoundError as exc:
        log.error("CSV not found for partition %s/%s: %s", partition.dataset, partition.cut, exc)
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

    log.info(
        "Partitions to process: %d | dry_run=%s | overwrite=%s",
        len(partitions),
        dry_run,
        overwrite,
    )

    failed = 0
    for i, part in enumerate(partitions, 1):
        log.info(
            "[%d/%d] dataset=%s  cut=%s  rows=%s",
            i,
            len(partitions),
            part.dataset,
            part.cut,
            f"{part.row_count:,}",
        )
        if dry_run:
            log.info("  [DRY-RUN] csv=%s", part.abs_partition_dir)
            log.info("  [DRY-RUN] out=%s", part.silver_output_dir())
            continue

        if not _check_csv_exists(part):
            log.error("Skipping partition %s/%s — CSV not found.", part.dataset, part.cut)
            failed += 1
            continue

        transform_fn = TRANSFORM_REGISTRY.get(part.dataset)
        if transform_fn is None:
            log.warning("No transform registered for dataset '%s'. Skipping.", part.dataset)
            continue

        t0 = time.monotonic()
        try:
            transform_fn(part, overwrite=overwrite)
            elapsed = time.monotonic() - t0
            log.info(
                "✔ DONE  dataset=%s  cut=%s  elapsed=%.1fs",
                part.dataset,
                part.cut,
                elapsed,
            )
        except AssertionError as exc:
            elapsed = time.monotonic() - t0
            log.error(
                "✘ COUNT ASSERTION FAILED  dataset=%s  cut=%s  elapsed=%.1fs — %s",
                part.dataset, part.cut, elapsed, exc,
            )
            failed += 1
        except RuntimeError as exc:
            # Raised by _validate_sample when error_rate > fail_rate
            elapsed = time.monotonic() - t0
            log.error(
                "✘ PYDANTIC FAIL RATE EXCEEDED  dataset=%s  cut=%s elapsed=%.1fs — %s",
                part.dataset, part.cut, elapsed, exc,
            )
            failed += 1
        except Exception:  # noqa: BLE001
            elapsed = time.monotonic() - t0
            log.exception(
                "✘ FAILED  dataset=%s  cut=%s  elapsed=%.1fs",
                part.dataset, part.cut, elapsed,
            )
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
        "Silver transform started | dataset=%s | cut=%s | dry_run=%s | "
        "overwrite=%s | warn_rate=%.1f%% | fail_rate=%.1f%%",
        args.dataset,
        args.cut,
        args.dry_run,
        args.overwrite,
        args.pydantic_warn_rate * 100,
        args.pydantic_fail_rate * 100,
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
    log.info("Silver transform finished | total_elapsed=%.1fs | failed=%d", elapsed, failed)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

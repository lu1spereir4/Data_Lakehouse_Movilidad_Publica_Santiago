"""
catalog.py — Lee lake_catalog.json y _meta.json; resuelve rutas absolutas de
             particiones RAW para la capa Silver.

Uso:
    from src.silver.catalog import Catalog
    cat = Catalog()
    partitions = cat.get_partitions(dataset="viajes", cut="2025-04-21")
    # Columnas filtradas (autoritativas desde _meta.json):
    cols = partitions[0].raw_columns
    # Spec SQL para read_csv con tipos todos VARCHAR:
    spec = partitions[0].columns_sql_spec()
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Rutas de proyecto ─────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CATALOG_PATH = _PROJECT_ROOT / "lake" / "lake_catalog.json"
_LAKE_ROOT    = _PROJECT_ROOT / "lake"


def _filter_columns(cols: list[str]) -> list[str]:
    """Elimina nombres de columna vacíos o puramente blancos (ej: '' en viajes)."""
    return [c for c in cols if c and c.strip()]


# ── PartitionInfo ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PartitionInfo:
    dataset:        str
    cut:            str
    year:           int
    month:          int
    partition_path: str           # relativo al lake root
    row_count:      int           # desde catalog (estimado)
    column_count:   int
    separator:      str
    encoding:       str
    meta_file:      str           # relativo al lake root
    # Columnas limpias (desde _meta.json, sin '' para viajes)
    raw_columns:    tuple[str, ...] = field(default_factory=tuple)

    # ── Rutas RAW ────────────────────────────────────────────────────────────

    @property
    def abs_partition_dir(self) -> Path:
        """Directorio absoluto de la partición RAW."""
        return _LAKE_ROOT / self.partition_path

    @property
    def csv_file(self) -> Path:
        """Primer CSV en el directorio de partición (ordenado)."""
        candidates = sorted(self.abs_partition_dir.glob("*.csv"))
        if not candidates:
            raise FileNotFoundError(
                f"No CSV found in: {self.abs_partition_dir}"
            )
        return candidates[0]

    @property
    def meta_file_abs(self) -> Path:
        return _LAKE_ROOT / self.meta_file

    # ── Rutas destino ─────────────────────────────────────────────────────────

    def silver_output_dir(self) -> Path:
        """Directorio de salida Parquet en la capa processed."""
        return (
            _LAKE_ROOT / "processed" / "dtpm"
            / f"dataset={self.dataset}"
            / f"year={self.year}"
            / f"month={self.month:02d}"
            / f"cut={self.cut}"
        )

    def quality_output_dir(self) -> Path:
        return (
            _LAKE_ROOT / "processed" / "_quality"
            / f"dataset={self.dataset}"
            / f"year={self.year}"
            / f"month={self.month:02d}"
            / f"cut={self.cut}"
        )

    def quarantine_output_dir(self) -> Path:
        return (
            _LAKE_ROOT / "processed" / "_quarantine"
            / f"dataset={self.dataset}"
            / f"year={self.year}"
            / f"month={self.month:02d}"
            / f"cut={self.cut}"
        )

    # ── meta.json helpers ─────────────────────────────────────────────────────

    def meta_row_count(self) -> int:
        """Row count desde _meta.json (autoritativo). Fallback al catalog."""
        mp = self.meta_file_abs
        if mp.exists():
            with open(mp, encoding="utf-8") as fh:
                d = json.load(fh)
            return int(d.get("row_count", self.row_count))
        return self.row_count

    def columns_sql_spec(self) -> str:
        """
        Genera el snippet SQL para el parámetro 'columns' de read_csv.
        Todos los tipos son VARCHAR para lectura segura:
            {'col1': 'VARCHAR', 'col2': 'VARCHAR', ...}
        """
        if not self.raw_columns:
            raise ValueError(
                f"No columns defined for partition {self.dataset}/{self.cut}"
            )
        pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in self.raw_columns)
        return "{" + pairs + "}"


# ── Catalog ───────────────────────────────────────────────────────────────────

class Catalog:
    """
    Wrapper sobre lake_catalog.json con resolución de rutas absolutas.

    Carga columnas autoritativas desde _meta.json de cada partición
    y filtra columnas vacías (ej: columna '' al final de viajes).
    """

    SUPPORTED_DATASETS = ("viajes", "etapas", "subidas_30m")

    def __init__(self, catalog_path: Path = _CATALOG_PATH) -> None:
        self._catalog_path = catalog_path
        self._data: dict = {}
        self._partitions: list[PartitionInfo] = []
        self._load()

    # ── Private ───────────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._catalog_path.exists():
            raise FileNotFoundError(f"Catalog not found: {self._catalog_path}")
        with open(self._catalog_path, encoding="utf-8") as fh:
            self._data = json.load(fh)
        self._partitions = self._parse_partitions()
        log.info(
            "Catalog loaded: %d partitions across %d datasets",
            len(self._partitions),
            len({p.dataset for p in self._partitions}),
        )

    def _read_meta_columns(self, meta_file_rel: str) -> list[str]:
        """Lee columnas desde _meta.json y filtra entradas vacías."""
        mp = _LAKE_ROOT / meta_file_rel
        if mp.exists():
            with open(mp, encoding="utf-8") as fh:
                d = json.load(fh)
            return _filter_columns(d.get("columns", []))
        return []

    def _catalog_columns(self, dataset: str) -> list[str]:
        """Fallback: columnas desde el bloque de dataset en lake_catalog.json."""
        for ds in self._data.get("datasets", []):
            if ds["dataset"] == dataset:
                return _filter_columns(ds.get("columns", []))
        return []

    def _parse_partitions(self) -> list[PartitionInfo]:
        result: list[PartitionInfo] = []
        for raw in self._data.get("partitions", []):
            meta_file = raw.get("meta_file", "")
            # Columns: prefer _meta.json, fallback to catalog dataset block
            if meta_file:
                cols = self._read_meta_columns(meta_file)
            else:
                cols = self._catalog_columns(raw["dataset"])

            if not cols:
                log.warning(
                    "No columns found for %s/%s — transforms may fail.",
                    raw["dataset"], raw["cut"],
                )

            result.append(
                PartitionInfo(
                    dataset=raw["dataset"],
                    cut=raw["cut"],
                    year=int(raw["year"]),
                    month=int(raw["month"]),
                    partition_path=raw["partition_path"],
                    row_count=int(raw.get("row_count", 0)),
                    column_count=int(raw.get("column_count", 0)),
                    separator=raw.get("separator", "|"),
                    encoding=raw.get("encoding", "utf-8"),
                    meta_file=meta_file,
                    raw_columns=tuple(cols),
                )
            )
        return result

    # ── Public API ────────────────────────────────────────────────────────────

    def get_partitions(
        self,
        dataset: Optional[str] = None,
        cut: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> list[PartitionInfo]:
        """
        Filtra particiones. Sin argumentos devuelve todas.
        """
        result = self._partitions
        if dataset:
            result = [p for p in result if p.dataset == dataset]
        if cut:
            result = [p for p in result if p.cut == cut]
        if year is not None:
            result = [p for p in result if p.year == year]
        if month is not None:
            result = [p for p in result if p.month == month]
        return result

    @property
    def all_datasets(self) -> list[str]:
        return sorted({p.dataset for p in self._partitions})

    @property
    def lake_root(self) -> Path:
        return _LAKE_ROOT

    @property
    def project_root(self) -> Path:
        return _PROJECT_ROOT

    def dataset_columns(self, dataset: str) -> list[str]:
        """Columnas del catalog para un dataset (filtradas de vacíos)."""
        return self._catalog_columns(dataset)

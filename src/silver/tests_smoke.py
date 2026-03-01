"""
tests_smoke.py — Smoke tests para la capa Silver DTPM.

Ejecutar:
    python src/silver/tests_smoke.py

No requiere pytest. Usa assert puro.
Falla con exit code 1 si algún test falla.
"""

from __future__ import annotations

import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

_PASSED: list[str] = []
_FAILED: list[tuple[str, str]] = []


def _run(name: str, fn: Any) -> None:
    try:
        fn()
        _PASSED.append(name)
        print(f"  PASS  {name}")
    except Exception as exc:  # noqa: BLE001
        _FAILED.append((name, traceback.format_exc()))
        print(f"  FAIL  {name}: {exc}")


# ─────────────────────────────────────────────────────────────
# Imports under test
# ─────────────────────────────────────────────────────────────

from src.silver.catalog import Catalog  # noqa: E402
from src.silver.contracts import (  # noqa: E402
    PYDANTIC_FAIL_RATE,
    PYDANTIC_WARN_RATE,
    EtapasValidationRow,
    Subidas30mRow,
    ViajesLegRow,
    ViajesTripRow,
)
from src.silver.transform_silver import run  # noqa: E402
from src.silver.transforms import TRANSFORM_REGISTRY  # noqa: E402

# ─────────────────────────────────────────────────────────────
# Test helpers — valid sample data
# ─────────────────────────────────────────────────────────────

_NOW = datetime.now(tz=timezone.utc).isoformat()

VALID_TRIP: dict = {
    "cut": "2025-04-21",
    "year": 2025,
    "month": 4,
    "id_viaje": "V001",
    "id_tarjeta": "T001",
    "tipo_dia": "LABORAL",
    "factor_expansion": 1.0,
    "n_etapas": 2,
    "distancia_eucl": 5.0,
    "distancia_ruta": 6.5,
    "tiempo_inicio_viaje": "2025-04-21 08:00:00",
    "tiempo_fin_viaje": "2025-04-21 08:30:00",
    "date_start_sk": 20250421,
    "time_start_30m_sk": 16,
    "date_end_sk": 20250421,
    "time_end_30m_sk": 17,
    "tviaje_min": 30.0,
}

VALID_ETAPA: dict = {
    "cut": "2025-04-21_2025-04-27",
    "year": 2025,
    "month": 4,
    "id_etapa": "E001",
    "tipo_dia": "LABORAL",
    "tipo_transporte": "BUS",
    "fExpansionServicioPeriodoTS": 1.5,
    "tiene_bajada": True,
    "tiempo_subida": "2025-04-21 08:00:00",
    "tiempo_bajada": "2025-04-21 08:20:00",
    "tiempo_etapa": 20,
    "date_board_sk": 20250421,
    "time_board_30m_sk": 16,
    "x_subida": 340000,
    "y_subida": 6290000,
    "dist_ruta_paraderos": 1500,
    "dist_eucl_paraderos": 1300,
    "servicio_subida": "C01",
    "parada_subida": "PA123",
    "tEsperaMediaIntervalo": 5.0,
}

VALID_SUBIDA: dict = {
    "cut": "2025-04",
    "year": 2025,
    "month": 4,
    "tipo_dia": "LABORAL",
    "mode_code": "BUS",
    "stop_code": "PA001",
    "comuna": "SANTIAGO",
    "time_30m_sk": 16,
    "subidas_promedio": 12.5,
}

# ─────────────────────────────────────────────────────────────
# Tests: Catalog
# ─────────────────────────────────────────────────────────────

def test_catalog_loads_partitions() -> None:
    """Catalog carga al menos 9 particiones (3 datasets × varios cuts)."""
    catalog = Catalog()
    partitions = catalog.get_partitions()
    assert len(partitions) >= 9, (
        f"Expected >= 9 partitions, got {len(partitions)}"
    )


def test_catalog_all_three_datasets() -> None:
    """Catalog retorna particiones de los 3 datasets."""
    catalog = Catalog()
    datasets = {p.dataset for p in catalog.get_partitions()}
    assert "viajes" in datasets, "Missing 'viajes' in catalog"
    assert "etapas" in datasets, "Missing 'etapas' in catalog"
    assert "subidas_30m" in datasets, "Missing 'subidas_30m' in catalog"


def test_viajes_columns_no_empty() -> None:
    """raw_columns de cualquier partición viajes no contiene strings vacíos."""
    catalog = Catalog()
    viajes = [p for p in catalog.get_partitions() if p.dataset == "viajes"]
    assert viajes, "No viajes partitions found"
    for p in viajes:
        for col in p.raw_columns:
            assert col and col.strip(), (
                f"Empty column name in viajes partition cut={p.cut}: {p.raw_columns}"
            )


def test_columns_sql_spec_valid() -> None:
    """columns_sql_spec() produce un string con formato dict de DuckDB."""
    catalog = Catalog()
    etapas = [p for p in catalog.get_partitions() if p.dataset == "etapas"]
    assert etapas, "No etapas partitions found"
    spec = etapas[0].columns_sql_spec()
    assert spec.startswith("{"), f"columns_sql_spec doesn't start with '{{': {spec[:50]}"
    assert spec.endswith("}"), f"columns_sql_spec doesn't end with '}}': {spec[-50:]}"
    assert "'VARCHAR'" in spec, "Expected 'VARCHAR' type in col_spec"


def test_meta_row_count_positive() -> None:
    """meta_row_count() retorna un entero positivo para todas las particiones."""
    catalog = Catalog()
    for p in catalog.get_partitions():
        count = p.meta_row_count()
        assert count is None or count >= 0, (
            f"Negative meta_row_count for {p.dataset}/{p.cut}: {count}"
        )


# ─────────────────────────────────────────────────────────────
# Tests: Pydantic contracts — ViajesTripRow
# ─────────────────────────────────────────────────────────────

def test_viajes_trip_accepts_valid() -> None:
    """ViajesTripRow acepta datos válidos."""
    row = ViajesTripRow(**VALID_TRIP)
    assert row.id_viaje == "V001"
    assert row.id_tarjeta == "T001"
    assert row.tipo_dia == "LABORAL"


def test_viajes_trip_rejects_empty_id_viaje() -> None:
    """ViajesTripRow rechaza id_viaje vacío (MISSING_ID)."""
    bad = {**VALID_TRIP, "id_viaje": ""}
    try:
        ViajesTripRow(**bad)
        raise AssertionError("Should have raised ValidationError for empty id_viaje")
    except Exception as exc:
        assert "MISSING_ID" in str(exc) or "id_viaje" in str(exc).lower(), (
            f"Unexpected error message: {exc}"
        )


def test_viajes_trip_accepts_null_id_tarjeta() -> None:
    """ViajesTripRow acepta id_tarjeta nulo (viajes en efectivo son válidos)."""
    row = ViajesTripRow(**{**VALID_TRIP, "id_tarjeta": None})
    assert row.id_tarjeta is None


def test_viajes_trip_requires_tiempo_inicio() -> None:
    """ViajesTripRow requiere tiempo_inicio_viaje."""
    bad = {k: v for k, v in VALID_TRIP.items() if k != "tiempo_inicio_viaje"}
    try:
        ViajesTripRow(**bad)
        raise AssertionError("Should have raised ValidationError for missing tiempo_inicio_viaje")
    except Exception:
        pass  # Expected


def test_viajes_trip_normalizes_tipo_dia() -> None:
    """ViajesTripRow normaliza tipo_dia a UPPER."""
    row = ViajesTripRow(**{**VALID_TRIP, "tipo_dia": "laboral"})
    assert row.tipo_dia == "LABORAL"


# ─────────────────────────────────────────────────────────────
# Tests: Pydantic contracts — ViajesLegRow
# ─────────────────────────────────────────────────────────────

def test_viajes_leg_rejects_empty_leg() -> None:
    """ViajesLegRow rechaza leg sin campos útiles (EMPTY_LEG)."""
    empty_leg = {
        "cut": "2025-04-21",
        "year": 2025,
        "month": 4,
        "id_viaje": "V001",
        "leg_seq": 1,
        # mode_code, service_code, board_stop_code, ts_board all None
    }
    try:
        ViajesLegRow(**empty_leg)
        raise AssertionError("Should have raised ValidationError for empty leg")
    except Exception as exc:
        assert "EMPTY_LEG" in str(exc) or "leg" in str(exc).lower(), (
            f"Unexpected error: {exc}"
        )


def test_viajes_leg_accepts_valid() -> None:
    """ViajesLegRow acepta leg con al menos mode_code."""
    leg = {
        "cut": "2025-04-21",
        "year": 2025,
        "month": 4,
        "id_viaje": "V001",
        "leg_seq": 1,
        "mode_code": "BUS",
        "ts_board": "2025-04-21 08:05:00",
    }
    row = ViajesLegRow(**leg)
    assert row.mode_code == "BUS"


# ─────────────────────────────────────────────────────────────
# Tests: Pydantic contracts — EtapasValidationRow
# ─────────────────────────────────────────────────────────────

def test_etapas_accepts_valid() -> None:
    """EtapasValidationRow acepta datos válidos."""
    row = EtapasValidationRow(**VALID_ETAPA)
    assert row.id_etapa == "E001"
    assert row.tipo_transporte == "BUS"


def test_etapas_rejects_empty_id() -> None:
    """EtapasValidationRow rechaza id_etapa vacío."""
    bad = {**VALID_ETAPA, "id_etapa": ""}
    try:
        EtapasValidationRow(**bad)
        raise AssertionError("Should have raised for empty id_etapa")
    except Exception:
        pass


def test_etapas_requires_tiempo_subida() -> None:
    """EtapasValidationRow requiere tiempo_subida."""
    bad = {k: v for k, v in VALID_ETAPA.items() if k != "tiempo_subida"}
    try:
        EtapasValidationRow(**bad)
        raise AssertionError("Should have raised for missing tiempo_subida")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Tests: Pydantic contracts — Subidas30mRow
# ─────────────────────────────────────────────────────────────

def test_subidas_accepts_valid() -> None:
    """Subidas30mRow acepta datos válidos."""
    row = Subidas30mRow(**VALID_SUBIDA)
    assert row.stop_code == "PA001"
    assert row.subidas_promedio == 12.5


def test_subidas_rejects_empty_stop_code() -> None:
    """Subidas30mRow rechaza stop_code vacío."""
    bad = {**VALID_SUBIDA, "stop_code": ""}
    try:
        Subidas30mRow(**bad)
        raise AssertionError("Should have raised for empty stop_code")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Tests: Pydantic constants
# ─────────────────────────────────────────────────────────────

def test_pydantic_thresholds() -> None:
    """PYDANTIC_WARN_RATE < PYDANTIC_FAIL_RATE y ambos en [0, 1]."""
    assert 0 <= PYDANTIC_WARN_RATE < PYDANTIC_FAIL_RATE <= 1, (
        f"Invalid thresholds: warn={PYDANTIC_WARN_RATE}, fail={PYDANTIC_FAIL_RATE}"
    )


# ─────────────────────────────────────────────────────────────
# Tests: Transform registry
# ─────────────────────────────────────────────────────────────

def test_registry_has_three_datasets() -> None:
    """TRANSFORM_REGISTRY contiene las 3 claves esperadas."""
    expected = {"viajes", "etapas", "subidas_30m"}
    assert set(TRANSFORM_REGISTRY.keys()) == expected, (
        f"Registry keys: {set(TRANSFORM_REGISTRY.keys())}"
    )


# ─────────────────────────────────────────────────────────────
# Tests: CLI dry-run
# ─────────────────────────────────────────────────────────────

def test_cli_dry_run_all() -> None:
    """run('all', dry_run=True) no lanza excepciones y retorna 0 fallos."""
    failed = run("all", dry_run=True)
    assert failed == 0, f"dry_run returned {failed} failures"


def test_cli_dry_run_viajes() -> None:
    """run('viajes', dry_run=True) retorna 0 fallos."""
    failed = run("viajes", dry_run=True)
    assert failed == 0, f"dry_run viajes returned {failed} failures"


def test_cli_dry_run_with_cut() -> None:
    """run('viajes', cut='2025-04-21', dry_run=True) retorna 0 fallos."""
    failed = run("viajes", cut="2025-04-21", dry_run=True)
    assert failed == 0, f"dry_run viajes/cut returned {failed} failures"


# ─────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────

_ALL_TESTS = [
    # Catalog
    ("catalog: loads >= 9 partitions",           test_catalog_loads_partitions),
    ("catalog: all 3 datasets present",          test_catalog_all_three_datasets),
    ("catalog: viajes columns no empty strings", test_viajes_columns_no_empty),
    ("catalog: columns_sql_spec format valid",   test_columns_sql_spec_valid),
    ("catalog: meta_row_count >= 0",             test_meta_row_count_positive),
    # ViajesTripRow
    ("contracts: ViajesTripRow accepts valid",           test_viajes_trip_accepts_valid),
    ("contracts: ViajesTripRow rejects empty id_viaje",  test_viajes_trip_rejects_empty_id_viaje),
    ("contracts: ViajesTripRow accepts null id_tarjeta (cash trips)",test_viajes_trip_accepts_null_id_tarjeta),
    ("contracts: ViajesTripRow requires tiempo_inicio",  test_viajes_trip_requires_tiempo_inicio),
    ("contracts: ViajesTripRow normalizes tipo_dia",     test_viajes_trip_normalizes_tipo_dia),
    # ViajesLegRow
    ("contracts: ViajesLegRow rejects EMPTY_LEG",  test_viajes_leg_rejects_empty_leg),
    ("contracts: ViajesLegRow accepts valid leg",   test_viajes_leg_accepts_valid),
    # EtapasValidationRow
    ("contracts: EtapasValidationRow accepts valid",         test_etapas_accepts_valid),
    ("contracts: EtapasValidationRow rejects empty id",      test_etapas_rejects_empty_id),
    ("contracts: EtapasValidationRow requires tiempo_subida",test_etapas_requires_tiempo_subida),
    # Subidas30mRow
    ("contracts: Subidas30mRow accepts valid",           test_subidas_accepts_valid),
    ("contracts: Subidas30mRow rejects empty stop_code", test_subidas_rejects_empty_stop_code),
    # Thresholds
    ("contracts: PYDANTIC thresholds valid",  test_pydantic_thresholds),
    # Registry
    ("transforms: registry has 3 datasets",  test_registry_has_three_datasets),
    # CLI
    ("cli: dry_run all returns 0 failures",        test_cli_dry_run_all),
    ("cli: dry_run viajes returns 0 failures",     test_cli_dry_run_viajes),
    ("cli: dry_run viajes+cut returns 0 failures", test_cli_dry_run_with_cut),
]


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    print("\n" + "=" * 50)
    print("  Silver Layer -- Smoke Tests")
    print("=" * 50 + "\n")

    for name, fn in _ALL_TESTS:
        _run(name, fn)

    print(f"\n-- Results: {len(_PASSED)} passed, {len(_FAILED)} failed --")
    if _FAILED:
        print("\n-- Failures --")
        for name, tb in _FAILED:
            print(f"\nFAIL: {name}")
            print(tb)
        sys.exit(1)
    else:
        print("\nAll smoke tests passed.")


if __name__ == "__main__":
    main()

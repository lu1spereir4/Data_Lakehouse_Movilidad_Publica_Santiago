"""
contracts.py — Pydantic v2 Data Contracts para la capa Silver DTPM.

IMPORTANTE: Estos modelos se usan SOLO para validar MUESTRAS (≤10k filas).
El quarantine masivo se hace en DuckDB con CASE rules en transforms.py.

Umbrales de alerta configurables:
  PYDANTIC_WARN_RATE = 0.01  (1%  -> WARNING)
  PYDANTIC_FAIL_RATE = 0.05  (5%  -> RuntimeError)
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ── Umbrales configurables ────────────────────────────────────────────────────
PYDANTIC_WARN_RATE: float = 0.01   # error_rate >= 1%  -> WARNING
PYDANTIC_FAIL_RATE: float = 0.05   # error_rate >= 5%  -> RuntimeError

# ── Shared types ──────────────────────────────────────────────────────────────
TipoDia  = Literal["LABORAL", "SABADO", "DOMINGO"]
ModeCode = Literal["BUS", "METRO", "METROTREN", "ZP", "UNKNOWN"]


def _require_nonempty(v: object, field_name: str) -> str:
    """Valida que un campo string no esté vacío."""
    s = str(v).strip() if v is not None else ""
    if not s:
        raise ValueError(f"{field_name} cannot be null or empty")
    return s


def _upper_or_none(v: object) -> Optional[str]:
    """Uppercase + strip; devuelve None si vacío."""
    if v is None:
        return None
    s = str(v).strip().upper()
    return s if s else None


# ──────────────────────────────────────────────────────────────
# 1) fct_trip ─ viajes_trip   (grano: 1 fila = 1 viaje)
# ──────────────────────────────────────────────────────────────
class ViajesTripRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    cut:   str
    year:  int
    month: int = Field(ge=1, le=12)

    # IDs obligatorios (spec E: MISSING_ID)
    id_viaje:   str
    id_tarjeta: Optional[str] = None   # null en viajes en efectivo / tarjeta turística

    tipo_dia:  TipoDia
    proposito: Optional[str] = None
    contrato:  Optional[str] = None

    factor_expansion: Optional[float] = Field(default=None, ge=0)
    n_etapas:         Optional[int]   = Field(default=None, ge=1, le=4)

    distancia_eucl: Optional[float] = Field(default=None, ge=0)
    distancia_ruta: Optional[float] = Field(default=None, ge=0)

    # tiempo_inicio_viaje obligatorio (spec E)
    tiempo_inicio_viaje: datetime
    tiempo_fin_viaje:    Optional[datetime] = None

    date_start_sk:     Optional[int] = None
    time_start_30m_sk: Optional[int] = Field(default=None, ge=0, le=47)
    date_end_sk:       Optional[int] = None
    time_end_30m_sk:   Optional[int] = Field(default=None, ge=0, le=47)

    paradero_inicio_viaje: Optional[str] = None
    paradero_fin_viaje:    Optional[str] = None
    comuna_inicio_viaje:   Optional[str] = None
    comuna_fin_viaje:      Optional[str] = None
    zona_inicio_viaje:     Optional[int] = None
    zona_fin_viaje:        Optional[int] = None
    periodo_inicio_viaje:  Optional[str] = None
    periodo_fin_viaje:     Optional[str] = None

    tviaje_min: Optional[float] = Field(default=None, ge=0)

    @field_validator("id_viaje", mode="before")
    @classmethod
    def validate_id_viaje(cls, v: object) -> str:
        return _require_nonempty(v, "id_viaje")

    @field_validator("id_tarjeta", mode="before")
    @classmethod
    def upper_id_tarjeta(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)

    @field_validator("tipo_dia", mode="before")
    @classmethod
    def validate_tipo_dia(cls, v: object) -> str:
        if v is None:
            raise ValueError("tipo_dia cannot be null")
        return str(v).strip().upper()

    @field_validator("proposito", "contrato", mode="before")
    @classmethod
    def upper_optional(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)


# ──────────────────────────────────────────────────────────────
# 2) fct_trip_leg ─ viajes_leg   (grano: 1 fila = 1 etapa, leg_seq 1..4)
# ──────────────────────────────────────────────────────────────
class ViajesLegRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    cut:   str
    year:  int
    month: int = Field(ge=1, le=12)

    id_viaje:   str
    id_tarjeta: Optional[str] = None
    leg_seq:    int = Field(ge=1, le=4)

    mode_code:     Optional[ModeCode] = None
    service_code:  Optional[str] = None
    operator_code: Optional[str] = None

    board_stop_code:  Optional[str] = None
    alight_stop_code: Optional[str] = None

    ts_board:  Optional[datetime] = None
    ts_alight: Optional[datetime] = None

    date_board_sk:      Optional[int] = None
    time_board_30m_sk:  Optional[int] = Field(default=None, ge=0, le=47)
    date_alight_sk:     Optional[int] = None
    time_alight_30m_sk: Optional[int] = Field(default=None, ge=0, le=47)

    fare_period_alight_code: Optional[str] = None
    zone_board:  Optional[int] = None
    zone_alight: Optional[int] = None

    tv_leg_min:      Optional[float] = Field(default=None, ge=0)
    tc_transfer_min: Optional[float] = Field(default=None, ge=0)
    te_wait_min:     Optional[float] = Field(default=None, ge=0)

    @field_validator("id_viaje", mode="before")
    @classmethod
    def validate_id_viaje(cls, v: object) -> str:
        return _require_nonempty(v, "id_viaje")

    @field_validator("mode_code", mode="before")
    @classmethod
    def normalize_mode(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)

    @field_validator("service_code", "operator_code", mode="before")
    @classmethod
    def upper_service(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)

    @model_validator(mode="after")
    def at_least_one_leg_field(self) -> "ViajesLegRow":
        """Al menos un campo útil por leg (reason_code: EMPTY_LEG)."""
        if all(
            x is None
            for x in [
                self.mode_code,
                self.service_code,
                self.board_stop_code,
                self.ts_board,
            ]
        ):
            raise ValueError(
                "EMPTY_LEG: leg must have at least one of "
                "mode_code, service_code, board_stop_code, ts_board"
            )
        return self


# ──────────────────────────────────────────────────────────────
# 3) fct_validation ─ etapas_validation
#    grano: 1 fila = 1 validación / id_etapa
# ──────────────────────────────────────────────────────────────
class EtapasValidationRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    cut:   str
    year:  int
    month: int = Field(ge=1, le=12)

    # IDs obligatorios (spec E: MISSING_ID)
    id_etapa: str

    operador: Optional[str] = None
    contrato: Optional[str] = None

    tipo_dia:        Optional[TipoDia]  = None
    tipo_transporte: Optional[ModeCode] = None

    fExpansionServicioPeriodoTS: Optional[float] = Field(default=None, ge=0)

    tiene_bajada: Optional[bool] = None   # 0/1 -> boolean (spec E: BAD_BOOLEAN)

    # tiempo_subida obligatorio (spec E)
    tiempo_subida: datetime
    tiempo_bajada: Optional[datetime] = None
    tiempo_etapa:  Optional[int] = Field(default=None, ge=0)

    date_board_sk:      Optional[int] = None
    time_board_30m_sk:  Optional[int] = Field(default=None, ge=0, le=47)
    date_alight_sk:     Optional[int] = None
    time_alight_30m_sk: Optional[int] = Field(default=None, ge=0, le=47)

    # UTM coords (spec E: BAD_UTM)
    x_subida: Optional[int] = Field(default=None, ge=250_000, le=450_000)
    y_subida: Optional[int] = Field(default=None, ge=6_200_000, le=6_400_000)
    x_bajada: Optional[int] = Field(default=None, ge=250_000, le=450_000)
    y_bajada: Optional[int] = Field(default=None, ge=6_200_000, le=6_400_000)

    # Distancias (spec E: NEG_DISTANCE)
    dist_ruta_paraderos: Optional[int] = Field(default=None, ge=0)
    dist_eucl_paraderos: Optional[int] = Field(default=None, ge=0)

    servicio_subida: Optional[str] = None
    servicio_bajada: Optional[str] = None
    parada_subida:   Optional[str] = None
    parada_bajada:   Optional[str] = None
    comuna_subida:   Optional[str] = None
    comuna_bajada:   Optional[str] = None
    zona_subida:     Optional[int] = None
    zona_bajada:     Optional[int] = None

    tEsperaMediaIntervalo: Optional[float] = Field(default=None, ge=0)
    periodoSubida: Optional[str] = None
    periodoBajada: Optional[str] = None

    @field_validator("id_etapa", mode="before")
    @classmethod
    def validate_id_etapa(cls, v: object) -> str:
        return _require_nonempty(v, "id_etapa")

    @field_validator("tipo_dia", "tipo_transporte", mode="before")
    @classmethod
    def upper_code(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)

    @field_validator(
        "servicio_subida", "servicio_bajada",
        "parada_subida",   "parada_bajada",
        "comuna_subida",   "comuna_bajada",
        "periodoSubida",   "periodoBajada",
        mode="before",
    )
    @classmethod
    def upper_str(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)


# ──────────────────────────────────────────────────────────────
# 4) fct_boardings_30m ─ subidas_30m
#    grano: 1 fila = paradero + time_30m + modo + tipo_dia
# ──────────────────────────────────────────────────────────────
class Subidas30mRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    cut:   str
    year:  int
    month: int = Field(ge=1, le=12)

    tipo_dia:  TipoDia
    mode_code: ModeCode
    stop_code: str
    comuna:    Optional[str] = None

    time_30m_sk:      int   = Field(ge=0, le=47)
    subidas_promedio: float = Field(ge=0)

    @field_validator("tipo_dia", "mode_code", mode="before")
    @classmethod
    def upper_code(cls, v: object) -> str:
        if v is None:
            raise ValueError("tipo_dia / mode_code cannot be null")
        s = str(v).strip().upper()
        if not s:
            raise ValueError("tipo_dia / mode_code cannot be empty")
        return s

    @field_validator("stop_code", mode="before")
    @classmethod
    def validate_stop_code(cls, v: object) -> str:
        return _require_nonempty(v, "stop_code")

    @field_validator("comuna", mode="before")
    @classmethod
    def upper_comuna(cls, v: object) -> Optional[str]:
        return _upper_or_none(v)
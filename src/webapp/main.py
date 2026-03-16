from __future__ import annotations

from enum import Enum
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.webapp.query_service import (
    QueryFilters,
    ensure_data_ready,
    query_demand_by_day_type,
    query_demand_by_mode,
    query_map_points,
    query_overview,
    query_top_boardings,
)


class QueryType(str, Enum):
    OVERVIEW = "overview"
    DEMAND_BY_DAY_TYPE = "demand_by_day_type"
    DEMAND_BY_MODE = "demand_by_mode"
    TOP_BOARDINGS = "top_boardings"


class UserQueryRequest(BaseModel):
    query_type: QueryType
    cut_from: str | None = None
    cut_to: str | None = None
    tipo_dia: list[str] = Field(default_factory=list)
    mode: list[str] = Field(default_factory=list)
    hour_from: int | None = Field(default=None, ge=0, le=23)
    hour_to: int | None = Field(default=None, ge=0, le=23)
    limit: int = Field(default=20, ge=1, le=200)


class UserQueryResponse(BaseModel):
    query_type: QueryType
    rows: list[dict]
    row_count: int


class MapPointsRequest(BaseModel):
    cut_from: str | None = None
    cut_to: str | None = None
    tipo_dia: list[str] = Field(default_factory=list)
    mode: list[str] = Field(default_factory=list)
    hour_from: int | None = Field(default=None, ge=0, le=23)
    hour_to: int | None = Field(default=None, ge=0, le=23)
    limit: int = Field(default=400, ge=1, le=2000)


app = FastAPI(
    title="Movilidad Santiago Query API",
    version="1.0.0",
    description="API de consultas para usuarios finales sobre la capa Silver",
)

STATIC_DIR = Path(__file__).resolve().parents[2] / "web" / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health() -> dict:
    return {
        "status": "ok",
        "data_ready": ensure_data_ready(),
    }


@app.post("/api/query", response_model=UserQueryResponse)
def query_endpoint(payload: UserQueryRequest) -> UserQueryResponse:
    if not ensure_data_ready():
        raise HTTPException(
            status_code=503,
            detail="No hay datos Silver disponibles. Ejecuta el pipeline demo primero.",
        )

    filters = QueryFilters(
        cut_from=payload.cut_from,
        cut_to=payload.cut_to,
        tipo_dia=payload.tipo_dia,
        mode=payload.mode,
        hour_from=payload.hour_from,
        hour_to=payload.hour_to,
    )

    if payload.query_type == QueryType.OVERVIEW:
        rows = query_overview(filters)
    elif payload.query_type == QueryType.DEMAND_BY_DAY_TYPE:
        rows = query_demand_by_day_type(filters)
    elif payload.query_type == QueryType.DEMAND_BY_MODE:
        rows = query_demand_by_mode(filters)
    elif payload.query_type == QueryType.TOP_BOARDINGS:
        rows = query_top_boardings(filters, limit=payload.limit)
    else:
        raise HTTPException(status_code=400, detail="Tipo de consulta no soportado")

    return UserQueryResponse(
        query_type=payload.query_type,
        rows=rows,
        row_count=len(rows),
    )


@app.post("/api/map_points")
def map_points_endpoint(payload: MapPointsRequest) -> dict:
    if not ensure_data_ready():
        raise HTTPException(
            status_code=503,
            detail="No hay datos Silver disponibles. Ejecuta el pipeline demo primero.",
        )

    filters = QueryFilters(
        cut_from=payload.cut_from,
        cut_to=payload.cut_to,
        tipo_dia=payload.tipo_dia,
        mode=payload.mode,
        hour_from=payload.hour_from,
        hour_to=payload.hour_to,
    )
    points = query_map_points(filters, limit=payload.limit)
    return {
        "point_count": len(points),
        "points": points,
    }

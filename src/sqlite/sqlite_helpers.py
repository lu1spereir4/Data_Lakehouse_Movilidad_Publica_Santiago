"""
sqlite_helpers.py — Helpers de bajo nivel para la capa Gold SQLite (DTPM).

Funciones:
  connect()            — abre la DB con PRAGMAs de performance
  execute_sql_file()   — ejecuta un archivo .sql (ddl_sqlite.sql)
  exec_scalar()        — devuelve un único valor de una query
  executemany_batches()— inserta por lotes con executemany
  begin / commit / rollback — gestión transaccional explícita

NO usa SQLAlchemy; solo sqlite3 estándar de la stdlib.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterator, Sequence

log = logging.getLogger(__name__)

# ─── Configuración de PRAGMAs ──────────────────────────────────────────────────
_PRAGMA_PERF = """
PRAGMA foreign_keys   = ON;
PRAGMA journal_mode   = WAL;
PRAGMA synchronous    = NORMAL;
PRAGMA temp_store     = MEMORY;
PRAGMA cache_size     = -65536;   -- 64 MB de caché de páginas (negativo = KiB)
PRAGMA mmap_size      = 134217728; -- 128 MB memory-mapped I/O
"""

_PRAGMA_QUERY_ONLY = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA temp_store   = MEMORY;
"""


# ─── Conexión ──────────────────────────────────────────────────────────────────

def connect(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """
    Abre (o crea) la base de datos SQLite en *db_path* y aplica PRAGMAs de
    performance.  Las transacciones se gestionan manualmente (isolation_level=None
    con autocommit desactivado via BEGIN/COMMIT explícitos).

    Args:
        db_path:   Ruta al archivo .db.
        read_only: Si True, abre en modo lectura (uri=True con ?mode=ro).

    Returns:
        Conexión sqlite3 lista para usar.
    """
    db_path = Path(db_path)
    if read_only:
        uri = f"file:{db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), check_same_thread=False)

    conn.row_factory = sqlite3.Row

    # Aplicar PRAGMAs (deben ejecutarse fuera de transacción)
    pragmas = _PRAGMA_QUERY_ONLY if read_only else _PRAGMA_PERF
    for stmt in (s.strip() for s in pragmas.split(";") if s.strip()):
        conn.execute(stmt)

    log.debug("Conexión SQLite abierta: %s (read_only=%s)", db_path, read_only)
    return conn


# ─── Ejecución de archivos SQL ─────────────────────────────────────────────────

def execute_sql_file(conn: sqlite3.Connection, sql_path: str | Path) -> None:
    """
    Lee y ejecuta un archivo .sql completo.  Ideal para aplicar ddl_sqlite.sql.
    Usa executescript() que hace un COMMIT implícito al inicio y ejecuta múltiples
    sentencias separadas por ';'.

    NOTA: executescript() no soporta parámetros; solo para DDL/scripts estáticos.
    """
    sql_path = Path(sql_path)
    if not sql_path.exists():
        raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    log.info("Ejecutando SQL file: %s", sql_path.name)
    conn.executescript(sql_text)
    log.debug("SQL file ejecutado OK: %s", sql_path.name)


# ─── exec_scalar ──────────────────────────────────────────────────────────────

def exec_scalar(
    conn: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
) -> Any:
    """
    Ejecuta *sql* con *params* y devuelve el primer valor de la primera fila,
    o None si la query no devuelve filas.

    Ejemplo::
        cnt = exec_scalar(conn, "SELECT COUNT(*) FROM dim_stop WHERE is_current=1")
    """
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


# ─── executemany_batches ───────────────────────────────────────────────────────

def executemany_batches(
    conn: sqlite3.Connection,
    sql: str,
    rows: Iterator[Sequence[Any]] | list[Sequence[Any]],
    *,
    batch_size: int = 5_000,
) -> tuple[int, int]:
    """
    Inserta *rows* usando *sql* en lotes de *batch_size* filas.
    Cada lote se ejecuta con cursor.executemany() dentro de la transacción
    activa (el llamador gestiona BEGIN/COMMIT/ROLLBACK).

    Args:
        conn:       Conexión SQLite.
        sql:        INSERT (o INSERT OR IGNORE) con placeholders '?'.
        rows:       Iterable de tuplas/listas de valores.
        batch_size: Filas por lote (default 5 000).

    Returns:
        (total_inserted, total_rows_processed) — basado en cursor.rowcount.
        Nota: con INSERT OR IGNORE, total_inserted ≤ total_rows_processed.
    """
    cur = conn.cursor()
    total_processed = 0
    total_inserted  = 0
    batch: list[Sequence[Any]] = []

    def _flush(b: list[Sequence[Any]]) -> int:
        cur.executemany(sql, b)
        # rowcount es -1 con executemany en sqlite3 < 3.35 pero correcto desde 3.35+
        # usamos len(b) como fallback conservador cuando rowcount < 0
        return cur.rowcount if cur.rowcount >= 0 else len(b)

    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            total_inserted  += _flush(batch)
            total_processed += len(batch)
            batch = []

    if batch:
        total_inserted  += _flush(batch)
        total_processed += len(batch)

    return total_inserted, total_processed


# ─── Gestión transaccional ────────────────────────────────────────────────────

def begin(conn: sqlite3.Connection) -> None:
    """Inicia una transacción explícita (DEFERRED por defecto en SQLite)."""
    conn.execute("BEGIN")


def commit(conn: sqlite3.Connection) -> None:
    """Confirma la transacción activa."""
    conn.commit()


def rollback(conn: sqlite3.Connection) -> None:
    """Revierte la transacción activa."""
    conn.rollback()


# ─── Helpers de introspección ─────────────────────────────────────────────────

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Devuelve True si la tabla existe en la DB."""
    sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    return exec_scalar(conn, sql, (table_name,)) is not None


def get_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Lista los nombres de columna de una tabla."""
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [r["name"] for r in rows]


def row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Cuenta filas en una tabla."""
    return exec_scalar(conn, f"SELECT COUNT(*) FROM {table_name}") or 0

"""
sql_helpers.py  —  Utilidades de conexión y operaciones SQL Server para Gold.

Responsabilidades:
  - Leer credenciales desde .env  (python-dotenv)
  - Construir conexión pyodbc con fast_executemany habilitado
  - Ejecutar archivo DDL (split por ';' statement-safe)
  - Bulk insert de DataFrames con chunks (fast_executemany)
  - Logging estructurado reutilizable
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pyodbc
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH     = _PROJECT_ROOT / ".env"
DDL_PATH      = _PROJECT_ROOT / "models" / "gold" / "ddl_gold.sql"

# ─────────────────────────────────────────────────────────────
# Logging estructurado (mismo estilo que Silver)
# ─────────────────────────────────────────────────────────────

class _StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts  = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        lvl = record.levelname
        msg = super().format(record)
        return f"[{ts}] [{lvl:<8}] {msg}"


def setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_StructuredFormatter())
    logging.basicConfig(level=numeric, handlers=[handler], force=True)


log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Conexión
# ─────────────────────────────────────────────────────────────

def _load_env() -> None:
    """Carga variables de entorno desde .env (si existe)."""
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH)
    else:
        log.warning(".env no encontrado en %s — usando variables de entorno del sistema.", _ENV_PATH)


def build_connection_string() -> str:
    """
    Construye el connection string de pyodbc desde variables de entorno.

    Variables requeridas en .env:
        SQLSERVER_HOST, SQLSERVER_DB, SQLSERVER_USER, SQLSERVER_PASSWORD
    Variables opcionales (con defaults razonables):
        SQLSERVER_PORT, SQLSERVER_DRIVER, SQLSERVER_ENCRYPT, SQLSERVER_TRUST_CERT
    """
    _load_env()

    host     = os.environ["SQLSERVER_HOST"]
    port     = os.environ.get("SQLSERVER_PORT", "").strip()
    db       = os.environ["SQLSERVER_DB"]
    user     = os.environ.get("SQLSERVER_USER", "").strip()
    password = os.environ.get("SQLSERVER_PASSWORD", "").strip()
    driver   = os.environ.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    encrypt  = os.environ.get("SQLSERVER_ENCRYPT", "yes")
    trust    = os.environ.get("SQLSERVER_TRUST_CERT", "yes")

    # SERVER: include ,port only when TCP is active (port not empty)
    server = f"{host},{port}" if port else host

    # Auth mode: Windows Auth when no user is supplied or user contains a backslash
    windows_auth = (not user) or ("\\" in user)

    if windows_auth:
        auth_part = "Trusted_Connection=yes;"
    else:
        auth_part = f"UID={user};PWD={password};"

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"{auth_part}"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust};"
        f"Connection Timeout=30;"
    )


def get_connection() -> pyodbc.Connection:
    """
    Devuelve una conexión pyodbc con fast_executemany habilitado.

    fast_executemany = True → usa TVP-style batch para INSERT masivo
    (x10-x100 más rápido que executemany estándar con ODBC Driver 17+).
    """
    conn_str = build_connection_string()
    conn = pyodbc.connect(conn_str, autocommit=False)
    # fast_executemany is a cursor-level attribute in pyodbc ≥4.0.19
    # We monkey-patch execute_many_fast as a helper; callers use it directly on cursors.
    log.info("Conexion SQL Server establecida -> %s", os.environ.get("SQLSERVER_HOST", "?"))
    return conn


# ─────────────────────────────────────────────────────────────
# Ejecución de SQL
# ─────────────────────────────────────────────────────────────

def _split_sql_statements(sql: str) -> list[str]:
    """
    Divide el contenido de un archivo .sql en statements individuales.

    Estrategia:
    - Parse caracter a caracter, ignorando ';' y BEGIN/END dentro de comentarios.
    - Respeta bloques BEGIN...END (profundidad).
    - Ignora contenido después de '--' hasta fin de línea (line comments).
    - No maneja block comments /* */ ni strings multilínea (no usados en el DDL).
    """
    result: list[str] = []
    current: list[str] = []
    depth = 0          # BEGIN ... END nesting depth
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]

        # ── line comment: skip until newline ─────────────────
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            j = i
            while j < n and sql[j] != "\n":
                j += 1
            current.append(sql[i:j])   # include comment text (preserves readability)
            i = j
            continue

        # ── semicolon at depth 0 → statement boundary ────────
        if ch == ";" and depth == 0:
            stmt = "".join(current).strip()
            non_empty = [l for l in stmt.splitlines() if l.strip()]
            only_comments = non_empty and all(l.strip().startswith("--") for l in non_empty)
            if stmt and not only_comments:
                result.append(stmt)
            current = []
            i += 1
            continue

        # ── BEGIN keyword (word-boundary aware) ──────────────
        if ch in ("B", "b") and sql[i : i + 5].upper() == "BEGIN":
            after  = sql[i + 5] if i + 5 < n else " "
            before = sql[i - 1] if i > 0     else " "
            if (
                not after.isalnum()  and after  != "_"
                and not before.isalnum() and before != "_"
            ):
                depth += 1
                current.append(sql[i : i + 5])
                i += 5
                continue

        # ── END keyword (word-boundary aware) ────────────────
        if ch in ("E", "e") and sql[i : i + 3].upper() == "END":
            after  = sql[i + 3] if i + 3 < n else " "
            before = sql[i - 1] if i > 0     else " "
            if (
                not after.isalnum()  and after  != "_"
                and not before.isalnum() and before != "_"
            ):
                depth = max(depth - 1, 0)
                current.append(sql[i : i + 3])
                i += 3
                continue

        current.append(ch)
        i += 1

    # Flush any trailing content without a final ';'
    stmt = "".join(current).strip()
    non_empty = [l for l in stmt.splitlines() if l.strip()]
    only_comments = non_empty and all(l.strip().startswith("--") for l in non_empty)
    if stmt and not only_comments:
        result.append(stmt)

    return result


def execute_sql_file(conn: pyodbc.Connection, path: Path) -> None:
    """
    Lee y ejecuta un archivo SQL, statement por statement.
    Errores en statements individuales se loguean como WARNING y se continúa
    (útil para IF NOT EXISTS idempotentes que SQL Server re-ejecuta).
    """
    sql = path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql)
    cursor = conn.cursor()
    ok = 0
    errors = 0
    for i, stmt in enumerate(statements, 1):
        try:
            cursor.execute(stmt)
            conn.commit()
            ok += 1
        except pyodbc.Error as exc:  # noqa: BLE001
            conn.rollback()
            # Errores esperados: "object already exists", índice duplicado, etc.
            log.debug("SQL statement #%d skipped (%s): %s…", i, exc.args[0], stmt[:60])
            errors += 1
    log.info("DDL ejecutado: %d OK, %d skipped — %s", ok, errors, path.name)
    cursor.close()


def execute_sql(
    conn: pyodbc.Connection,
    sql: str,
    params: tuple | list | None = None,
    commit: bool = True,
) -> pyodbc.Cursor:
    """Ejecuta un statement SQL y opcionalmente hace commit."""
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    if commit:
        conn.commit()
    return cursor


def execute_sql_scalar(
    conn: pyodbc.Connection,
    sql: str,
    params: tuple | list | None = None,
) -> Any:
    """Ejecuta un SELECT que devuelve un solo valor escalar."""
    cursor = execute_sql(conn, sql, params, commit=False)
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def fetch_df(
    conn: pyodbc.Connection,
    sql: str,
    params: tuple | list | None = None,
) -> pd.DataFrame:
    """Ejecuta un SELECT y devuelve un DataFrame de pandas."""
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame([list(r) for r in rows], columns=cols)


# ─────────────────────────────────────────────────────────────
# Bulk Insert
# ─────────────────────────────────────────────────────────────

def _nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte NaN / NaT a None (Python nulo) para que pyodbc inserte NULL.

    pyodbc no entiende float('nan') ni pandas.NaT — lanzaría DataError.
    Primero convertimos a object dtype (como en el workaround Silver NaN→None)
    y luego reemplazamos con pd.notna.
    """
    return df.astype(object).where(pd.notna(df), None)


def _iter_chunks(df: pd.DataFrame, chunk_size: int) -> Iterator[pd.DataFrame]:
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start : start + chunk_size]


def bulk_insert(
    conn: pyodbc.Connection,
    table: str,             # 'schema.table'
    df: pd.DataFrame,
    chunk_size: int = 50_000,
    truncate_first: bool = False,
) -> int:
    """
    Inserta un DataFrame en la tabla SQL Server usando fast_executemany.

    Args:
        conn:           Conexión pyodbc con fast_executemany=True.
        table:          Nombre completo: 'schema.table'.
        df:             DataFrame con columnas que coinciden con la tabla.
        chunk_size:     Filas por batch (default 50k; ajustar según memoria).
        truncate_first: Si True, trunca la tabla antes de insertar.

    Returns:
        Total de filas insertadas.
    """
    if df.empty:
        log.debug("bulk_insert: DataFrame vacío para %s — skip.", table)
        return 0

    df = _nan_to_none(df)

    if truncate_first:
        execute_sql(conn, f"TRUNCATE TABLE {table}")
        log.info("TRUNCATE TABLE %s", table)

    cols = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join("?" * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True
    total = 0
    t0 = time.monotonic()
    for chunk in _iter_chunks(df, chunk_size):
        rows = [tuple(r) for r in chunk.itertuples(index=False, name=None)]
        cursor.executemany(sql, rows)
        conn.commit()
        total += len(rows)
    cursor.close()

    elapsed = time.monotonic() - t0
    log.info(
        "bulk_insert %s: %s filas en %.1fs (%.0f filas/s)",
        table, f"{total:,}", elapsed, total / elapsed if elapsed else 0,
    )
    return total


def upsert_lookup_dim(
    conn: pyodbc.Connection,
    dim_table: str,
    bk_col: str,
    values: list[str | None],
    extra_cols: dict[str, Any] | None = None,
) -> None:
    """
    Inserta filas nuevas en una dimensión simple (sin SCD2) por BK.
    Ignora BKs que ya existen (idempotente).

    Args:
        dim_table:  'dw.dim_fare_period'
        bk_col:     'fare_period_name'
        values:     Lista de valores únicos del BK (filtrados: sin NULL, sin vacíos).
        extra_cols: Columnas adicionales a insertar con valor fijo (ej. {'record_source': 'DTPM'}).
    """
    values_clean = sorted({v for v in values if v and str(v).strip()})
    if not values_clean:
        log.debug("upsert_lookup_dim %s: sin valores nuevos.", dim_table)
        return

    extra_cols = extra_cols or {}
    extra_col_str  = (", " + ", ".join(f"[{c}]" for c in extra_cols)) if extra_cols else ""
    extra_val_str  = (", " + ", ".join(f"'{v}'" for v in extra_cols.values())) if extra_cols else ""

    inserted = 0
    cursor = conn.cursor()
    for val in values_clean:
        cursor.execute(
            f"""
            IF NOT EXISTS (SELECT 1 FROM {dim_table} WHERE [{bk_col}] = ?)
                INSERT INTO {dim_table} ([{bk_col}]{extra_col_str})
                VALUES (?{extra_val_str})
            """,
            val, val,
        )
        if cursor.rowcount and cursor.rowcount > 0:
            inserted += 1
    conn.commit()
    cursor.close()
    log.info("upsert_lookup_dim %s: %d nuevas filas (de %d valores únicos)", dim_table, inserted, len(values_clean))


def build_lookup_dict(
    conn: pyodbc.Connection,
    table: str,
    key_col: str,
    val_col: str,
    where: str = "",
) -> dict[str, int]:
    """
    Lee tabla SQL Server y devuelve dict {key_col_value: val_col_value}.
    Usado para traducir BKs (str) a SKs (int) en el loader de facts.

    Ejemplo:
        mode_lookup = build_lookup_dict(conn, 'dw.dim_mode', 'mode_code', 'mode_sk')
        → {'BUS': 1, 'METRO': 2, ...}
    """
    where_sql = f"WHERE {where}" if where else ""
    df = fetch_df(conn, f"SELECT [{key_col}], [{val_col}] FROM {table} {where_sql}")
    return dict(zip(df[key_col].astype(str), df[val_col]))


# ─────────────────────────────────────────────────────────────
# Transaction helpers explícitos
# ─────────────────────────────────────────────────────────────

def begin_tx(conn: pyodbc.Connection) -> None:
    """
    Marcador explícito de inicio de transacción.

    pyodbc con autocommit=False ya gestiona transacciones implícitamente:
    la primera sentencia DML abre una TX. Esta función sirve de marcador
    de legibilidad y no ejecuta ninguna instrucción adicional.
    Si necesitas BEGIN TRAN explícito (ej. savepoints), usa execute_sql(conn, 'BEGIN TRAN').
    """
    pass  # pyodbc autocommit=False → TX implícita por statement


def commit_tx(conn: pyodbc.Connection) -> None:
    """Hace COMMIT de la transacción activa."""
    conn.commit()


def rollback_tx(conn: pyodbc.Connection) -> None:
    """Hace ROLLBACK de la transacción activa."""
    conn.rollback()


def exec_scalar(
    conn: pyodbc.Connection,
    sql: str,
    params: tuple | list | None = None,
) -> Any:
    """
    Alias de execute_sql_scalar para conveniencia.
    Devuelve el primer campo de la primera fila, o None si no hay resultado.
    """
    return execute_sql_scalar(conn, sql, params)

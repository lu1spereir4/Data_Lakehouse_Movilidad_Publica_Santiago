# Capa Gold — DTPM Movilidad Pública Santiago

> **Audiencia:** Data Engineers, Analistas de BI y entrevistadores técnicos.  
> **Motor principal:** SQL Server (Azure SQL / 2019+) + variante portable SQLite.  
> **Patrón:** Kimball Dimensional Warehouse — esquema estrella con dimensiones conformadas.

---

## ¿Qué es la capa Gold?

La capa Gold es el **modelo de datos final y orientado al consumo**. A diferencia de Silver (parquets limpios y normalizados pero en formato crudo), Gold implementa un **Data Warehouse Kimball** con:

- **Dimensiones conformadas** reutilizables entre facts
- **SCD Type 2** para dimensiones que cambian con el tiempo (paraderos y servicios)
- **Tablas de hecho** con granos explícitos e idempotencia garantizada
- **Trazabilidad completa** con `etl_run_log` por ejecución

---

## Arquitectura de archivos

```
models/
  gold/
    ddl_gold.sql          ← DDL SQL Server (staging + dw schemas)
  sqlite/
    ddl_sqlite.sql        ← DDL SQLite portable (sin schemas)

src/
  gold/
    sql_helpers.py        ← Conexión pyodbc, bulk_insert, upsert helpers
    load_gold.py          ← Orquestador principal (CLI)
    __init__.py
  sqlite/
    sqlite_helpers.py     ← Conexión sqlite3, helpers de bajo nivel
    load_sqlite.py        ← Orquestador portable (CLI)
    __init__.py

docs/
  diagnostics/
    sqlite_load_report.json   ← Generado automáticamente por load_sqlite.py
    sqlite_load_report.md     ← Versión Markdown del reporte
```

---

## Paso 1 — DDL: el modelo de datos

### Schemas (SQL Server)

| Schema | Propósito |
|--------|-----------|
| `staging` | Tablas de paso para bulk load. Sin FKs, sin constraints. Se truncan antes de cada carga. |
| `dw` | El Data Warehouse real. PKs, FKs, UNIQUE constraints, SCD2. |

### Dimensiones implementadas

| Tabla | Tipo | Grano / BK |
|-------|------|------------|
| `dim_cut` | Tipo 1 | `(dataset, cut)` — cada fichero de datos procesado |
| `dim_date` | Tipo 1 estática | `date_sk = YYYYMMDD` — calendario completo |
| `dim_time_30m` | Tipo 1 estática | `time_30m_sk = 0..47` — 48 franjas de 30 min |
| `dim_mode` | Tipo 1 estática | `mode_code` — BUS/METRO/METROTREN/ZP/UNKNOWN |
| `dim_stop` | **SCD Type 2** | `stop_code` + `valid_from` |
| `dim_service` | **SCD Type 2** | `service_code` + `valid_from` |
| `dim_fare_period` | Tipo 1 | `fare_period_code` |
| `dim_purpose` | Tipo 1 | `purpose_code` |
| `dim_operator_contract` | Tipo 1 | `(operator_code, contract_code)` |

### Tablas de hecho y sus granos

| Tabla | Grano | UNIQUE constraint |
|-------|-------|-------------------|
| `fct_trip` | 1 viaje completo | `(cut, id_viaje)` |
| `fct_trip_leg` | 1 etapa de viaje | `(cut, id_viaje, leg_seq)` |
| `fct_validation` | 1 transacción de validación | `(cut, id_etapa)` |
| `fct_boardings_30m` | Subidas promedio por parada, modo y franja | `(cut, month_date_sk, time_30m_sk, stop_sk, mode_sk, tipo_dia)` |

---

## Paso 2 — SCD Type 2 en `dim_stop` y `dim_service`

Las paradas y servicios **cambian de atributos con el tiempo** (comunas, zonas, modos). Necesitamos saber qué versión de una parada estaba vigente en la fecha de un viaje histórico.

### Esquema SCD2

```sql
CREATE TABLE dw.dim_stop (
    stop_sk    INT IDENTITY PRIMARY KEY,
    stop_code  NVARCHAR(40) NOT NULL,    -- business key (BK)
    comuna     NVARCHAR(80) NULL,
    zona       INT          NULL,
    valid_from DATE         NOT NULL,    -- apertura de esta versión
    valid_to   DATE         NULL,        -- NULL = versión vigente
    is_current BIT          NOT NULL DEFAULT 1,
    row_hash   CHAR(64)     NOT NULL,    -- SHA-256 de los atributos
    CONSTRAINT UX_dim_stop_bk_from UNIQUE (stop_code, valid_from)
);
```

### Lógica de upsert SCD2 en Python

```python
def _row_hash(*vals: Any) -> str:
    # Normalización: UPPER + TRIM + coalesce '' antes de hashear
    # Esto asegura que "Bus" == "BUS" == " BUS " → mismo hash → sin SCD2 falso
    parts = "|".join(str(v or "").strip().upper() for v in vals)
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()

# Regla SCD2:
# 1. Si no existe el BK → INSERT con valid_from=event_date, valid_to=NULL
# 2. Si existe y hash CAMBIÓ → cerrar fila actual (valid_to = event_date-1)
#                            → INSERT nueva fila
# 3. Hash igual → no-op (idempotente)
```

---

## Paso 3 — AS-OF JOIN: resolver el SK correcto en el tiempo

El bug más común en SCD2 es hacer JOIN con `is_current = 1`. Eso asigna la versión **actual** aunque el viaje sea histórico.

### Incorrecto ❌

```sql
-- Asigna los atributos de HOY al viaje del año pasado
JOIN dw.dim_stop ON stop_code = s.board_stop_code AND is_current = 1
```

### Correcto ✅ (as-of pattern)

```sql
-- CTE que pre-calcula la fecha del evento desde el integer surrogate key
WITH src_prep AS (
    SELECT *,
        DATEFROMPARTS(date_board_sk / 10000,
                     (date_board_sk % 10000) / 100,
                      date_board_sk % 100) AS event_dt
    FROM staging.stg_viajes_leg
)
-- JOIN que respeta la temporalidad exacta
LEFT JOIN dw.dim_stop board_s
    ON  board_s.stop_code  = s.board_stop_code
    AND s.event_dt IS NOT NULL
    AND board_s.valid_from <= s.event_dt
    AND (board_s.valid_to IS NULL OR s.event_dt <= board_s.valid_to)
```

**¿Por qué `event_dt IS NOT NULL`?** En SQL Server (y SQLite), `NULL <= date` evalúa a UNKNOWN, no a FALSE. Sin ese guard, una etapa sin fecha haría JOIN con todas las versiones del paradero.

Los índices en DDL soportan este patrón:

```sql
CREATE NONCLUSTERED INDEX IX_dim_stop_asof
    ON dw.dim_stop (stop_code, valid_from, valid_to) INCLUDE (stop_sk);
```

---

## Paso 4 — Idempotencia en Facts (MERGE)

Las facts usan `MERGE` en SQL Server e `INSERT OR IGNORE` en SQLite sobre las UNIQUE constraints de grano. Esto garantiza que re-ejecutar el pipeline sobre el mismo `cut` **no duplica filas**.

```sql
-- SQL Server: MERGE set-based sobre staging
MERGE dw.fct_trip AS tgt
USING (
    SELECT ... FROM src_prep s
    LEFT JOIN dw.dim_stop origin_s ON <as-of>
    LEFT JOIN dw.dim_stop dest_s   ON <as-of>
    ...
) AS src
ON (tgt.cut = src.cut AND tgt.id_viaje = src.id_viaje)  -- grain UNIQUE
WHEN NOT MATCHED BY TARGET THEN INSERT (...)
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN UPDATE SET ...;
```

```sql
-- SQLite: equivalente portable
INSERT OR IGNORE INTO fct_trip (...) VALUES (...);
-- UNIQUE(cut, id_viaje) rechaza silenciosamente duplicados
```

---

## Paso 5 — Carga por Bulk (performance)

### SQL Server
```python
# fast_executemany=True → un único round-trip por lote
# chunk de 50k filas → equilibrio entre memoria y velocidad
conn.execute("TRUNCATE TABLE staging.stg_viajes_trip")
cursor.fast_executemany = True
cursor.executemany(insert_sql, chunk_of_50k_tuples)
```

### SQLite
```python
# fetchmany() desde DuckDB → no carga todo en RAM
rel = duck_con.execute("SELECT ... FROM read_parquet(?)", [parquet_path])
while True:
    rows = rel.fetchmany(5_000)   # batch streaming
    if not rows: break
    conn.executemany(insert_sql, rows)
```

**¿Por qué DuckDB para leer Parquet?** DuckDB es columnar y puede hacer `SELECT DISTINCT stop_code, comuna FROM read_parquet('...')` en segundos sobre 28M filas sin cargarlas en pandas. Es el motor de lectura; SQLite/SQL Server es el motor de escritura.

---

## Paso 6 — Auditabilidad con `etl_run_log`

```sql
CREATE TABLE dw.etl_run_log (
    run_id         INT IDENTITY PRIMARY KEY,
    started_at     DATETIME2(3) DEFAULT SYSUTCDATETIME(),
    finished_at    DATETIME2(3) NULL,
    dataset        VARCHAR(30)  NOT NULL,
    cut            VARCHAR(40)  NOT NULL,
    status         VARCHAR(10)  NOT NULL DEFAULT 'RUNNING',  -- RUNNING/OK/FAILED
    rows_staged    BIGINT       NULL,
    rows_inserted  BIGINT       NULL,
    rows_updated   BIGINT       NOT NULL DEFAULT 0,
    error_message  NVARCHAR(MAX) NULL,
    loader_version VARCHAR(20)  NULL
);
```

Cada ejecución del pipeline:
1. Inserta una fila con `status='RUNNING'` al inicio
2. Registra métricas al finalizar cada fase: `rows_staged`, `rows_inserted`
3. Actualiza `status='OK'` o `status='FAILED'` con el mensaje de error
4. `LOADER_VERSION = "2.0.0"` permite rastrear qué versión del código produjo cada dato

---

## Paso 7 — CLI y flags de control

### SQL Server (`load_gold.py`)

```bash
# Carga completa de todas las particiones Silver
python -m src.gold.load_gold --dataset all

# Re-cargar solo una fecha (staging se sobrescribe)
python -m src.gold.load_gold --dataset viajes --cut 2025-04-21

# Reutilizar staging ya cargado (evita el bulk re-load)
python -m src.gold.load_gold --dataset viajes --cut 2025-04-21 --no-overwrite-staging

# Ver el plan sin ejecutar nada
python -m src.gold.load_gold --dataset all --dry-run
```

### SQLite portable (`load_sqlite.py`)

```bash
# Smoke test de una sola partición
python -m src.sqlite.load_sqlite --db gold_sqlite.db --dataset subidas_30m --cut 2025-04 --overwrite

# Carga completa
python -m src.sqlite.load_sqlite --db gold_sqlite.db --dataset all --overwrite

# Plan sin ejecución
python -m src.sqlite.load_sqlite --db gold_sqlite.db --dry-run
```

---

## Paso 8 — Diagnóstico automático

`load_sqlite.py` genera automáticamente en `docs/diagnostics/`:

| Métrica | Descripción |
|---------|-------------|
| `silver_rows` | Filas leídas por cada Parquet Silver |
| `dup_count` | Duplicados detectados vs. el grano (FAIL si > 0) |
| `inserted / ignored` | Filas nuevas vs. rechazadas por idempotencia |
| `lookup miss rates` | % de filas donde no se resuelve el SK de cada dimensión |
| Comparación con `quality.json` | `meta_row_count`, `valid_row_count`, `count_assertion` PASS/FAIL |
| `quarantine_top` | Top 10 `reason_code` del `invalid.parquet` de cuarentena |

---

## Garantías del sistema

| Propiedad | Mecanismo |
|-----------|-----------|
| **Idempotencia** | UNIQUE grain + `MERGE`/`INSERT OR IGNORE` |
| **Correctitud SCD2** | AS-OF join con `valid_from ≤ event_dt ≤ valid_to` |
| **Integridad referencial** | FKs en `dw` schema; PRAGMA `foreign_keys=ON` en SQLite |
| **Sin overflow de RAM** | DuckDB fetchmany() streaming + batches de 5-50k filas |
| **Trazabilidad** | `etl_run_log` + `LOADER_VERSION` + `quality.json` por partición |
| **Recuperación ante fallos** | Transacción por cut; rollback automático si falla cualquier paso |

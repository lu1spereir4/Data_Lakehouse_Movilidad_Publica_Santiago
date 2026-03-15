# Arquitectura Medallion — DTPM Movilidad Pública Santiago

> Pipeline de datos para el sistema de transporte público de Santiago (DTPM).  
> Procesa **+28M transacciones diarias** de validación de tarjeta, viajes y subidas.  
> Patrón: **Bronze → Silver → Gold** (Medallion Architecture).

---

## Diagrama de arquitectura completo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CAPA BRONZE  (Raw)                                 │
│                                                                             │
│  lake/raw/dtpm/                                                             │
│    dataset=viajes/year=2025/month=04/cut=2025-04-21/                       │
│      viajes.csv          ← CSV original DTPM (~3.6M filas/día)             │
│      _meta.json          ← columnas, separador, encoding, row_count        │
│    dataset=etapas/...                                                       │
│      etapas.csv          ← 28M transacciones/semana                        │
│    dataset=subidas_30m/...                                                  │
│      subidas_30m.csv     ← promedio subidas por paradero+30min             │
│                                                                             │
│  Origen: APIs/SFTP DTPM Chile. Sin transformación. Inmutable.              │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
                             │  src/silver/transform_silver.py
                             │  python -m src.silver.transform_silver --dataset all --overwrite
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CAPA SILVER  (Processed)                           │
│                                                                             │
│  lake/processed/dtpm/                                                       │
│    dataset=viajes/year=2025/month=04/cut=2025-04-21/                       │
│      viajes_trip.parquet       ← 1 fila = 1 viaje completo                 │
│      viajes_leg.parquet        ← 1 fila = 1 etapa del viaje                │
│    dataset=etapas/.../                                                      │
│      etapas_validation.parquet ← 1 fila = 1 validación de tarjeta          │
│    dataset=subidas_30m/.../                                                 │
│      subidas_30m.parquet                                                    │
│                                                                             │
│  lake/processed/_quality/...   ← quality.json (assertion PASS/FAIL)        │
│  lake/processed/_quarantine/... ← invalid.parquet + reason_code            │
└──────────┬────────────────────────────────────────────┬─────────────────────┘
           │                                            │
           │ src/gold/load_gold.py                      │ src/sqlite/load_sqlite.py
           │ (SQL Server / Azure SQL)                   │ (SQLite portable)
           ▼                                            ▼
┌──────────────────────────┐             ┌──────────────────────────────────┐
│   CAPA GOLD — SQL Server │             │   CAPA GOLD — SQLite portable    │
│   schema: staging + dw   │             │   gold_sqlite.db                 │
│                          │             │                                  │
│  staging.*               │             │  Mismo modelo dimensional        │
│    stg_viajes_trip       │             │  sin dependencias externas       │
│    stg_viajes_leg        │             │                                  │
│    stg_etapas_validation │             │  Diagnóstico:                    │
│    stg_subidas_30m       │             │  docs/diagnostics/               │
│                          │             │    sqlite_load_report.json       │
│  dw.*                    │             │    sqlite_load_report.md         │
│    dim_cut               │             └──────────────────────────────────┘
│    dim_date              │
│    dim_time_30m          │
│    dim_mode              │
│    dim_stop     (SCD2)   │
│    dim_service  (SCD2)   │
│    dim_fare_period       │
│    dim_purpose           │
│    dim_operator_contract │
│    fct_trip              │
│    fct_trip_leg          │
│    fct_validation        │
│    fct_boardings_30m     │
│    etl_run_log           │
└──────────────────────────┘
```

---

## Diagrama de flujo interno por partición

```
                     Silver Parquet (DuckDB read)
                             │
              ┌──────────────▼──────────────┐
              │   1. Descubrimiento          │  catalog.py / filesystem scan
              │   dataset + cut + year/month │  → SilverPartition dataclass
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   2. Dims estáticas          │  Solo una vez por sesión
              │   dim_date / dim_time_30m    │  YYYYMMDD range de los parquets
              │   dim_mode (BUS/METRO/…)     │  Estáticas hardcoded
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   3. dim_cut                 │  INSERT OR IGNORE (Tipo 1)
              │   quality.json → extracted_at│  (dataset, cut) → cut_sk
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   4. Dims simples (Tipo 1)   │  DISTINCT desde parquets
              │   fare_period, purpose       │  INSERT OR IGNORE
              │   operator_contract          │  code → sk lookup dict
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   5. SCD2: dim_stop          │  Por cada stop_code:
              │           dim_service        │  hash = SHA256(UPPER(TRIM(attrs)))
              │                             │  si hash ≠ actual → cerrar + nueva fila
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   6. Facts (MERGE / IGNORE)  │  Resuelve SKs con as-of join
              │   fct_trip                   │  event_dt desde date_*_sk integer
              │   fct_trip_leg               │  INSERT OR IGNORE por grain UNIQUE
              │   fct_validation             │  Registra miss rates por dimensión
              │   fct_boardings_30m          │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   7. etl_run_log             │  status RUNNING → OK / FAILED
              │   Diagnóstico                │  rows_staged, rows_inserted
              └─────────────────────────────┘
```

---

## Las líneas de código más significativas

### 1. Lectura Bronze sin corrupción silenciosa

```python
# transforms.py — La clave: all-VARCHAR explícito desde _meta.json
# Sin esto, DuckDB infiere tipos y convierte "08:30" → TIME perdiendo
# filas inconsistentes sin avisar (ignore_errors=True)
def _build_varchar_read(csv_path: Path, col_spec: str) -> str:
    return f"""
        read_csv('{csv_path}',
            columns    = {col_spec},   -- {'col1': 'VARCHAR', 'col2': 'VARCHAR', ...}
            delim      = ';',
            header     = true,
            encoding   = 'latin1',
            null_padding = true
        )
    """
```

**Por qué importa:** Leer todo como VARCHAR garantiza que ninguna fila se pierde silenciosamente. La cuarentena es explícita, no accidental.

---

### 2. Quarantine con reason_code explícito en DuckDB

```python
# transforms.py — Cuarentena basada en reglas de negocio
quarantine_sql = f"""
COPY (
    SELECT *, 
        CASE
            WHEN distancia_ruta < 0 THEN 'NEG_DISTANCE'
            WHEN x_subida < 100000 OR x_subida > 900000 THEN 'BAD_UTM_X'
            WHEN n_etapas < 1 OR n_etapas > 4 THEN 'BAD_RANGE_N_ETAPAS'
        END AS _reason_code
    FROM silver_view
    WHERE distancia_ruta < 0
       OR x_subida < 100000 OR x_subida > 900000
       OR n_etapas < 1 OR n_etapas > 4
) TO '{quarantine_path}/invalid.parquet' (FORMAT PARQUET, CODEC 'zstd')
"""
```

**Por qué importa:** Los registros inválidos se preservan con su motivo. No se borran. Un analista puede revisar `invalid.parquet` y el negocio puede decidir si la regla era correcta.

---

### 3. Surrogate key de fecha como integer YYYYMMDD

```python
# transforms.py — date_sk desde timestamp en DuckDB SQL
def _ts_to_date_sk(col: str) -> str:
    return f"CAST(strftime({col}, '%Y%m%d') AS INTEGER)"
    # → 20250421 para 2025-04-21T08:32:11

def _ts_to_time_30m_sk(col: str) -> str:
    return (
        f"(DATEPART('hour', {col}) * 2 + "
        f"CASE WHEN DATEPART('minute', {col}) >= 30 THEN 1 ELSE 0 END)"
    )
    # → 17 para 08:32:xx (franja 08:30-09:00)
```

**Por qué YYYYMMDD y no DATE:** El integer es auto-join con `dim_date`, es compacto, funciona en SQL Server, SQLite y DuckDB, y permite aritmética directa (`sk / 10000 = year`).

---

### 4. Conversión de integer SK a DATE sin strings (AS-OF)

```sql
-- load_gold.py — En el MERGE de facts, convertir el SK a DATE en SQL
-- Sin CONVERT(VARCHAR, ...) que es lento y frágil por locale
WITH src_prep AS (
    SELECT *,
        CASE WHEN date_board_sk IS NOT NULL
             THEN DATEFROMPARTS(
                     date_board_sk / 10000,          -- año
                    (date_board_sk % 10000) / 100,   -- mes
                     date_board_sk % 100             -- día
                 )
             ELSE NULL
        END AS event_dt
    FROM staging.stg_viajes_leg
)
```

```python
# load_sqlite.py — Equivalente en Python para SQLite
def _sk_to_date(sk: Any) -> Optional[str]:
    sk = int(sk)
    y, m, d = sk // 10000, (sk % 10000) // 100, sk % 100
    return datetime.date(y, m, d).isoformat()  # → '2025-04-21'
```

---

### 5. AS-OF JOIN — el corazón del SCD2 correcto

```sql
-- load_gold.py — Resuelve la versión del paradero vigente en la fecha del evento
LEFT JOIN dw.dim_stop board_s
    ON  board_s.stop_code  = s.board_stop_code
    AND s.event_dt         IS NOT NULL          -- guard: NULL event_dt → NULL sk
    AND board_s.valid_from <= s.event_dt        -- versión abierta antes del evento
    AND (board_s.valid_to  IS NULL              -- o no ha cerrado aún
         OR s.event_dt    <= board_s.valid_to)  -- o el evento cae dentro del rango
```

```python
# load_sqlite.py — Equivalente en Python con caché para performance
def resolve_stop(self, conn, code, event_date):
    key = (code.strip().upper(), event_date)
    if key in self._stop_cache:
        return self._stop_cache[key]   # evita N queries por fila
    row = conn.execute("""
        SELECT stop_sk FROM dim_stop
        WHERE stop_code=?
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to >= ?)
        ORDER BY valid_from DESC LIMIT 1
    """, (code, event_date, event_date)).fetchone()
    self._stop_cache[key] = row[0] if row else None
    return self._stop_cache[key]
```

---

### 6. SCD2 row_hash normalizado

```python
# load_gold.py y load_sqlite.py — Idéntica normalización en ambos motores
def _row_hash(*vals: Any) -> str:
    # UPPER + TRIM + coalesce '' → mismo resultado que T-SQL UPPER(RTRIM(LTRIM(COALESCE(x,''))))
    # Sin esto: "Bus " ≠ "BUS" → SCD2 falso positivo → explosión de versiones
    parts = "|".join(str(v or "").strip().upper() for v in vals)
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()
```

---

### 7. Idempotencia en facts

```python
# load_gold.py — MERGE SQL Server (set-based, una sola query para millones de filas)
merge_sql = """
MERGE dw.fct_trip AS tgt
USING (...CTE con as-of joins...) AS src
ON (tgt.cut = src.cut AND tgt.id_viaje = src.id_viaje)  -- grain UNIQUE
WHEN NOT MATCHED BY TARGET THEN
    INSERT (cut_sk, date_start_sk, ...) VALUES (src.cut_sk, ...)
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET ...;
"""

# load_sqlite.py — Equivalente SQLite sin MERGE nativo
"INSERT OR IGNORE INTO fct_trip (...) VALUES (...)"
# UNIQUE(cut, id_viaje) hace el trabajo
```

---

### 8. Streaming Parquet con DuckDB — sin cargar en RAM

```python
# load_sqlite.py — 28M filas de etapas procesadas en batches
def _iter_parquet_batches(duck_con, path, select_sql, batch=5_000):
    rel = duck_con.execute(select_sql, [path])  # cursor DuckDB
    while True:
        rows = rel.fetchmany(batch)   # 5k filas a la vez → ~1MB por batch
        if not rows:
            break
        yield rows   # el llamador hace executemany() en SQLite
```

```python
# El motor de lectura (DuckDB columnar) y escritura (SQLite/SQL Server)
# son deliberadamente distintos:
# DuckDB → óptimo para DISTINCT/GROUP BY sobre Parquet sin índices
# SQLite  → óptimo para writes transaccionales con UNIQUE constraints
duck_con.execute(
    "SELECT DISTINCT stop_code, comuna, zona, date_board_sk FROM read_parquet(?)",
    [parquet_path]
)  # retorna 11.896 paradas únicas de 28M filas en <2 segundos
```

---

### 9. Auditabilidad — etl_run_log

```python
# load_gold.py — Ciclo de vida de una ejecución
def _run_log_start(conn, dataset, cut):
    run_id = exec_scalar(conn, """
        INSERT INTO dw.etl_run_log (dataset, cut, loader_version)
        OUTPUT INSERTED.run_id
        VALUES (?, ?, ?)
    """, (dataset, cut, LOADER_VERSION))
    return run_id   # None si la tabla no existe (non-fatal)

def _run_log_finish(conn, run_id, status, rows_staged, rows_inserted, error=None):
    conn.execute("""
        UPDATE dw.etl_run_log
        SET finished_at   = SYSUTCDATETIME(),
            status        = ?,
            rows_staged   = ?,
            rows_inserted = ?,
            error_message = ?
        WHERE run_id = ?
    """, (status, rows_staged, rows_inserted, error, run_id))
```

---

## Por qué cada decisión técnica

| Decisión | Alternativa descartada | Razón |
|----------|----------------------|-------|
| **all-VARCHAR en Bronze → Silver** | Inferencia de tipos | La inferencia silencia errores; all-VARCHAR expone todos los datos malformados |
| **DuckDB para leer Parquet** | pandas `pd.read_parquet` | pandas carga todo en RAM; DuckDB hace `DISTINCT` en 28M filas con streaming |
| **Integer YYYYMMDD para date_sk** | DATE nativo | Funciona igual en SQL Server, SQLite, DuckDB, Python; aritmética sin CONVERT |
| **DATEFROMPARTS() en vez de CONVERT(VARCHAR)** | `CONVERT(VARCHAR(8), date, 112)` | DATEFROMPARTS no depende del locale del servidor; más rápido |
| **SHA-256 con UPPER+TRIM en SCD2** | Comparación directa campo a campo | Detección de cambio en O(1); normalización evita falsos positivos por mayúsculas/espacios |
| **AS-OF join vs `is_current=1`** | `AND is_current = 1` | `is_current=1` asigna atributos de hoy a viajes históricos: dato incorrecto |
| **MERGE en SQL Server / INSERT OR IGNORE en SQLite** | DELETE + INSERT | MERGE es atómico y set-based; no hay ventana de inconsistencia |
| **Transacción por cut** | Transacción global | Un cut fallido no corrompe los anteriores; recuperación granular |
| **etl_run_log en DB** | Logs en fichero | Consultable con SQL: `WHERE status='FAILED'` en un dashboard |
| **batch_size = 5.000 filas** | 1 fila / 50k filas | 1 fila: mucho overhead de red. 50k: presión de memoria en SQLite WAL |

---

## Órdenes de magnitud de los datos

| Dataset | Filas/cut | Frecuencia | Fact resultante |
|---------|-----------|------------|-----------------|
| `viajes` (trip) | ~3.6M | Diario | `fct_trip` |
| `viajes` (leg) | ~14.5M | Diario | `fct_trip_leg` |
| `etapas` | ~28.3M | Semanal | `fct_validation` |
| `subidas_30m` | ~747k | Mensual | `fct_boardings_30m` |

**Cuarentena típica (etapas):** ~107k registros (`NEG_DISTANCE` + `BAD_UTM_X`) → 0.376% — dentro del umbral aceptable de calidad.

---

## Cómo defender esto en una entrevista

### "¿Por qué Medallion y no un DW clásico directo?"

> El DW clásico requiere que la fuente esté limpia antes de llegar. Aquí la fuente son CSVs crudos de sistemas de validación de tarjeta que tienen registros con UTM mal codificados, distancias negativas y nulos en campos obligatorios. Medallion nos permite separar la responsabilidad: Bronze preserva el origen sin modificar, Silver lo limpia con reglas de negocio explícitas y cuarentena auditada, Gold consume datos ya garantizados.

### "¿Qué garantiza que no hay duplicados?"

> El grano de cada fact está definido con un `UNIQUE constraint` en la tabla. `MERGE`/`INSERT OR IGNORE` lo hacen cumplir de forma atómica. Si re-ejecuto el mismo cut dos veces, el segundo pasa sin insertar nada: `inserted=0, ignored=N`. Lo puedo verificar en `etl_run_log`.

### "¿Cómo resuelves el SCD2 correctamente?"

> Con un AS-OF join: `valid_from ≤ event_date AND (valid_to IS NULL OR event_date ≤ valid_to)`. El `event_date` se calcula desde el `date_board_sk` (integer YYYYMMDD) con aritmética pura: `sk/10000` es el año, `(sk%10000)/100` el mes, `sk%100` el día. Sin conversión a string, sin locale dependency. El índice `IX_dim_stop_asof(stop_code, valid_from, valid_to) INCLUDE(stop_sk)` hace que esta búsqueda sea O(log n).

### "¿Por qué DuckDB y no Spark?"

> Para este volumen (28M filas/semana, procesamiento batch), DuckDB en un solo proceso es suficiente y tiene menos complejidad operacional que un cluster Spark. DuckDB ejecuta `SELECT DISTINCT stop_code, comuna FROM read_parquet('28M_filas.parquet')` en <2 segundos con un solo hilo. Si el volumen escala a decenas de billones de filas, el mismo código de Silver funciona sobre Delta Lake con Spark SQL porque la lógica está en SQL estándar, no en APIs propietarias.

### "¿Cómo aseguras la calidad del dato?"

> En tres capas: (1) all-VARCHAR en Bronze evita que DuckDB descarte filas por inferencia de tipo. (2) La cuarentena en Silver separa inválidos con `_reason_code` explícito en `invalid.parquet` — el dato incorrecto se preserva, no se borra. (3) `quality.json` contiene `valid_row_count`, `invalid_row_count` y `count_assertion: PASS/FAIL` que el loader Gold compara automáticamente. (4) En Gold, los miss rates de lookup se loguean por dimensión: si el 10% de los viajes no resuelve `stop_sk`, es una alerta de que la dimensión tiene cobertura insuficiente.

### "¿Qué harías diferente a escala de producción real?"

> Tres cosas: (1) Reemplazar el catalog.py de filesystem scan con un metastore (Unity Catalog o Glue) para descubrimiento centralizado. (2) Añadir Delta Lake con `MERGE` nativo en lugar de Parquet inmutable, para soportar actualizaciones late-arriving en Silver. (3) Orquestar con Airflow/Prefect en vez de CLI manual, con DAGs que representen las dependencias: `transform_silver >> load_dims >> load_facts`.

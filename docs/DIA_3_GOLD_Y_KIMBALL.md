# Día 3 — Capa Gold: Kimball, Star Schema, SCD2 e Idempotencia
> **Plan**: 10 horas · SQL Server · MERGE · SCD2 · Grain · Idempotencia  
> **Meta**: Explicar cada tabla del DW, cada decisión de diseño, incluyendo el debugging real de un grain bug.

---

## Bloque 0 — Kimball y el modelo dimensional: fundamentos (1 h)

### 0.1 ¿Qué es el modelo dimensional (Kimball)?

El modelo dimensional es una técnica de diseño de Data Warehouses del libro *"The Data Warehouse Toolkit"* de Ralph Kimball (1996, revisado 2013). Su premisa: los datos analíticos se organizan mejor en **hechos** y **dimensiones**.

```
              dim_date ─────────────────────────────────┐
              dim_stop (SCD2)                            │
              dim_service (SCD2)                         │
              dim_mode                                   │
              dim_fare_period                            │
              dim_purpose                                │
              dim_operator_contract         ┌────────────▼──────────────┐
              dim_cut                 ────► │      fct_trip             │
              dim_time_30m            ────► │   (grain: 1 viaje)        │
                                            │   3.6M filas /día         │
                                            └─────────────┬─────────────┘
                                                          │ FK trip_sk
                                            ┌─────────────▼─────────────┐
                                            │      fct_trip_leg         │
                                            │   (grain: 1 etapa/viaje)  │
                                            │   14.4M filas /día        │
                                            └───────────────────────────┘
```

**Ventajas del star schema**:
1. **Intuitividad**: Business users entienden "viajes × parada × tiempo"
2. **Performance**: Joins simples (PK/FK enteros), índices bien definidos
3. **Dimensiones conformadas**: `dim_date` es la misma para `fct_trip` y `fct_boarding_30m`
4. **Additividad**: Puedes sumar `n_etapas` en cualquier combinación de dimensiones

### 0.2 Facts vs Dimensions — la distinción clave

**Dimension table** (dim):
- Describe quién/qué/dónde/cuándo
- Cambio lento (una parada de metro no cambia su nombre cada día)
- Pocos registros por columna, muchas columnas
- `dim_stop`: código parada, nombre, comuna, zona, coordenadas UTM

**Fact table** (fct):
- Mide qué pasó (hechos numéricos)
- Crece constantemente (un nuevo viaje cada segundo)
- Muchos registros, pocas columnas (las medidas + FKs a dims)
- `fct_trip`: n_etapas, tviaje_min, distancia_eucl_m, distancia_ruta_m

**Dimensión degenerada** (degenerate dimension — DD):
- Atributos descriptivos que van en la fact table en lugar de en una dim
- Razón: cardinalidad muy alta (millones de valores únicos) → la dim sería tan grande como la fact
- En este proyecto: `id_viaje`, `id_tarjeta`, `tipo_dia` en `fct_trip`

### 0.3 Surrogate Keys vs Business Keys

| Tipo | Ejemplo | Dónde |
|------|---------|-------|
| **Business Key (BK)** | `"stop_code='PA001'"` | Fuente, mundo real |
| **Surrogate Key (SK)** | `stop_sk = 4521` | Solo en el DW |

**¿Por qué SK?**
1. Los BKs pueden cambiar (DTPM renombra una parada)
2. Los BKs pueden repetirse en fuentes diferentes
3. Los enteros son más rápidos en JOINs que strings
4. SCD2: la misma parada tiene múltiples filas con distintos SK pero el mismo BK

---

## Bloque 1 — El schema completo del DW (1.5 h)

### 1.1 Los 4 schemas / capas del DDL

```sql
-- 18 tablas totales:
--   staging.*   → 4 tablas de paso (truncar+cargar por cut)
--   dw.*        → 9 dims + 4 facts + 1 audit log
```

#### STAGING — buffer de carga sin constraints

```sql
staging.stg_viajes_trip       -- de viajes_trip.parquet
staging.stg_viajes_leg        -- de viajes_leg.parquet
staging.stg_etapas_validation -- de etapas_validation.parquet
staging.stg_subidas_30m       -- de subidas_30m.parquet
```

**Características**:
- No tienen Foreign Keys (velocidad de inserción)
- Se truncan antes de cada carga (`TRUNCATE TABLE`)
- Tipos amplios (`NVARCHAR(80)`, `FLOAT`) para absorber cualquier valor Silver
- `stg_loaded_at DATETIME2(0) DEFAULT SYSUTCDATETIME()` para trazabilidad

#### DIMENSIONS — 9 dimensiones conformadas

```
dim_date           → Calendario (date_sk = INT YYYYMMDD, no identity)
dim_time_30m       → 48 slots de 30 minutos (estático)
dim_mode           → 5 modos: BUS, METRO, METROTREN, ZP, UNKNOWN
dim_stop           → Paraderos [SCD2]
dim_service        → Servicios DTPM [SCD2]
dim_operator_contract → Operadores y contratos
dim_fare_period    → Periodos tarifarios (Punta, Valle, …)
dim_purpose        → Propósito del viaje (Trabajo, Estudio, …)
dim_cut            → Metadatos de cada corte Silver
```

#### FACTS — 4 tablas de hechos

```
fct_trip           → 1 viaje completo (OD matrix)
fct_trip_leg       → 1 etapa/tramo de un viaje
fct_validation     → 1 validación de tarjeta (de etapas)
fct_boarding_30m   → Subidas promedio cada 30 min por parada
```

### 1.2 `dim_date` — Dimensión fecha especial

```sql
CREATE TABLE dw.dim_date (
    date_sk     INT         NOT NULL,   -- YYYYMMDD (20250421) — PK natural, no IDENTITY
    full_date   DATE        NOT NULL,
    year        SMALLINT    NOT NULL,
    month       TINYINT     NOT NULL,
    day         TINYINT     NOT NULL,
    iso_week    TINYINT     NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,   -- 'Lunes', 'Martes', …
    month_name  VARCHAR(10) NOT NULL,   -- 'Enero', 'Febrero', …
    is_weekend  BIT         NOT NULL,
    year_month  CHAR(7)     NOT NULL,   -- '2025-04'
    tipo_dia    VARCHAR(10) NULL,       -- 'LABORAL','SABADO','DOMINGO'
    CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_sk)
);
```

**¿Por qué `date_sk = INT YYYYMMDD` y no `IDENTITY`?**
- Es determinista: `20250421 → 21 de abril de 2025` sin necesidad de join
- Compatible con fuentes externas (cualquier equipo puede generar el SK correcto)
- El Gold loader puede insertar fechas de cualquier fuente sin "registrarlas" primero

**Cómo el loader genera filas de `dim_date`**:
```python
def _upsert_dim_date(cur: pyodbc.Cursor, dates: set[int]) -> None:
    """Inserta fechas (YYYYMMDD) faltantes en dim_date."""
    for date_sk in sorted(dates):
        year  = date_sk // 10000
        month = (date_sk // 100) % 100
        day   = date_sk % 100
        full  = datetime.date(year, month, day)
        dow   = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"][full.weekday()]
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dw.dim_date WHERE date_sk = ?)
            INSERT INTO dw.dim_date (date_sk, full_date, year, month, day,
                                     iso_week, day_of_week, month_name, is_weekend, year_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, date_sk, date_sk, full, year, month, day,
             full.isocalendar()[1], dow, full.strftime("%B"),
             1 if full.weekday() >= 5 else 0, f"{year}-{month:02d}")
```

### 1.3 `dim_cut` — Dimensión de metadatos de corte

```sql
dw.dim_cut (
    cut_sk       INT IDENTITY,
    dataset_name VARCHAR(30),   -- 'viajes', 'etapas', 'subidas_30m'
    cut_id       VARCHAR(40),   -- '2025-04-21', '2025-04'
    year         SMALLINT,
    month        TINYINT,
    source_file  VARCHAR(200),
    extracted_at DATETIME2(0),  -- del quality.json (generated_at)
    row_count    BIGINT,        -- del quality.json (read_row_count)
    ...
    CONSTRAINT UQ_dim_cut UNIQUE (dataset_name, cut_id)
)
```

`dim_cut` permite responder: "¿Cuántas filas tenía el CSV de viajes del 21 de abril?" directamente en SQL, sin ir al filesystem. Los `cut_sk` son FK en todas las fact tables — es la "dimensión de snapshot".

---

## Bloque 2 — SCD Tipo 2: el corazón del historial (2 h)

### 2.1 ¿Qué es SCD?

**SCD** = Slowly Changing Dimension (Dimensión de Cambio Lento). Define cómo manejar cambios en atributos de una dimensión.

| Tipo | Estrategia | Trade-off |
|------|-----------|-----------|
| **SCD0** | No se actualiza (ignora cambios) | Más simple, pierde historia |
| **SCD1** | Overwrite (sobrescribe el valor actual) | Pierde historia, fácil |
| **SCD2** | Nueva fila por cada cambio | Preserva historia completa, más complejo |
| **SCD3** | Columna adicional "valor anterior" | Historia parcial (solo 1 cambio) |
| **SCD4** | Tabla de historia separada | Complejo, raramente usado |

**Este proyecto usa SCD2 en `dim_stop` y `dim_service`.**

### 2.2 Estructura SCD2 en `dim_stop`

```sql
dw.dim_stop (
    stop_sk     BIGINT IDENTITY(1,1),  -- SK generado por el DW
    stop_code   NVARCHAR(40)  NOT NULL,-- BK: código DTPM del paradero
    comunal     NVARCHAR(80)  NULL,    -- atributo tracked
    zone_code   VARCHAR(20)   NULL,    -- atributo tracked
    x_utm       INT           NULL,    -- atributo tracked
    y_utm       INT           NULL,    -- atributo tracked
    -- SCD2 columns:
    row_hash    CHAR(64)      NOT NULL, -- SHA2_256 de atributos concatenados
    valid_from  DATE          NOT NULL, -- fecha de vigencia inicio
    valid_to    DATE          NULL,     -- NULL = registro vigente actualmente
    is_current  BIT           NOT NULL DEFAULT 1,
    is_active   BIT           NOT NULL DEFAULT 1,
    record_source VARCHAR(30) NOT NULL DEFAULT 'DTPM',
    loaded_at   DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
)
```

**Estado de una parada en SCD2**:
```
stop_sk | stop_code | comuna        | valid_from | valid_to   | is_current
-------- ----------- --------------- ------------ ------------ -----------
  1001  | 'PA001'   | 'SANTIAGO'    | 2020-01-01 | 2024-12-31 | 0  ← histórico
  2847  | 'PA001'   | 'PROVIDENCIA' | 2025-01-01 | NULL       | 1  ← actual
```

Cuando la parada cambió de SANTIAGO a PROVIDENCIA, no se actualizó la fila 1001. Se cerró (`valid_to = 2024-12-31`, `is_current = 0`) y se creó una nueva fila 2847 con los nuevos valores.

### 2.3 `row_hash` — Detectar cambios eficientemente

```sql
-- En la lógica de carga, detectar si los atributos cambiaron:
SELECT
    stop_code,
    CONVERT(CHAR(64),
        HASHBYTES('SHA2_256',
            ISNULL(CAST(comunal AS NVARCHAR(MAX)), '') + '|' +
            ISNULL(CAST(zone_code AS VARCHAR(MAX)), '') + '|' +
            ISNULL(CAST(x_utm AS VARCHAR(20)), '') + '|' +
            ISNULL(CAST(y_utm AS VARCHAR(20)), '')
        ), 2
    ) AS row_hash
FROM staging.stg_viajes_trip
```

Si el nuevo `row_hash` ≠ `row_hash` del registro actual → hay cambio → aplicar SCD2.

**¿Por qué hash y no comparar columna a columna?**
- Una tabla con 20 atributos tracked requeriría `WHERE col1 != col1_new OR col2 != col2_new OR ...` → complejo y lento
- Un solo `WHERE row_hash != new_hash` es un solo comparison
- El hash también sirve como signature de la fila para auditoría

### 2.4 `scd2_upsert` — Implementación Python

```python
def scd2_upsert(
    cur: pyodbc.Cursor,
    table: str,
    bk_col: str,
    hash_col: str,
    new_rows: list[dict],
    cut_date: datetime.date,
) -> tuple[int, int]:
    """
    Implementa SCD2: inserta filas nuevas, cierra filas existentes si cambiaron.
    Retorna (insertadas, actualizadas).
    """
    inserted = updated = 0
    
    for row in new_rows:
        bk_val  = row[bk_col]
        new_hash = row[hash_col]
        
        # 1. Buscar si existe un registro actual
        cur.execute(f"""
            SELECT {hash_col} 
            FROM {table}
            WHERE {bk_col} = ? AND is_current = 1
        """, bk_val)
        existing = cur.fetchone()
        
        if existing is None:
            # Caso A: Fila nueva (el BK no existe)
            cur.execute(f"""
                INSERT INTO {table} ({', '.join(row.keys())}, valid_from, is_current)
                VALUES ({', '.join('?' * len(row))}, ?, 1)
            """, *row.values(), cut_date)
            inserted += 1
            
        elif existing[0] != new_hash:
            # Caso B: BK existe pero atributos cambiaron → SCD2
            # Primero cerrar el registro actual
            cur.execute(f"""
                UPDATE {table}
                SET valid_to = ?, is_current = 0
                WHERE {bk_col} = ? AND is_current = 1
            """, cut_date - datetime.timedelta(days=1), bk_val)
            
            # Luego insertar nuevo registro con nuevos valores
            cur.execute(f"""
                INSERT INTO {table} ({', '.join(row.keys())}, valid_from, is_current)
                VALUES ({', '.join('?' * len(row))}, ?, 1)
            """, *row.values(), cut_date)
            updated += 1
            
        # Caso C: BK existe, mismo hash → no hacer nada (idempotente)
    
    return inserted, updated
```

### 2.5 As-of join con SCD2

Para joinear un hecho con la dimensión SCD2 **en el momento en que ocurrió el hecho**:

```sql
-- ❌ JOIN incorrecto: siempre usa la versión actual
SELECT t.trip_sk, s.comuna
FROM dw.fct_trip t
JOIN dw.dim_stop s ON s.stop_code = t.paradero_origen
WHERE s.is_current = 1

-- ✅ JOIN correcto (as-of): usa la versión vigente cuando ocurrió el viaje
SELECT t.trip_sk, s.comuna
FROM dw.fct_trip t
JOIN dw.dim_date d ON d.date_sk = t.date_start_sk
JOIN dw.dim_stop s ON s.stop_code = t.paradero_origen
  AND s.valid_from <= d.full_date
  AND (s.valid_to IS NULL OR s.valid_to >= d.full_date)
```

**Índice para acelerar el as-of join**:
```sql
-- Índice filtrado en is_current=1 (la mayoría de los joins son con registros vigentes):
CREATE NONCLUSTERED INDEX IX_dim_stop_current
    ON dw.dim_stop (stop_code, stop_sk)
    WHERE is_current = 1;
```

---

## Bloque 3 — Grain: la lección más valiosa del proyecto (1.5 h)

### 3.1 ¿Qué es el grain de una fact table?

El **grain** (grano) define exactamente qué representa una fila en la fact table. Es la declaración más importante del diseño de un DW.

> "Una fila en `fct_trip` representa... UN viaje completo (desde paradero origen hasta destino) realizado por UNA tarjeta específica EN UN corte diario dado."

**El grain determina**:
- La clave única de negocio (no PK surrogado)
- Qué medidas son aditivas y en qué nivel
- Qué preguntas se pueden responder directamente

### 3.2 El bug real del proyecto: grain incorrecto

**Situación inicial (incorrecta)**:
```python
# Se asumió que id_viaje era un ID global único por día
# → Grain definido como: UNIQUE(id_viaje, cut_sk)
```

**Resultado catastrófico**:
```
fct_trip: 27 filas insertadas (de 520,000 esperadas)
```

**¿Qué pasó?** `id_viaje` es un contador por tarjeta Bip! que va de 1 a 27 (máximo 27 viajes en un día). Para 520,000 viajes, los ID son:

```
Tarjeta A: id_viaje = 1, 2, 3, ..., 12
Tarjeta B: id_viaje = 1, 2, 3, ...,  8
Tarjeta C: id_viaje = 1, 2, 3, ..., 27   ← máximo
```

Con `UNIQUE(id_viaje, cut_sk)`, solo podían entrar 27 filas diferentes (una por cada valor de id_viaje). La 28ª inserción violaba el constraint.

### 3.3 El diagnóstico

```sql
-- ¿Por qué solo 27 filas?
SELECT id_viaje, COUNT(*) as cnt
FROM staging.stg_viajes_trip
GROUP BY id_viaje
ORDER BY id_viaje;
-- → id_viaje va de 1 a 27 con millones de repeticiones por valor

-- Confirmación: el grain correcto es (tarjeta + contador_diario + corte)
SELECT id_tarjeta, id_viaje, COUNT(*) as cnt
FROM staging.stg_viajes_trip
WHERE id_tarjeta IS NOT NULL
GROUP BY id_tarjeta, id_viaje
HAVING COUNT(*) > 1;
-- → 0 filas (cada tarjeta tiene el id_viaje único dentro de su día)
```

### 3.4 La corrección

```sql
-- ❌ Constraint incorrecto (eliminado)
-- UNIQUE(id_viaje, cut_sk)

-- ✅ Índice filtrado correcto (creado)
CREATE UNIQUE NONCLUSTERED INDEX UX_fct_trip_grain
    ON dw.fct_trip (cut_sk, id_tarjeta, id_viaje)
    WHERE id_tarjeta IS NOT NULL;
```

**¿Por qué `WHERE id_tarjeta IS NOT NULL`?**
- Los viajes en efectivo (sin tarjeta Bip!) tienen `id_tarjeta = NULL`
- No se puede garantizar unicidad de viajes en efectivo (no hay BK)
- Los filtramos del fact (no contamos métricas de efectivo)
- El índice filtrado excluye las filas NULL → NULL no viola el constraint

**Resultado post-fix**:
```
fct_trip:     3,605,891 filas insertadas ✅
fct_trip_leg: 14,423,564 filas insertadas ✅
```

### 3.5 Lecciones críticas del grain bug

1. **Siempre verifica el grain en los datos reales** antes de definir constraints. Un `SELECT id_viaje, COUNT(*)` habría revelado el problema.
2. **El grain es un enunciado de negocio**, no técnico. La pregunta es: "¿Qué REPRESENTA una fila?" no "¿Qué columnas son únicas?"
3. **Los índices filtrados son herramienta poderosa** cuando hay valores NULL o excepciones al grain.
4. **Los contadores por entidad son peligrosos**: `id_viaje` se repite globalmente porque es un contador diario *por tarjeta*.

---

## Bloque 4 — MERGE: el corazón del pipeline idempotente (1.5 h)

### 4.1 ¿Qué es idempotencia?

> Una operación es **idempotente** si ejecutarla múltiples veces produce el mismo resultado que ejecutarla una sola vez.

```
primera ejecución:   3,605,891 filas insertadas
segunda ejecución:   0 filas insertadas (ya existían)
tercera ejecución:   0 filas insertadas (ya existían)
```

Sin idempotencia, un pipeline que se re-ejecuta por cualquier razón (fallo de red, timeout, debugging) duplica los datos.

### 4.2 El patrón MERGE en SQL Server

```sql
MERGE dw.fct_trip AS tgt
USING (
    -- Fuente: los datos de staging deduplicados por grain
    SELECT DISTINCT
        s.cut_sk,
        s.id_tarjeta,
        s.id_viaje,
        s.n_etapas,
        s.tviaje_min,
        s.distancia_ruta_m,
        -- ... otras medidas
    FROM staging.stg_viajes_trip stg
    JOIN dw.dim_cut dc ON dc.dataset_name = 'viajes' AND dc.cut_id = stg.cut
    WHERE stg.id_tarjeta IS NOT NULL   -- excluir viajes en efectivo
) AS src
ON (
    -- Grain: identifica la fila en target
    tgt.cut_sk    = src.cut_sk
    AND tgt.id_tarjeta = src.id_tarjeta
    AND tgt.id_viaje   = src.id_viaje
)
WHEN NOT MATCHED BY TARGET THEN
    -- Fila nueva: insertar
    INSERT (cut_sk, id_tarjeta, id_viaje, n_etapas, tviaje_min, ...)
    VALUES (src.cut_sk, src.id_tarjeta, src.id_viaje, src.n_etapas, src.tviaje_min, ...)
WHEN MATCHED THEN
    -- Fila existe: no hacer nada (idempotente) o actualizar si es SCD1
    -- Para facts, generalmente no se actualiza
    UPDATE SET tgt.loaded_at = SYSUTCDATETIME();   -- solo para registrar re-load
```

**`WHEN MATCHED THEN UPDATE`**: En factories de hechos históricos (como viajes, que no cambian una vez validados), podrías usar `WHEN MATCHED THEN DELETE` (eliminar y reinsertar) o simplemente no hacer nada (`WHEN MATCHED THEN UPDATE SET loaded_at = ...`). El proyecto usa UPDATE dummy para idempotencia sin pérdida de datos.

### 4.3 Preflight diagnostics antes del MERGE

```python
def _preflight_check(cur: pyodbc.Cursor, cut: str, dataset: str) -> dict:
    """Diagnósticos antes del MERGE para detectar anomalías."""
    cur.execute("""
        SELECT COUNT(*) as stg_rows, COUNT(DISTINCT id_viaje) as distinct_viaje
        FROM staging.stg_viajes_trip
        WHERE cut = ?
    """, cut)
    row = cur.fetchone()
    
    cur.execute("""
        SELECT COUNT(*) as existing_rows
        FROM dw.fct_trip t
        JOIN dw.dim_cut c ON c.cut_sk = t.cut_sk
        WHERE c.cut_id = ? AND c.dataset_name = ?
    """, cut, dataset)
    existing = cur.fetchone()
    
    return {
        "stg_rows": row.stg_rows,
        "distinct_viaje": row.distinct_viaje,
        "existing_in_fct": existing.existing_rows,
    }
```

**¿Para qué?** Si `stg_rows = 27` (el bug anterior), el preflight revela que algo está mal antes de ejecutar el MERGE costoso. Mucho más fácil detectar en preflight que debuggear después.

### 4.4 El flujo completo de `load_gold.py`

```python
class GoldLoader:
    def __init__(self, partition: SilverPartition, conn: pyodbc.Connection) -> None:
        self.partition = partition
        self.conn = conn
    
    def run(self) -> None:
        with self.conn:   # context manager → commit on success, rollback on exception
            # 1. Leer Parquet Silver con pandas
            df_trip = pd.read_parquet(self.partition.trip_parquet_path)
            df_leg  = pd.read_parquet(self.partition.leg_parquet_path)
            
            # 2. Upsert dim_cut (get or create cut_sk)
            cut_sk = self._upsert_dim_cut()
            
            # 3. Upsert dims conformadas (date, time, stop, service, ...)
            self._upsert_simple_dims(df_trip)
            self._scd2_upsert_stops(df_trip, df_leg)
            self._scd2_upsert_services(df_leg)
            
            # 4. Bulk load staging → truncar y recargar
            self._truncate_staging()
            bulk_insert(self.conn.cursor(), "staging.stg_viajes_trip", df_trip)
            bulk_insert(self.conn.cursor(), "staging.stg_viajes_leg",  df_leg)
            
            # 5. MERGE staging → facts
            n_trip = self.merge_fct_trip(cut_sk)
            n_leg  = self.merge_fct_trip_leg(cut_sk)
            
            # 6. Audit log
            self._run_log_finish(n_trip, n_leg)
```

### 4.5 `bulk_insert` con `fast_executemany`

```python
def bulk_insert(
    cur: pyodbc.Cursor,
    table: str,
    df: pd.DataFrame,
    chunk_size: int = 5000,
) -> None:
    """
    Inserta un DataFrame en una tabla SQL Server vía bulk insert.
    fast_executemany = True → usa TVP-style batch (x10-x100 más rápido).
    """
    cur.fast_executemany = True   # pyodbc ≥4.0.19

    cols   = list(df.columns)
    placeholders = ", ".join("?" * len(cols))
    sql    = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"

    # Chunk para no enviar todo a la vez (memoria)
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        # .itertuples(index=False) es más rápido que .to_dict("records")
        cur.executemany(sql, chunk.values.tolist())
    
    log.info("bulk_insert: %d filas → %s", len(df), table)
```

**`fast_executemany = True`**: En lugar de enviar cada INSERT individualmente (`n` round-trips al servidor), pyodbc los agrupa en un solo TVP (Table-Valued Parameter) batch. Para 500k filas: de ~60 segundos a ~6 segundos.

---

## Bloque 5 — Índices y optimización SQL Server (1 h)

### 5.1 Tipos de índices usados

```sql
-- Clustered: determina el orden físico de los datos
CONSTRAINT PK_fct_trip PRIMARY KEY CLUSTERED (trip_sk)
-- Una tabla solo puede tener 1 clustered index

-- Nonclustered: árbol B+ separado, punteros al clustered
CREATE NONCLUSTERED INDEX IX_fct_trip_date_start
    ON dw.fct_trip (date_start_sk, cut_sk)
    INCLUDE (origin_stop_sk, dest_stop_sk, n_etapas);
-- INCLUDE: columnas extra que van en el nivel hoja del índice
-- (evita "key lookup" al servidor para esas columnas)

-- Filtered: solo indexa filas que cumplan la condición
CREATE UNIQUE NONCLUSTERED INDEX UX_fct_trip_grain
    ON dw.fct_trip (cut_sk, id_tarjeta, id_viaje)
    WHERE id_tarjeta IS NOT NULL;
-- Solo las ~3.6M filas con tarjeta entran al índice
-- Las filas de efectivo (NULL id_tarjeta) no

-- Columnstore (comentado en DDL, para referencia futura)
-- CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_fct_trip
--     ON dw.fct_trip (date_start_sk, origin_stop_sk, ...);
-- Incompatible con FKs enforced en SQL Server
```

### 5.2 Cuándo usar qué índice

| Scenario | Índice recomendado |
|---------|-------------------|
| Grain / unicidad con NULLs | Filtered Unique Nonclustered |
| JOIN frecuente con FK + columnas adicionales | Nonclustered con INCLUDE |
| PK → orden físico de datos | Clustered |
| Análisis de rangos amplios (fact grande) | Columnstore |
| Lookup puntual por BK en dims | Nonclustered en BK |

### 5.3 El truco del índice filtrado en dim_stop

```sql
-- Para encontrar la parada vigente en un as-of join:
CREATE NONCLUSTERED INDEX IX_dim_stop_current
    ON dw.dim_stop (stop_code, stop_sk)
    WHERE is_current = 1;
```

Si `dim_stop` tiene 50,000 BKs y promedialmente 2 versiones SCD2 cada una → 100,000 filas. Pero `WHERE is_current = 1` tiene solo 50,000. El índice filtrado es el doble de pequeño, queries de `is_current = 1` lo usan automáticamente.

---

## Bloque 6 — Audit log y trazabilidad (`etl_run_log`) (30 min)

```sql
CREATE TABLE dw.etl_run_log (
    run_id              BIGINT       NOT NULL IDENTITY(1,1),
    dataset_name        VARCHAR(30)  NOT NULL,
    cut_id              VARCHAR(40)  NOT NULL,
    run_start           DATETIME2(0) NOT NULL,
    run_end             DATETIME2(0) NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'RUNNING',  -- RUNNING/OK/ERROR
    rows_staging        BIGINT       NULL,
    rows_fct_inserted   BIGINT       NULL,
    rows_fct_updated    BIGINT       NULL,
    ignored_cash_rows   BIGINT       NULL,    -- viajes en efectivo sin grain
    error_msg           NVARCHAR(MAX) NULL,
    CONSTRAINT PK_etl_run_log PRIMARY KEY CLUSTERED (run_id)
);
```

**Flujo de uso**:
```python
# Al inicio del run:
cur.execute("""
    INSERT INTO dw.etl_run_log (dataset_name, cut_id, run_start, status)
    OUTPUT INSERTED.run_id
    VALUES (?, ?, SYSUTCDATETIME(), 'RUNNING')
""", dataset, cut_id)
run_id = cur.fetchone()[0]

# Al finalizar exitosamente:
cur.execute("""
    UPDATE dw.etl_run_log
    SET run_end = SYSUTCDATETIME(), status = 'OK',
        rows_fct_inserted = ?, ignored_cash_rows = ?
    WHERE run_id = ?
""", n_inserted, n_cash_ignored, run_id)

# Si hay error:
cur.execute("""
    UPDATE dw.etl_run_log
    SET run_end = SYSUTCDATETIME(), status = 'ERROR', error_msg = ?
    WHERE run_id = ?
""", str(exc), run_id)
```

**¿Para qué sirve?**
1. Re-procesar cortes fallidos (`WHERE status = 'ERROR'`)
2. Detectar cortes que no se cargaron (`cut_id` no en `etl_run_log`)
3. Auditores externos pueden verificar cuándo se cargó cada dato
4. SLAs: ¿cuánto tarda cada dataset? (`run_end - run_start`)

---

## Bloque 7 — `sql_helpers.py` y la conexión a SQL Server (30 min)

### 7.1 `_split_sql_statements` — Parser de SQL

El archivo `ddl_gold.sql` tiene 730 líneas con múltiples statements separados por `;`. `cursor.execute()` en pyodbc no acepta múltiples statements. La solución: un parser propio.

```python
def _split_sql_statements(sql: str) -> list[str]:
    """
    Divide SQL en statements individuales respetando:
    - BEGIN...END blocks (no partir por ';' internos)
    - Comentarios -- (ignorar ';' dentro de comments)
    - Statements vacíos o solo-comentarios (filtrar)
    """
    result: list[str] = []
    current: list[str] = []
    depth = 0    # profundidad de BEGIN...END anidados
    i = 0
    
    while i < n:
        ch = sql[i]
        
        # Detectar inicio de line comment
        if ch == "-" and sql[i+1:i+2] == "-":
            j = i
            while j < n and sql[j] != "\n":
                j += 1
            current.append(sql[i:j])   # incluir el comentario
            i = j
            continue
        
        # ';' solo parte el statement si estamos fuera de BEGIN...END
        if ch == ";" and depth == 0:
            stmt = "".join(current).strip()
            if stmt:
                result.append(stmt)
            current = []
            i += 1
            continue
        
        # Detectar BEGIN y END respetando word boundaries
        if ch in ("B","b") and sql[i:i+5].upper() == "BEGIN":
            after = sql[i+5:i+6]; before = sql[i-1:i]
            if not after.isalnum() and not before.isalnum():
                depth += 1
        
        if ch in ("E","e") and sql[i:i+3].upper() == "END":
            after = sql[i+3:i+4]; before = sql[i-1:i]
            if not after.isalnum() and not before.isalnum():
                depth = max(depth - 1, 0)
        
        current.append(ch)
        i += 1
    
    return result
```

**¿Por qué hacer el parser a mano y no con regex?** Las expresiones regulares son stateless — no pueden trackear profundidad de anidamiento. `BEGIN...END` puede estar anidado en triggers o stored procedures. Un parser character-by-character con un contador de profundidad es más robusto.

### 7.2 Conexión Windows Auth vs SQL Auth

```python
def build_connection_string() -> str:
    user     = os.environ.get("SQLSERVER_USER", "").strip()
    password = os.environ.get("SQLSERVER_PASSWORD", "").strip()
    
    # Windows Auth cuando no hay usuario O el usuario tiene '\'
    # ('DOMINIO\\usuario' → Windows Auth automático)
    windows_auth = (not user) or ("\\" in user)
    
    if windows_auth:
        auth_part = "Trusted_Connection=yes;"      # Windows Auth (Named Pipe)
    else:
        auth_part = f"UID={user};PWD={password};"  # SQL Server Auth (TCP)
    
    # SERVER: incluir puerto solo si está definido (TCP)
    # Sin puerto → Named Pipe local (default SQL Server)
    port   = os.environ.get("SQLSERVER_PORT", "").strip()
    server = f"{host},{port}" if port else host
    
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={db};"
        f"{auth_part}"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )
```

**Named Pipe** (sin puerto): Para SQL Server local en Windows. Más rápido que TCP para localhost. No requiere que el servicio TCP esté habilitado.

**`TrustServerCertificate=yes`**: Necesario con ODBC Driver 18 que exige TLS. En producción usar certificados válidos.

---

## Bloque 8 — Preguntas de entrevista: Gold y Kimball (1 h)

### N1 — Conceptuales

**¿Qué es el modelo de Kimball?**
> Es una metodología para diseñar Data Warehouses centrada en las necesidades del negocio. Organiza los datos en tablas de hechos (qué ocurrió, métricas) y dimensiones (quién/qué/cuándo/dónde). Genera modelos "star" o "snowflake" optimizados para consultas analíticas. La clave diferenciadora de Kimball vs Inmon es que Kimball prioriza la usabilidad por el negocio (bottom-up por data mart) vs Inmon que prioriza un modelo corporativo normalizado centralizado (top-down EDW).

**¿Qué es el grain de una tabla de hechos y cómo lo defines?**
> El grain es la declaración de qué representa exactamente una fila. Para definirlo: (1) identifica el proceso de negocio (¿qué evento estamos midiendo?); (2) declara la granularidad (¿al nivel de qué detalle?); (3) identifica las dimensiones que aplican a ese nivel de detalle; (4) identifica qué medidas numéricas son aditivas. Para `fct_trip`: "1 fila = 1 viaje completo realizado por 1 tarjeta específica en 1 corte diario dado."

**¿Qué es SCD2 y cuándo lo usas?**
> SCD2 (Slowly Changing Dimension Type 2) mantiene el historial completo de cambios en una dimensión creando una nueva fila por cada cambio, con `valid_from`, `valid_to` e `is_current`. Lo usas cuando necesitas reportes históricos correctos: si un paradero cambió de zona tarifaria, el conteo de viajes de hace 2 años debe reflejar la zona que tenía entonces, no la zona actual. En este proyecto: `dim_stop` y `dim_service`.

### N2 — Técnicas

**¿Qué diferencia hay entre una dimensión conformada y una degenerada?**
> Una dimensión **conformada** es una tabla de dimensión completa compartida entre múltiples fact tables (ej: `dim_date` es la misma para `fct_trip` y `fct_boarding_30m`). Una dimensión **degenerada** es un atributo descriptivo de alta cardinalidad que se almacena directamente en la fact table en lugar de en una dim separada (ej: `id_viaje` en `fct_trip` — tiene millones de valores únicos, una dim separada sería enorme).

**¿Por qué usar MERGE en lugar de INSERT/UPDATE separados?**
> (1) Atomicidad: MERGE es una instrucción SQL atómica — se aplican todos los cambios o ninguno. Con INSERT+UPDATE separados hay una ventana de inconsistencia. (2) Idempotencia: `WHEN NOT MATCHED THEN INSERT` garantiza que re-ejecutar no duplica datos. (3) Rendimiento: el motor puede optimizar el plan de ejecución de MERGE mejor que dos DMLs separados.

**¿Qué es `fast_executemany` en pyodbc y por qué importa?**
> `cursor.fast_executemany = True` activa un modo de batch insert en pyodbc que agrupa múltiples INSERTs en un solo TVP (Table-Valued Parameter). En lugar de n round-trips al servidor (uno por fila), hace un solo envío con todas las filas. Para inserciones masivas: x10-x100 de mejora en throughput. Disponible en pyodbc ≥4.0.19 con ODBC Driver 17+.

### N3 — Avanzadas / situacionales

**¿Cómo detectarías el grain bug que ocurrió en este proyecto?**
> Después de la primera carga, ejecutar:
> ```sql
> SELECT COUNT(*) FROM dw.fct_trip WHERE cut_sk = ?;
> -- Si el resultado es << número esperado de viajes, algo está mal
> 
> SELECT id_viaje, COUNT(*) FROM staging.stg_viajes_trip GROUP BY id_viaje ORDER BY id_viaje;
> -- Revela el patrón: id_viaje 1..27 repetido millones de veces
> ```
> La clave: siempre validar el count post-load contra el count en staging. Un `27 rows inserted` para 520k staging rows es la señal definitiva.

**¿Cómo escalarías este pipeline de SQL Server a la nube?**
> Opciones: (1) **Azure Synapse Analytics** — replace SQL Server con Synapse Dedicated Pool. El patrón MERGE/COPY sigue siendo compatible. Las staging tables se pueden cargar con COPY INTO desde ADLS (mucho más rápido que bulk_insert). Columnstore indexes son default en Synapse. (2) **Databricks/Delta Lake** — replace SQL Server con Delta tables en ADLS. El "MERGE" equivalente es `DeltaTable.merge()`. SCD2 con Delta es más simple gracias a `MERGE` extendido. (3) **BigQuery** — modelo similar pero con MERGE, no tiene índices tradicionales.

**¿Bajo qué circunstancias un MERGE puede ser más lento que INSERT+DELETE?**
> Con tablas muy grandes y una gran proporción de MATCHED rows (updates). El MERGE necesita hacer el lookup de matching para cada fila source. Si el 99% de las filas ya existen y `WHEN MATCHED THEN UPDATE` no hace nada real, el costo del matching es alto sin beneficio. En ese caso, un `TRUNCATE + INSERT` puede ser más rápido (destruir y reconstruir es más barato que buscar cada fila). Pero TRUNCATE sacrifica idempotencia real.

---

## Bloque 9 — Integración: El flujo completo end-to-end (30 min)

```
1. DTPM publica CSV diario para 2025-04-21
   └─ viajes_20250421.csv (pipe-delimited, ~500k rows)

2. Bronze: extract_data.py
   └─ Descomprime ZIP → CSV en data/raw/

3. Bronze: build_lake.py
   └─ Mueve a lake/raw/dtpm/dataset=viajes/year=2025/month=04/cut=2025-04-21/
   └─ Genera _meta.json (row_count=520431, columns=[...])

4. Bronze: build_catalog.py
   └─ Actualiza lake/lake_catalog.json con la nueva partición

5. Silver: transform_silver.py --dataset viajes --cut 2025-04-21
   └─ Catalog().get_partitions() → PartitionInfo
   └─ DuckDB: read_csv all-VARCHAR → raw view
   └─ DuckDB: encriquecer (TRY_CAST, UPPER, mapeos)
   └─ DuckDB: quality CASE WHEN → _reason_code
   └─ COPY valid → viajes_trip.parquet + viajes_leg.parquet (ZSTD)
   └─ COPY invalid → _quarantine/invalid.parquet
   └─ quality.json: {read_row_count: 520431, invalid_rate: 0.11%}
   └─ Pydantic: 10k sample → 0 errors ✅

6. Gold: load_gold.py --cut 2025-04-21
   └─ Lee viajes_trip.parquet + viajes_leg.parquet con pandas
   └─ Upsert dim_cut (get or create cut_sk=42)
   └─ Upsert dim_date (fechas nuevas que aparecen en los datos)
   └─ SCD2 dim_stop (paraderos nuevos o con atributos cambiados)
   └─ SCD2 dim_service (servicios nuevos)
   └─ Truncate staging.stg_viajes_trip, staging.stg_viajes_leg
   └─ bulk_insert (fast_executemany) → staging tables
   └─ MERGE staging → dw.fct_trip         → 3,605,891 rows inserted
   └─ MERGE staging → dw.fct_trip_leg     → 14,423,564 rows inserted
   └─ etl_run_log: {status: 'OK', rows_fct_inserted: 3605891}
```

---

## Checklist del Día 3

- [ ] Explico star schema vs snowflake sin dudar
- [ ] Distingo dimensión vs hecho, conformada vs degenerada
- [ ] Explico qué es el grain y lo defino para fct_trip, fct_trip_leg y fct_validation
- [ ] Cuento la historia del grain bug (id_viaje = contador 1..27) como caso real
- [ ] Explico SCD2: valid_from, valid_to, is_current, row_hash
- [ ] Escribo un as-of join con SCD2 de memoria
- [ ] Explico por qué MERGE es idempotente
- [ ] Explico `fast_executemany` y su impacto
- [ ] Explico índices filtrados y cuándo usarlos
- [ ] Explico la diferencia entre surrogate key y business key
- [ ] Explico qué es `etl_run_log` y para qué sirve
- [ ] Respondo "¿cómo escalarías esto a la nube?" con múltiples opciones

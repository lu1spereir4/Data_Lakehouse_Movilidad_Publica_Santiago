# Guía de Defensa: Capa Silver — DTPM Movilidad Santiago

> **Propósito:** Entender y defender cada decisión de diseño del pipeline Silver antes de la etapa Gold. Una lectura de ~4-6 h.

---

## Índice

1. [Arquitectura general](#1-arquitectura-general)
2. [catalog.py — Resolución de rutas](#2-catalogpy--resolución-de-rutas)
3. [All-VARCHAR read — Por qué no leer con dtypes directamente](#3-all-varchar-read--por-qué-no-leer-con-dtypes-directamente)
4. [Escritura atómica (tmp → rename)](#4-escritura-atómica-tmp--rename)
5. [Quality view + Quarantine — El patrón de separación](#5-quality-view--quarantine--el-patrón-de-separación)
6. [Pydantic v2 — Contratos de datos por muestreo](#6-pydantic-v2--contratos-de-datos-por-muestreo)
7. [El bug de NaN → None y su solución](#7-el-bug-de-nan--none-y-su-solución)
8. [Surrogate keys temporales (date_sk / time_30m_sk)](#8-surrogate-keys-temporales-date_sk--time_30m_sk)
9. [UNPIVOT manual de legs (viajes)](#9-unpivot-manual-de-legs-viajes)
10. [Idempotencia y `--overwrite`](#10-idempotencia-y---overwrite)
11. [quality.json — Observabilidad del pipeline](#11-qualityjson--observabilidad-del-pipeline)
12. [Preguntas de entrevista y respuestas rápidas](#12-preguntas-de-entrevista-y-respuestas-rápidas)

---

## 1. Arquitectura general

```
RAW (CSV separado por |)
    ↓  catalog.py  (rutas, metadatos, columnas)
    ↓  transforms.py
        ├── read_csv ALL-VARCHAR  →  raw_xxxxx view
        ├── enriched_xxxxx view  (TRY_CAST, normalización)
        ├── xxxxx_quality view   (CASE → _reason_code)
        │       ├── valid rows   →  dataset.parquet   (silver)
        │       └── invalid rows →  _quarantine/invalid.parquet
        ├── _quarantine/valid.parquet   (audit copy)
        ├── _quality/quality.json       (métricas)
        └── Pydantic sample 10k filas   (contrato de schema)
```

### Separación de responsabilidades

| Archivo | Hace qué |
|---|---|
| `catalog.py` | Resuelve rutas, lee `lake_catalog.json` y `_meta.json`, expone `PartitionInfo` |
| `contracts.py` | Define los modelos Pydantic (schema esperado) y los umbrales de alerta |
| `transforms.py` | Toda la lógica DuckDB: views, escritura, quarantine, quality report |
| `transform_silver.py` | CLI: parsing de args, loop sobre particiones, manejo de errores |

**Por qué esta separación:** cada archivo tiene un único motivo para cambiar (Single Responsibility Principle). Si el schema cambia, solo tocas `contracts.py`. Si las reglas de negocio cambian, solo tocas `transforms.py`.

---

## 2. `catalog.py` — Resolución de rutas

### `PartitionInfo` es un dataclass `frozen=True`

```python
@dataclass(frozen=True)
class PartitionInfo:
    dataset: str
    cut: str
    ...
    raw_columns: tuple[str, ...] = field(default_factory=tuple)
```

**Por qué `frozen=True`:** las particiones son inmutables por naturaleza — son snapshots de datos históricos. La inmutabilidad evita bugs donde alguien modifica el objeto accidentalmente. También permite usarlo como clave de diccionario o en sets.

**Por qué `tuple` y no `list` para `raw_columns`:** por la misma razón — `tuple` es hashable y deja claro que la lista de columnas no se modifica en tiempo de ejecución.

### Columnas autoritativas desde `_meta.json`

```python
def columns_sql_spec(self) -> str:
    pairs = ", ".join(f"'{c}': 'VARCHAR'" for c in self.raw_columns)
    return "{" + pairs + "}"
```

**Decisión clave:** las columnas vienen de `_meta.json` (no hardcodeadas). Eso significa que si el proveedor de datos (DTPM) agrega o quita columnas, solo cambia el CSV y el meta; el código no necesita tocarse.

**Por qué filtrar columnas vacías (`_filter_columns`):** el CSV de `viajes` tiene una columna anónima al final (bug del proveedor: `col1|col2|`). Sin el filtro, DuckDB crea una columna `''` que rompe el read_csv posterior.

### `silver_output_dir()` / `quarantine_output_dir()` / `quality_output_dir()`

Cada partición sabe cuál es su destino. Esto centraliza la lógica de rutas: si la convención de carpetas cambia, solo cambia `PartitionInfo`, no `transforms.py`.

---

## 3. All-VARCHAR read — Por qué no leer con dtypes directamente

```python
def _build_varchar_read(csv_path: Path, col_spec: str) -> str:
    return (
        f"read_csv('{p}', "
        f"delim='|', header=True, encoding='utf-8', "
        f"nullstr=['-'], "          # ← guión = NULL en este dataset
        f"columns={col_spec})"      # ← todos VARCHAR
    )
```

### El problema que evitamos

DuckDB tiene una opción `ignore_errors=True` que silenciosamente descarta filas que no parsean. **No la usamos** porque:

1. Pierdes filas sin que nadie se entere.
2. La cuenta de filas no coincide con `_meta.json`, y el count assertion falla (que es justo lo que queremos: que falle ruidosamente).

### Por qué ALL-VARCHAR

Si dejas que DuckDB infiera tipos, una columna como `tiempo_subida` puede inferirse como `TEXT` en un corte y como `TIMESTAMP` en otro, dependiendo de los primeros valores del archivo. Con `VARCHAR` explícito:

- La lectura **nunca falla** (cada valor entra como string).
- La conversión de tipos se hace explícitamente con `TRY_CAST` en la vista enriquecida.
- Si `TRY_CAST(tiempo_subida AS TIMESTAMP) IS NULL`, sabes exactamente qué falló.

```sql
-- En enriched_viajes: conversión explícita con TRY_CAST
TRY_CAST(tiempo_inicio_viaje AS TIMESTAMP) AS tiempo_inicio_viaje,
```

`TRY_CAST` devuelve `NULL` si el cast falla (al contrario de `CAST` que lanza excepción). El `NULL` luego es catcheado por la quality view (MISSING_TIMESTAMP).

### nullstr=['-']

El CSV de DTPM usa `-` como valor nulo (no el estándar vacío `""`). Sin `nullstr=['-']`, DuckDB leería el guión como string literalmente, y los `TRY_CAST` a tipos numéricos fallarían o devolverían valores incorrectos.

---

## 4. Escritura atómica (tmp → rename)

```python
def _write_parquet_atomic(con, query, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f"._tmp_{uuid4().hex}_{dest.name}"
    tmp_str = str(tmp).replace("\\", "/")
    sql = f"COPY ({query}) TO '{tmp_str}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    try:
        con.execute(sql)
        shutil.move(str(tmp), str(dest))   # ← operación atómica
    except Exception:
        tmp.unlink(missing_ok=True)        # ← limpieza si falla
        raise
```

### Por qué es necesario

Imagina que el proceso se interrumpe (Ctrl+C, corte de luz, OOM) mientras DuckDB está escribiendo el Parquet. Sin escritura atómica, queda un archivo parcialmente escrito en `dest`. La próxima vez que corras el pipeline, ese archivo existe (se puede abrir, tiene bytes) pero está corrupto.

Con escritura atómica:
- DuckDB escribe a `._tmp_<uuid>_viajes_trip.parquet` (archivo temporal oculto)
- Solo cuando DuckDB termina **completamente**, `shutil.move()` renombra el archivo
- En filesystems locales (y la mayoría de redes), `rename` es una operación **atómica del SO** — no puede quedar a medias
- Si algo falla antes del rename, el `.unlink()` limpia el temporal

### Por qué `uuid4()` en el nombre del tmp

Si dos procesos corren en paralelo sobre la misma partición, sin UUID ambos escribirían al mismo `._tmp_viajes_trip.parquet` y se pisarían mutuamente. Con UUID cada proceso tiene su propio temporal.

### Por qué ZSTD

ZSTD es el compresor por defecto de DuckDB para Parquet. Comparado con Snappy (default original de Parquet):
- Mejor ratio de compresión (~30-50% menos espacio)
- Velocidad de descompresión comparable
- Mejor para almacenamiento a largo plazo (lake)

---

## 5. Quality view + Quarantine — El patrón de separación

Esta es la decisión de diseño más importante del pipeline. Entenderla bien es clave para cualquier entrevista de Data Engineering.

### El patrón en código

```sql
-- 1. Vista que etiqueta cada fila con su problema (o NULL si está bien)
CREATE OR REPLACE VIEW viajes_quality AS
SELECT *,
    CASE
        WHEN id_viaje IS NULL OR TRIM(id_viaje) = ''  THEN 'MISSING_ID'
        WHEN tiempo_inicio_viaje IS NULL               THEN 'MISSING_TIMESTAMP'
        WHEN n_etapas IS NOT NULL AND (n_etapas < 1 OR n_etapas > 4)
                                                       THEN 'BAD_RANGE_N_ETAPAS'
        WHEN tviaje_min IS NOT NULL AND tviaje_min < 0 THEN 'NEG_DISTANCE'
        -- ... más reglas
        ELSE NULL
    END AS _reason_code
FROM enriched_viajes

-- 2. Silver: solo filas limpias
SELECT * EXCLUDE (_reason_code)
FROM viajes_quality
WHERE _reason_code IS NULL

-- 3. Quarantine: filas con problema + la razón
SELECT *, _reason_code AS reason_code
FROM viajes_quality
WHERE _reason_code IS NOT NULL
```

### Por qué una sola vista con `_reason_code` y no dos queries separadas

**Eficiencia:** DuckDB evalúa `enriched_viajes` una sola vez. Si fueran dos queries independientes, el CSV se leería y transformaría dos veces. Con la vista, el plan de ejecución puede reusar el resultado.

**Consistencia:** la misma lógica determina si una fila es válida o inválida. No puede haber una fila que "desaparezca" porque fue rechazada por una query y no capturada por la otra.

**Auditabilidad:** el `_reason_code` permite saber *exactamente* por qué cada fila fue rechazada. En auditorías o debugging, querias como `GROUP BY reason_code ORDER BY COUNT(*) DESC` te dicen inmediatamente dónde está el problema sistemático.

### El `valid.parquet` en quarantine

```python
_write_parquet_atomic(con, trip_valid_query, quarantine_dir / "valid.parquet")
```

Esto puede sorprender: ¿por qué guardar una copia de las filas válidas en el directorio de quarantine?

**Razón:** para auditoría de conteo. El count assertion verifica:

```python
assert read_row_count == total_valid + total_invalid
```

Si el assertion falla, `valid.parquet` + `invalid.parquet` en quarantine te permiten reconciliar manualmente sin necesidad de reprocesar el CSV. Es una fotocopia del estado en el momento del procesamiento.

### Prioridad de las reglas CASE

El orden del CASE importa: la primera regla que hace match "gana". Las reglas van de lo más catastrófico a lo menos:

1. `MISSING_ID` — sin ID no hay fila
2. `MISSING_TIMESTAMP` — sin timestamp no es usable en análisis temporal
3. Rangos inválidos (`BAD_RANGE_*`, `BAD_TIME_SLOT`)
4. Valores negativos (`NEG_DISTANCE`)
5. Coordenadas fuera de la región de Santiago (`BAD_UTM_*`)

Si una fila tiene tanto `id_viaje = NULL` como coordenadas inválidas, el reason code es `MISSING_ID` — lo más grave primero. Esto simplifica el análisis y asegura que una sola fila no aparezca dos veces en el quarantine.

---

## 6. Pydantic v2 — Contratos de datos por muestreo

### Por qué Pydantic si ya hay quarantine en DuckDB

El quarantine en DuckDB captura reglas de negocio conocidas (nulls, rangos, coordenadas). Pydantic captura **violaciones del schema que no anticipaste** — es la segunda línea de defensa.

Ejemplos de lo que Pydantic puede detectar que DuckDB no detectaría sin una regla explícita:
- Un campo `Optional[float]` que el proveedor empieza a enviar como string `"N/D"` (TRY_CAST devuelve NULL, pero si el campo es Required en Pydantic, falla)
- Un campo `datetime` que cambia de formato (e.g., de `YYYY-MM-DD` a `DD/MM/YYYY`)
- Violaciones de invariantes cruzados entre campos (model_validator)

### Muestreo, no validación total

```python
SAMPLE_ROWS = 10_000  # filas para validación Pydantic

rows = con.execute(
    f"SELECT * FROM {view} USING SAMPLE {n} ROWS"
).fetchdf()
```

**Por qué no validar todas las filas con Pydantic:**
- Con 28M de filas en `etapas`, Pydantic (Python puro) tardaría horas
- DuckDB ya procesó y aprobó/rechazó todas las filas con las reglas SQL
- Una muestra aleatoria de 10k filas da ~99% de confianza para detectar problemas sistemáticos

Si el error rate en la muestra es 0%, la probabilidad de que el dataset completo tenga > 5% de errores no capturados es astronómicamente baja.

### Los umbrales: warn y fail

```python
PYDANTIC_WARN_RATE: float = 0.01   # >= 1%  → WARNING (pipeline continúa)
PYDANTIC_FAIL_RATE: float = 0.05   # >= 5%  → RuntimeError (pipeline aborta)
```

Estos umbrales son configurables por CLI:
```
--pydantic-warn-rate 0.02   (2%)
--pydantic-fail-rate 0.10   (10%)
```

**Por qué dos umbrales en vez de uno:**
- El warn te avisa que algo empieza a degradarse sin cortar el pipeline en producción
- El fail te detiene antes de escribir datos que no cumplen el contrato mínimo
- Estadísticamente, cierto nivel de ruido (< 1%) es normal en datos de transporte público. Un fail a 5% indica un problema sistemático.

### `mode="before"` en los validators

```python
@field_validator("tipo_dia", mode="before")
@classmethod
def validate_tipo_dia(cls, v: object) -> str:
    if v is None:
        raise ValueError("tipo_dia cannot be null")
    return str(v).strip().upper()
```

`mode="before"` significa que el validator corre **antes** de que Pydantic intente coercionar el tipo. Esto es crítico porque los datos vienen de un DataFrame de pandas, donde los valores pueden ser `numpy.int64`, `numpy.float64`, `None`, o strings. Sin `mode="before"`, Pydantic intentaría parsear `numpy.int64` como `Literal["LABORAL"]` y fallaría con un error confuso.

### `Optional[str] = None` vs `str` — La decisión del `id_tarjeta`

```python
# ViajesTripRow — ANTES (incorrecto):
id_tarjeta: str

# ViajesTripRow — DESPUÉS (correcto):
id_tarjeta: Optional[str] = None   # null en viajes en efectivo / tarjeta turística
```

**Por qué importa:** ~40% de los viajes son en efectivo. Los pasajeros que pagan en efectivo no tienen tarjeta Bip, por lo que `id_tarjeta` es genuinamente nulo. Modelarlo como `str` requerido haría que el 40% de los viajes fallara Pydantic — no un error de datos, sino un error de modelado.

La documentación del proveedor (DTPM) confirma que `id_tarjeta` es nulo para pagos en efectivo y tarjetas turísticas. El campo `Optional[str] = None` refleja correctamente la realidad del dominio.

---

## 7. El bug de NaN → None y su solución

Este es el bug más sutil del pipeline y el más importante de entender para entrevistas.

### El problema

```python
# DuckDB fetchdf() convierte NULL SQL → float('nan') en columnas float de pandas
rows = con.execute("SELECT * FROM trip_for_pydantic USING SAMPLE 1000 ROWS").fetchdf()

# Intento 1 (INCORRECTO):
rows = rows.where(rows.notna(), None)
# ↑ No funciona para columnas float: pandas las mantiene como dtype=float64
# y NaN es un valor válido de float64, así que nunca se reemplaza por None

# Pydantic recibe NaN, intenta validar NaN >= 0 (ge=0)...
# NaN >= 0 es False en IEEE 754 → ValidationError
```

### Por qué `rows.where(rows.notna(), None)` no funciona

En pandas, las columnas de tipo `float64` no pueden contener `None` (un objeto Python). Cuando llamas a `.where(cond, None)`, pandas convierte internamente `None` a `float('nan')` para mantener el dtype. Es una no-operación para columnas float.

```python
df = pd.DataFrame({'a': [float('nan')]})
df = df.where(df.notna(), None)
print(df['a'].iloc[0])   # → nan  (NO se convirtió a None)
print(type(df['a'].iloc[0]))  # → <class 'float'>
```

### La solución: convertir a dtype object primero

```python
# CORRECTO:
rows = rows.astype(object).where(pd.notna(rows), None)
```

Al convertir a `dtype=object`, las celdas pasan a ser referencias a objetos Python arbitrarios. Ahora `None` (el objeto Python nulo) sí puede vivir en esa columna. El `pd.notna(rows)` detecta los `NaN` en el DataFrame original y los reemplaza por `None` en la versión object.

```python
df = pd.DataFrame({'a': [float('nan')]})
fixed = df.astype(object).where(pd.notna(df), None)
print(fixed['a'].iloc[0])   # → None
print(type(fixed['a'].iloc[0]))  # → <class 'NoneType'>
```

### Por qué esto soluciona el problema con Pydantic

Cuando Pydantic recibe `None` en un campo `Optional[float] = Field(default=None, ge=0)`, entiende que el campo no tiene valor y **no evalúa la constrainta `ge=0`**. Si recibe `float('nan')`, intenta evaluar `NaN >= 0` → `False` → `ValidationError`.

### La diferencia entre validar en DuckDB y validar en Python

DuckDB maneja NULL según la lógica SQL trivalue (TRUE/FALSE/UNKNOWN). En SQL:
```sql
NULL >= 0  →  UNKNOWN  (no es TRUE ni FALSE)
```
Por eso DuckDB no quarantina filas con `distancia_eucl = NULL` aunque tenga `ge=0` en Pydantic — el CASE en SQL solo quarantina cuando el valor ES negativo, nunca cuando es NULL.

Pydantic en Python espera `None` para representar "ausencia de valor". Si recibe `NaN`, lo evalúa como un número inválido.

---

## 8. Surrogate keys temporales (`date_sk` / `time_30m_sk`)

### `date_sk` — Entero YYYYMMDD

```python
def _ts_to_date_sk(col: str) -> str:
    return f"CAST(strftime({col}, '%Y%m%d') AS INTEGER)"
```

Ejemplo: `2025-04-21 14:35:00` → `20250421`

**Por qué un entero y no una DATE:**
- Los enteros se comparan y ordenan más rápido que strings de fecha
- Los joins con una dimensión de calendario (`dim_date`) se hacen con `JOIN ON date_sk = dim_date.date_sk` (integer join, muy eficiente)
- Es el patrón estándar en Data Warehousing (Kimball)

### `time_30m_sk` — Entero 0..47

```python
def _ts_to_time_30m_sk(col: str) -> str:
    return (
        f"(DATEPART('hour', {col}) * 2 + "
        f"CASE WHEN DATEPART('minute', {col}) >= 30 THEN 1 ELSE 0 END)"
    )
```

Ejemplo: `14:35:00` → `hora=14, minuto=35 ≥ 30` → `14*2 + 1 = 29`

El día tiene 48 slots de 30 minutos (0 = 00:00-00:29, 47 = 23:30-23:59).

**Por qué 30 minutos:** los datos de subidas (`subidas_30m`) ya vienen agregados cada 30 minutos. El surrogate key permite hacer joins directos entre `viajes_trip` y `subidas_30m` usando `time_start_30m_sk JOIN time_30m_sk`.

### El caso especial de subidas_30m: fracción Excel

```python
def _excel_fraction_to_time_30m_sk(col: str) -> str:
    return f"CAST(FLOOR({col} * 48) AS INTEGER)"
```

El dataset `subidas_30m` guarda la hora como fracción del día estilo Excel (0.0 = medianoche, 0.5 = mediodía, 1.0 = fin del día). Esta es una decisión del proveedor (DTPM), no nuestra. Convertimos multiplicando por 48 (slots de 30min en un día) y tomando el floor.

---

## 9. UNPIVOT manual de legs (viajes)

El CSV de viajes tiene hasta 4 etapas por viaje en formato **wide** (una columna por etapa):

```
id_viaje | modo_1 | paradero_1 | modo_2 | paradero_2 | ...modo_4 | paradero_4
```

Para el modelo dimensional (Gold), necesitamos formato **long** (una fila por etapa).

```python
leg_unions = []
for i in range(1, 5):
    leg_unions.append(f"""
    SELECT
        id_viaje, id_tarjeta,
        {i} AS leg_seq,
        mode_code_{i} AS mode_code,
        ...
    FROM viajes_quality
    WHERE _reason_code IS NULL
      AND (
          mode_code_{i} IS NOT NULL
          OR service_code_{i} IS NOT NULL
          OR board_stop_{i} IS NOT NULL
          OR ts_board_{i} IS NOT NULL
      )
    """)

leg_query = " UNION ALL ".join(leg_unions)
```

### Por qué UNION ALL y no UNPIVOT nativo

DuckDB tiene `UNPIVOT` nativo, pero la lógica de legs es compleja (columnas heterogéneas, filtro de legs vacíos, mapeos distintos por leg). El UNION ALL manual da control total sobre qué se incluye en cada leg y permite el filtro `AND (mode_code IS NOT NULL OR ...)` que descarta legs vacíos.

### Por qué filtrar legs vacíos en el WHERE

Un viaje de 1 etapa tiene `modo_2`, `paradero_2` etc. todos null. Sin el filtro, el UNION ALL generaría una fila leg_2 completamente vacía para cada viaje. El modelo dimensional de Gold necesita solo los legs que realmente ocurrieron.

La condición `mode_code_{i} IS NOT NULL OR service_code_{i} IS NOT NULL OR board_stop_{i} IS NOT NULL OR ts_board_{i} IS NOT NULL` garantiza que al menos uno de los campos descriptivos del leg tiene dato útil.

---

## 10. Idempotencia y `--overwrite`

### ¿Qué es idempotencia en pipelines?

Un pipeline es **idempotente** si correrlo múltiples veces sobre el mismo input produce el mismo output que correrlo una sola vez. Sin idempotencia, si un pipeline falla y lo reinicias, puedes terminar con datos duplicados o mezclados.

### Sin `--overwrite` (comportamiento por defecto)

```python
# En transform_viajes, sin overwrite:
# Si silver_output_dir() existe, el COPY TO falla porque ya existe el archivo
# → RuntimeError, partition falla
# → Los datos existentes quedan intactos
```

Este es el comportamiento correcto para producción: si la partición ya fue procesada, no la reescribas a menos que lo pidas explícitamente.

### Con `--overwrite`

```python
def _clear_partition_dirs(partition: PartitionInfo) -> None:
    for d in [
        partition.silver_output_dir(),
        partition.quality_output_dir(),
        partition.quarantine_output_dir(),
    ]:
        if d.exists():
            shutil.rmtree(d)
```

`shutil.rmtree()` elimina el directorio completo antes de empezar. Así, si el pipeline anterior dejó archivos parciales o un `quality.json` incorrecto, todo se limpia y se reprocesa desde cero.

**Importante:** el borrado ocurre ANTES de cualquier escritura. Si el pipeline subsecuente falla, la partición queda vacía (mejor que con datos corruptos). El siguiente `--overwrite` la reprocesará.

### Count assertion como guardián de idempotencia

```python
assert read_row_count == total_valid + total_invalid, (
    f"viajes cut={partition.cut}: read_row_count={read_row_count} "
    f"!= valid({total_valid}) + invalid({total_invalid})"
)
```

Esta assertión garantiza que cada fila del CSV terminó exactamente en uno de los dos destinos (silver o quarantine). Si alguna fila se "pierde" por un bug en las views, el count no cierra y el pipeline falla ruidosamente.

---

## 11. `quality.json` — Observabilidad del pipeline

```json
{
  "generated_at": "2026-03-01T19:08:00.598Z",
  "duckdb_version": "0.10.x",
  "git_hash": "a1b2c3d",
  "dataset": "viajes",
  "cut": "2025-04-21",
  "meta_row_count": 3621017,
  "read_row_count": 3621017,
  "valid_row_count": 3580123,
  "invalid_row_count": 40894,
  "count_assertion": "PASS",
  "quarantine_rate_pct": 1.129,
  "reason_distribution": [
    {"_reason_code": "MISSING_TIMESTAMP", "cnt": 38000},
    {"_reason_code": "NEG_DISTANCE", "cnt": 2894}
  ],
  "pydantic_stats": {
    "sample_size": 10000,
    "error_count": 0,
    "error_rate_pct": 0.0
  }
}
```

### Por qué `duckdb_version` y `git_hash`

Reproducibilidad: si años después alguien quiere saber cómo se generó este archivo, el `git_hash` apunta al commit exacto del código. El `duckdb_version` permite reproducir el entorno exacto (DuckDB tuvo cambios de comportamiento entre versiones menores).

### Por qué `meta_row_count` vs `read_row_count`

`meta_row_count` viene de `_meta.json` (lo que el proveedor dice que envió). `read_row_count` es lo que DuckDB realmente leyó del CSV. Si difieren, hay un problema en la transmisión del archivo (truncado, líneas extras). El log registra un WARNING cuando no coinciden.

---

## 12. Preguntas de entrevista y respuestas rápidas

### "¿Por qué DuckDB y no Spark?"

Para un dataset de ~50M filas procesadas en un solo proceso con 6GB de RAM, DuckDB es más rápido que Spark local (sin overhead de scheduling, sin serialización de datos entre workers). Spark escala horizontalmente; DuckDB escala verticalmente y lo hace muy bien hasta varias decenas de GB. El dataset de DTPM entra perfectamente en esta categoría.

### "¿Qué pasa si el pipeline falla a mitad de una partición?"

La escritura atómica garantiza que nunca haya un Parquet parcialmente escrito. Si el proceso se mata, el directorio de destino queda vacío (fue limpiado por `--overwrite` al inicio). El siguiente run con `--overwrite` lo reprocesa desde cero.

### "¿Por qué no usar Pandas para todo?"

Pandas carga todo en memoria y procesa fila a fila en Python. DuckDB procesa columnarmente en C++ y puede hacer streaming. Para 28M filas de etapas, cargar el CSV en Pandas requeriría varios GB de RAM y tardaría minutos solo la lectura. DuckDB lee, transforma, filtra y escribe Parquet en una sola pasada.

### "¿Cómo garantizas que los datos en Silver son correctos?"

Tres capas:
1. **DuckDB quality rules:** 100% de las filas son revisadas con CASE rules específicas por dataset. Las inválidas van a quarantine con reason_code.
2. **Count assertion:** `read == valid + invalid` garantiza que ninguna fila se pierde.
3. **Pydantic sample:** 10k filas aleatorias son validadas contra el schema Python. Detecta problemas estructurales no anticipados.

### "¿Qué es un surrogate key y por qué usarlo?"

Un surrogate key (SK) es un identificador artificial que reemplaza al key de negocio para joins en el warehouse. `date_sk = 20250421` (int) es más eficiente para joins que `fecha = '2025-04-21'` (string) porque los enteros se comparan más rápido y ocupan menos espacio. Es el patrón estándar de Kimball para dimensiones de tiempo.

### "¿Por qué `USING SAMPLE n ROWS` y no `LIMIT n`?"

`LIMIT n` toma siempre las primeras `n` filas del resultado, que pueden ser atípicas (p.ej., todos del mismo servicio si el CSV está ordenado). `USING SAMPLE n ROWS` toma una muestra aleatoria uniforme del dataset completo, lo que da una validación estadísticamente representativa.

### "¿Cuál es el riesgo principal del patrón de quarantine?"

Si las reglas de quarantine son demasiado laxas, dejas entrar datos sucios al Silver. Si son demasiado estrictas, quarantinas datos válidos. El `quality.json` con `reason_distribution` te permite monitorear esto en el tiempo: si `MISSING_TIMESTAMP` sube de 0.1% a 5% en un corte nuevo, es una señal de alerta temprana de un problema upstream (el proveedor cambió el formato).

### "¿Por qué `Optional[str] = None` para `id_tarjeta`?"

Porque es un dominio de negocio real: los pasajeros que pagan en efectivo no tienen tarjeta Bip. Forzarlo a `str` requerido sería un error de modelado — estarías quarantinando datos válidos. El contrato de datos debe reflejar la realidad del dominio, no el ideal técnico.

### "¿Cómo manejas el NaN de pandas en Pydantic?"

Cuando DuckDB devuelve un DataFrame con `fetchdf()`, los NULL SQL en columnas float se convierten a `float('nan')`. Pydantic evalúa `NaN >= 0` como `False` (IEEE 754) y lanza `ValidationError`. La solución es convertir el DataFrame a `dtype=object` antes de pasarlo a Pydantic: `df.astype(object).where(pd.notna(df), None)`. Esto convierte cada NaN a `None` (el objeto Python nulo), que Pydantic maneja correctamente como valor ausente.

---

## Mapa mental para el día de estudio

```
Hora 1-2: Architecture overview
  └─ Lee transforms.py de arriba a abajo
  └─ Dibuja el flujo: CSV → raw view → enriched → quality → parquet/quarantine/quality.json

Hora 3: Deep dive en los patrones clave
  └─ _write_parquet_atomic: ¿qué pasa si falla en cada línea?
  └─ viajes_quality CASE: lee cada regla, entiende el orden
  └─ count assertion: ¿qué violación detectaría?

Hora 4: Pydantic + nulls
  └─ Abre contracts.py, lee cada modelo campo por campo
  └─ Entiende mode="before" vs mode="after"
  └─ Reproduce el bug NaN en un notebook: crea un DataFrame float con NaN,
     verifica que where() no lo convierte, verifica que astype(object) sí

Hora 5: Catalog + CLI
  └─ Lee catalog.py: PartitionInfo properties, silver_output_dir(), columns_sql_spec()
  └─ Lee transform_silver.py: TRANSFORM_REGISTRY, _resolve_partitions, el loop

Hora 6: Preguntas de entrevista en voz alta
  └─ Cuéntale a alguien (o a ti mismo en voz alta) cada decisión
  └─ Enfócate en: ¿por qué este diseño y no la alternativa obvia?
```

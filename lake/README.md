# Data Lake — Movilidad Pública Santiago (DTPM)

Catálogo técnico de la capa **raw** del data lake de transporte público de Santiago,
construido a partir de los datos abiertos del DTPM (Directorio de Transporte Público Metropolitano).

---

## Estructura de capas

```
lake/
├── raw/          ← Datos fuente sin transformar (esta capa)
├── processed/    ← Datos limpios, tipados y normalizados (próximamente)
└── curated/      ← Modelo estrella / DW listo para analítica (próximamente)
```

---

## Convención de particionado

Todas las rutas siguen el estándar Hive-partitioning para compatibilidad con
herramientas analíticas (Spark, DuckDB, Trino, SQL Server BULK INSERT, etc.):

```
raw/dtpm/
  dataset=<nombre>/
    year=<YYYY>/
      month=<MM>/
        cut=<periodo>/
          <nombre>.csv
          _meta.json
```

| Segmento | Descripción |
|----------|-------------|
| `dataset=` | Nombre lógico del conjunto de datos |
| `year=` / `month=` | Año y mes del corte |
| `cut=` | Granularidad del corte: fecha diaria `YYYY-MM-DD`, rango `YYYY-MM-DD_YYYY-MM-DD` o mensual `YYYY-MM` |
| `_meta.json` | Metadatos de la partición (ver §Metadatos) |

---

## Resumen del lake (raw)

| Métrica | Valor |
|---------|-------|
| Datasets | 3 |
| Particiones totales | 9 |
| Filas totales | 50 508 171 |
| Tamaño total | ~15.9 GB |
| Fuente | DTPM — datos abiertos abril 2025 |
| Separador CSV | `\|` (pipe) |
| Encoding | UTF-8 |

Catálogo completo en machine-readable: [`lake_catalog.json`](lake_catalog.json)

---

## Datasets

### 1. `dataset=viajes`

Registros de viajes completos por tarjeta Bip!, incluyendo origen-destino,
modos usados, tiempos y distancias. **Un cut por día.**

| Atributo | Valor |
|----------|-------|
| Granularidad | Diaria |
| Cortes disponibles | 2025-04-21 → 2025-04-27 (7 días) |
| Filas totales | ~21 313 043 |
| Tamaño total | ~8.2 GB |
| Columnas | 101 |
| Fuente original | `Tabla-de-viajes-011025.zip` |

<details>
<summary>Ver columnas (101)</summary>

| # | Columna | Descripción |
|---|---------|-------------|
| 1 | `tipodia` | Tipo de día (LABORAL / SABADO / DOMINGO) |
| 2 | `factor_expansion` | Factor de expansión estadístico del viaje |
| 3 | `n_etapas` | Número de etapas del viaje |
| 4 | `tviaje` | Tiempo total del viaje (minutos) |
| 5 | `distancia_eucl` | Distancia euclidiana origen-destino (metros) |
| 6 | `distancia_ruta` | Distancia ruta recorrida (metros) |
| 7 | `tiempo_inicio_viaje` | Timestamp inicio del viaje |
| 8 | `tiempo_fin_viaje` | Timestamp fin del viaje |
| 9 | `mediahora_inicio_viaje` | Media hora de inicio (0–47) |
| 10 | `mediahora_fin_viaje` | Media hora de término (0–47) |
| 11 | `periodo_inicio_viaje` | Período tarifario de inicio |
| 12 | `periodo_fin_viaje` | Período tarifario de término |
| 13-16 | `tipo_transporte_1..4` | Tipo de transporte por etapa |
| 17-20 | `srv_1..4` | Servicio (línea de bus / metro) por etapa |
| 21 | `paradero_inicio_viaje` | Código paradero de origen |
| 22 | `paradero_fin_viaje` | Código paradero de destino |
| 23 | `comuna_inicio_viaje` | Comuna de origen |
| 24 | `comuna_fin_viaje` | Comuna de destino |
| 25 | `zona_inicio_viaje` | Zona tarifaria de origen |
| 26 | `zona_fin_viaje` | Zona tarifaria de destino |
| 27 | `modos` | Modos de transporte del viaje |
| 28-31 | `tiempo_subida_1..4` | Timestamp de subida por etapa |
| 32-35 | `tiempo_bajada_1..4` | Timestamp de bajada por etapa |
| 36-39 | `zona_subida_1..4` | Zona de subida por etapa |
| 40-43 | `zona_bajada_1..4` | Zona de bajada por etapa |
| 44-47 | `paradero_subida_1..4` | Paradero de subida por etapa |
| 48-51 | `paradero_bajada_1..4` | Paradero de bajada por etapa |
| 52-55 | `mediahora_bajada_1..4` | Media hora de bajada por etapa |
| 56-59 | `periodo_bajada_1..4` | Período tarifario de bajada por etapa |
| 60 | `id_tarjeta` | Identificador anónimo de tarjeta Bip! |
| 61 | `id_viaje` | Identificador del viaje |
| 62 | `netapassinbajada` | Número de etapas sin bajada detectada |
| 63 | `ultimaetapaconbajada` | Índice de la última etapa con bajada |
| 64 | `contrato` | Código de contrato del operador |
| 65 | `mediahora_inicio_viaje_hora` | Hora legible del inicio del viaje |
| 66 | `mediahora_fin_viaje_hora` | Hora legible del término del viaje |
| 67-70 | `op_1era_etapa..op_4ta_etapa` | Operador por etapa |
| 71-85 | `dt1..dveh_eucfinal` | Métricas de distancia y tiempo entre etapas |
| 86 | `tipo_corte_etapa_viaje` | Tipo de corte de etapa-viaje |
| 87 | `proposito` | Propósito del viaje (TRABAJO / HOGAR / EDUCACION…) |
| 88 | `entrada` | Entrada al sistema |
| 89-101 | `te0`, `tv1..tv4`, `tc1..tc3`, `te1..te3`, `egreso`, `tviaje2` | Tiempos desagregados por etapa |

</details>

---

### 2. `dataset=etapas`

Registros a nivel de etapa individual (1 fila = 1 validación Bip!).
**Todos los días del período concatenados en un solo CSV.**

| Atributo | Valor |
|----------|-------|
| Granularidad | Rango semanal |
| Corte | 2025-04-21 → 2025-04-27 |
| Filas totales | 28 447 535 |
| Tamaño total | ~7.68 GB |
| Columnas | 35 |
| Fuente original | `Tabla-de-etapas-011025.zip` (7 archivos `.csv.gz`) |

<details>
<summary>Ver columnas (35)</summary>

| # | Columna | Descripción |
|---|---------|-------------|
| 1 | `operador` | Código del operador (contrato) |
| 2 | `id_etapa` | Identificador único de la etapa |
| 3 | `correlativo_viajes` | Número de viaje del pasajero en el día |
| 4 | `correlativo_etapas` | Número de etapa dentro del viaje |
| 5 | `tipo_dia` | Tipo de día (LABORAL / SABADO / DOMINGO) |
| 6 | `tipo_transporte` | Modo de transporte (BUS / METRO / METROTREN) |
| 7 | `fExpansionServicioPeriodoTS` | Factor de expansión del servicio en el período |
| 8 | `tiene_bajada` | Indicador si se detectó bajada (1/0) |
| 9 | `tiempo2` | Timestamp de la validación (subida) |
| 10 | `tiempo_subida` | Timestamp de subida al vehículo |
| 11 | `tiempo_bajada` | Timestamp de bajada del vehículo |
| 12 | `tiempo_etapa` | Duración de la etapa (segundos) |
| 13 | `media_hora_subida` | Media hora de subida (0–47) |
| 14 | `media_hora_bajada` | Media hora de bajada (0–47) |
| 15 | `x_subida` | Coordenada X subida (UTM 19S) |
| 16 | `y_subida` | Coordenada Y subida (UTM 19S) |
| 17 | `x_bajada` | Coordenada X bajada (UTM 19S) |
| 18 | `y_bajada` | Coordenada Y bajada (UTM 19S) |
| 19 | `dist_ruta_paraderos` | Distancia por ruta entre paraderos (metros) |
| 20 | `dist_eucl_paraderos` | Distancia euclidiana entre paraderos (metros) |
| 21 | `servicio_subida` | Servicio al subir |
| 22 | `servicio_bajada` | Servicio al bajar |
| 23 | `parada_subida` | Código de parada de subida |
| 24 | `parada_bajada` | Código de parada de bajada |
| 25 | `comuna_subida` | Comuna de subida |
| 26 | `comuna_bajada` | Comuna de bajada |
| 27 | `zona_subida` | Zona tarifaria de subida |
| 28 | `zona_bajada` | Zona tarifaria de bajada |
| 29 | `sitio_subida` | Código de sitio de subida |
| 30 | `fExpansionZonaPeriodoTS` | Factor de expansión por zona y período |
| 31 | `tEsperaMediaIntervalo` | Tiempo de espera estimado (min) |
| 32 | `periodoSubida` | Período tarifario de subida (nombre) |
| 33 | `periodoBajada` | Período tarifario de bajada (nombre) |
| 34 | `tiempoIniExpedicion` | Timestamp de inicio de expedición del vehículo |
| 35 | `contrato` | Código de contrato del operador |

</details>

---

### 3. `dataset=subidas_30m`

Promedio de subidas (validaciones Bip!) por paradero/estación en ventanas
de 30 minutos, separado por tipo de día.

| Atributo | Valor |
|----------|-------|
| Granularidad | Mensual (promedio laboral + sáb + dom por separado) |
| Corte | 2025-04 |
| Filas totales | 747 593 |
| Tamaño total | ~40 MB |
| Columnas | 6 |
| Fuente original | `Subida_Paradero_Estacion_2025.04.xlsb` (hoja `SUBIDAS_2025_04`) |

| # | Columna | Descripción |
|---|---------|-------------|
| 1 | `Tipo_dia` | Tipo de día (LABORAL / SABADO / DOMINGO) |
| 2 | `Modo` | Modo de transporte (BUS / METRO / METROTREN) |
| 3 | `Paradero` | Código de paradero o estación |
| 4 | `Comuna` | Comuna donde se ubica el paradero |
| 5 | `Media_hora` | Ventana de 30 minutos (ej: `06:30:00`) |
| 6 | `Subidas_Promedio` | Promedio de subidas en esa ventana y tipo de día |

> **Nota de la fuente:** Los datos no incluyen evasión. El promedio laboral
> corresponde a los días 21–25 de abril 2025. Sábado = 26/04, Domingo = 27/04.

---

## Metadatos por partición (`_meta.json`)

Cada partición contiene un `_meta.json` con los siguientes campos:

```jsonc
{
  "dataset"        : "viajes",           // nombre del dataset
  "source"         : "DTPM - ...",       // descripción de la fuente
  "cut"            : "2025-04-21",       // identificador del corte
  "year"           : 2025,
  "month"          : 4,
  "separator"      : "|",               // separador del CSV
  "encoding"       : "utf-8",
  "columns"        : ["col1", "col2"],  // lista de columnas
  "column_count"   : 101,
  "row_count"      : 3621017,           // filas sin encabezado
  "file_size_bytes": 1499362295,
  "source_file"    : "2025-04-21.viajes.csv",
  "extracted_at"   : "2026-02-27T14:06:50+00:00"
  // etapas añade además: source_files[], date_range{}
  // subidas_30m añade además: source_sheet, ficha{}
}
```

---

## Scripts del pipeline

| Script | Propósito |
|--------|-----------|
| [`extract_data.py`](../extract_data.py) | Extrae `.zip` y descomprime `.csv.gz` → `data/extracted/` |
| [`build_lake.py`](../build_lake.py) | Organiza los CSV en la estructura del lake + genera `_meta.json` |
| [`build_catalog.py`](../build_catalog.py) | Consolida todos los `_meta.json` → `lake_catalog.json` |

### Ejecutar el pipeline completo

```powershell
# 1. Extraer fuentes
python extract_data.py

# 2. Construir lake/raw/
python build_lake.py

# 3. Regenerar catálogo
python build_catalog.py
```

---

## Carga en SQL Server

Los CSV usan `|` como separador y codificación UTF-8. Ejemplo de `BULK INSERT`:

```sql
BULK INSERT staging.viajes
FROM 'C:\lake\raw\dtpm\dataset=viajes\year=2025\month=04\cut=2025-04-21\viajes.csv'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '\n',
    FIRSTROW        = 2,          -- omite el encabezado
    CODEPAGE        = '65001',    -- UTF-8
    TABLOCK
);
```

---

## Próximas capas

| Capa | Estado | Descripción |
|------|--------|-------------|
| `raw/` | ✅ Completa | Datos fuente sin transformar |
| `processed/` | 🔜 Próximo | Limpieza, tipado, normalización de códigos |
| `curated/` | 🔜 Próximo | Modelo estrella para DW en SQL Server |

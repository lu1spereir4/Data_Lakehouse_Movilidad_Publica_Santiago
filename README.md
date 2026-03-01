# 🚇 Data Lakehouse — Movilidad Pública Santiago

![Python](https://img.shields.io/badge/Python-3.14-3776AB?style=flat&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFCC00?style=flat&logo=duckdb&logoColor=black)
![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-blueviolet?style=flat)
![Kimball](https://img.shields.io/badge/Modeling-Kimball_Star_Schema-0078D4?style=flat)
![Status](https://img.shields.io/badge/Status-En_Desarrollo-orange?style=flat)

> **Implementación de un Data Lakehouse de alto rendimiento** diseñado para procesar, transformar y analizar **50.5 millones de registros** del transporte público de Santiago. El proyecto muestra la transición desde datos crudos hacia un **Data Warehouse dimensional (Kimball)** optimizado para analítica, usando procesamiento eficiente en local (DuckDB/Parquet) y carga a SQL Server.

---

## 🧭 Modelo Dimensional (vista general)

> **Tip:** Si vienes directo por el DW/modelado, esta es la sección clave. 
> 📄 **[Lee la documentación detallada y justificación de decisiones arquitectónicas aquí ➡️](movilidad_publicaStgo/models/README.md)**

![Constellation Model](movilidad_publicaStgo/models/constelar_model.png)

### 🔎 Data Marts (diagramas por área temática)

Separé el DW en **Data Marts** para que el diseño sea legible (evitar un diagrama gigante con líneas cruzadas) y para que cada mart tenga un objetivo claro.

- **1) Trips & OD (Movilidad/Demanda de viajes)** Objetivo: OD, duración, transbordos, propósito, horas punta por viaje.  
  👉 Ver diagrama: [data_mart_trips_od.png](movilidad_publicaStgo/models/data_mart_trips_od.png)

- **2) Trip Legs (Etapas dentro del viaje)** Objetivo: transbordos, secuencias BUS→METRO, puntos de intercambio, carga por servicio por etapa.  
  👉 Ver diagrama: [data_mart_trip_legs.png](movilidad_publicaStgo/models/data_mart_trip_legs.png)

- **3) Stages & Operations (Operación por validación)** Objetivo: servicio, modo, espera, bajadas detectadas, performance por paradero/servicio.  
  👉 Ver diagrama: [data_mart_stages_operations.png](movilidad_publicaStgo/models/data_mart_stages_operations.png)

- **4) Network Demand (Demanda agregada por paradero/30m)** Objetivo: perfiles horarios de subidas promedio por paradero, modo y tipo de día.  
  👉 Ver diagrama: [data_mart_network_demand.png](movilidad_publicaStgo/models/data_mart_network_demand.png)

> Nota: Todos los marts comparten **dimensiones conformadas** (ej. `dim_date`, `dim_time_30m`, `dim_stop`), por lo que se pueden cruzar para análisis más completos.

---

## ⚡ Métricas del Proyecto

| Indicador | Valor |
|-----------|-------|
| 📦 Registros procesados | **50,508,171** filas |
| 💾 Volumen de datos raw | **~15.9 GB** |
| 📁 Datasets integrados | 3 (viajes, etapas, subidas_30m) |
| 🗓️ Período cubierto | Semana 21–27 abril 2025 |
| 🏙️ Fuente | DTPM — RED Movilidad Santiago |
| 🔻 Reducción de columnas (viajes) | **100 → 21 cols (ahorro 67%)** |

---

## 🏛️ Arquitectura: Medallion

Este proyecto implementa la **Medallion Architecture**, el estándar de la industria para Data Lakehouses (usado por Databricks, Azure y AWS), adaptado a un entorno local de alto rendimiento.

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   ZIP + CSV.GZ (viajes, etapas)  │  XLSB (subidas paradero)   │
└───────────────────┬─────────────────────────────┬───────────────┘
                    │                             │
                    ▼                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — lake/raw/                                          │
│  Datos fuente sin modificar, particionados con Hive-style       │
│  dataset=<X>/year=YYYY/month=MM/cut=<periodo>/                  │
│  ✓ Extracción automatizada  ✓ _meta.json por partición          │
│  ✓ lake_catalog.json centralizado                               │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — lake/processed/                           [WIP]    │
│  Limpieza · Tipado · Normalización · Selección de columnas      │
│  Motor: DuckDB / Polars · Salida: Parquet                       │
│  ✓ Análisis de calidad por columna (null_rate, dtype)           │
│  ✓ CSVs slim generados (columnas de negocio)                    │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD — lake/curated/ + SQL Server DW              [WIP]     │
│  Star Schema (Kimball)  ·  SQL Server                           │
│  fact_viajes · fact_etapas · dim_tiempo · dim_paradero          │
│  dim_servicio · dim_comuna · dim_periodo                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Estructura del Repositorio

```
📦 Data_Lakehouse_Movilidad_Pública_Santiago/
│
├── 📂 data/                          # Fuentes originales (ZIPs, XLSB)
│   └── extracted/                    # Temporal — eliminable post-pipeline
│
├── 📂 lake/
│   ├── lake_catalog.json             # Catálogo unificado de todos los metadatos
│   ├── README.md                     # Documentación técnica del lake
│   ├── raw/dtpm/                     # 🥉 Bronze
│   │   ├── dataset=viajes/           #   3.6M filas/día × 7 días
│   │   ├── dataset=etapas/           #   28.4M filas (semana completa)
│   │   └── dataset=subidas_30m/      #   747K filas (promedio mensual)
│   ├── processed/dtpm/               # 🥈 Silver — columnas slim + Parquet
│   └── curated/                      # 🥇 Gold — Star Schema [WIP]
│
├── 📂 models/                        # DDL del Star Schema en SQL Server
│
├── extract_data.py                   # Descomprime ZIP/GZ anidados
├── build_lake.py                     # Construye estructura Hive + _meta.json
├── build_catalog.py                  # Genera lake_catalog.json consolidado
└── analyze_columns.py                # Análisis de calidad + CSV slim
```

---

## 🔬 Los Datos

Los datos provienen del **DTPM (Directorio de Transporte Público Metropolitano)**, publicados como datos abiertos. Combinan dos fuentes críticas:

- 📡 **GPS de la flota** — posición y tiempo de cada bus en operación
- 💳 **Transacciones Bip!** — cada validación de tarjeta en subida y bajada inferida

### Datasets

| Dataset | Granularidad | Filas | Columnas raw | Descripción |
|---------|-------------|-------|--------------|-------------|
| `viajes` | 1 fila = 1 viaje completo | ~21.3M | 101 | Origen-destino, modos, tiempos, propósito del viaje |
| `etapas` | 1 fila = 1 validación Bip! | 28.4M | 35 | Detalle de cada tramo con coordenadas GPS |
| `subidas_30m` | Promedio por paradero/30min | 747K | 6 | Demanda agregada por parada, modo y tipo de día |

---

## 🧹 Capa Silver — Decisiones de Ingeniería

### ¿Por qué ETL y no ELT?

Las empresas modernas migran hacia **ELT** porque es más flexible: cargas los datos crudos al motor SQL (Snowflake, BigQuery, Redshift) y transformas ahí con SQL puro. Si una columna cambia o un analista necesita un campo nuevo, es una línea de SQL, no re-ejecutar un pipeline Python con 50M de filas.

**Sin embargo, para este proyecto se optó por ETL** por razones deliberadas:

1. **Consolidación de fundamentos** — El objetivo es dominar Python + SQL antes de abstraerlos con herramientas cloud.
2. **Restricciones de almacenamiento local** — 15.9 GB raw requieren una estrategia cuidadosa sin cloud storage (S3/ADLS).
3. **Escalabilidad futura planeada** — El diseño Hive-partitioned es compatible con migración directa a Spark/Databricks.

### Transformaciones planeadas (Silver)

```
RAW (todo texto, pipe-separated)
  │
  ├─ Tipado estricto
  │    • timestamps   → DATETIME
  │    • coordenadas  → FLOAT (UTM 19S)
  │    • contadores   → INTEGER
  │    • categorías   → VARCHAR normalizado
  │
  ├─ Normalización de strings
  │    • comunas: 'santiago' / 'Santiago' / 'SANTIAGO' → 'SANTIAGO'
  │    • modos: 'Bus' / 'BUS' → 'BUS'
  │
  ├─ Tratamiento de nulos (valor centinela "-")
  │    • Bajada no registrada → NULL (pérdida de señal GPS en túnel)
  │    • Sin bajada por evasión → flag separado
  │    • Viajes sin destino → conservados con flag, no eliminados
  │    └── Decisión: los outliers son DATOS, no errores — el DTPM los documenta
  │
  └─ Selección de columnas (analyze_columns.py)
       • viajes:      101 → 21 columnas  (ahorro 67% en tamaño)
       • etapas:       35 → 24 columnas  (ahorro 37%)
       • subidas_30m:   6 →  6 columnas  (todas relevantes)
```

### Motor de procesamiento

| Opción | RAM requerida | Velocidad | Decisión |
|--------|-------------|-----------|----------|
| **DuckDB** | ~8 GB | ⚡⚡⚡⚡ | ✅ Primera opción |
| **Polars** | ~4 GB (streaming) | ⚡⚡⚡ | ✅ Fallback si RAM insuficiente |
| PySpark | Cluster | ⚡⚡⚡⚡⚡ | 🔜 Futuro con S3 |

DuckDB puede leer Parquet/CSV directamente desde disco sin cargar todo en RAM, ejecutando SQL ANSI estándar. Los resultados se escriben en Parquet columnar para maximizar velocidad en la carga Gold.

---

## ⭐ Capa Gold — Star Schema (Kimball)

El modelo sigue la **metodología Kimball** con dimensiones conformadas entre `fact_viajes` y `fact_etapas`, permitiendo análisis cruzado a cualquier nivel de granularidad.

```
                    ┌────────────────┐
                    │   dim_tiempo   │
                    │  PK: time_id   │
                    │  fecha, hora   │
                    │  periodo, tipo │
                    └───────┬────────┘
                            │
┌──────────────┐    ┌───────▼────────┐    ┌────────────────┐
│  dim_servicio│    │  fact_viajes   │    │  dim_paradero  │
│  PK: srv_id  ├────│  grain: viaje  ├────│  PK: par_id    │
│  linea, modo │    │  n_etapas      │    │  codigo, nombre│
│  operador    │    │  duracion_min  │    │  comuna, zona  │
│  contrato    │    │  dist_ruta_m   │    │  coord UTM     │
└──────────────┘    │  factor_exp    │    └────────────────┘
                    │  proposito     │
┌──────────────┐    └───────┬────────┘    ┌────────────────┐
│  dim_comuna  │            │             │  dim_periodo   │
│  PK: com_id  │    ┌───────▼────────┐    │  PK: per_id    │
│  nombre      ├────│  fact_etapas   ├────│  nombre        │
│  region      │    │  grain: etapa  │    │  hora_inicio   │
└──────────────┘    │  tiempo_etapa  │    │  hora_fin      │
                    │  espera_min    │    └────────────────┘
                    │  tiene_bajada  │
                    │  dist_ruta_m   │
                    └────────────────┘
```

---

## 🛠️ Stack Tecnológico

| Categoría | Herramienta | Uso |
|-----------|------------|-----|
| Lenguaje | Python 3.14 | Scripting, ETL, análisis |
| Procesamiento | DuckDB / Polars | Transformación Silver en memoria eficiente |
| Data Warehouse | SQL Server | Capa Gold, Star Schema |
| Formato columnar | Parquet | Almacenamiento intermedio Silver |
| Particionado | Hive-style | Compatibilidad Spark/Trino futura |
| Versionado | Git + GitHub | Control de versiones |
| Catalogación | JSON (_meta.json) | Linaje de datos por partición |

---

## 🚀 Pipeline — Cómo ejecutar

### Requisitos

```bash
pip install pyxlsb pandas duckdb polars pyarrow
```

### Paso a paso

```powershell
# 1. Extraer fuentes (ZIP → CSV.GZ → CSV)
python extract_data.py

# 2. Construir lake/raw/ con particionado Hive
python build_lake.py

# 3. Generar catálogo unificado
python build_catalog.py

# 4. Analizar columnas y generar CSVs slim
python analyze_columns.py

# 5. [WIP] Transformación Silver → Parquet
# python transform_silver.py

# 6. [WIP] Carga Gold → SQL Server
# python load_gold.py
```

---

## 📊 Preguntas de Negocio a Responder

El Star Schema estará optimizado para responder:

- 🕐 **¿Cuáles son los períodos de mayor demanda por modo de transporte?**
- 🗺️ **¿Qué flujos origen-destino (comuna-comuna) concentran más viajes?**
- 🔄 **¿Cuántos transbordos promedio realiza un usuario según el período del día?**
- ⏱️ **¿Cuál es el tiempo promedio de espera en paradero por servicio y hora?**
- 📍 **¿Qué paraderos tienen mayor concentración de subidas en hora punta?**
- 🎯 **¿Cómo varía el propósito del viaje (trabajo/hogar/educación) por zona?**
- 📉 **¿Qué servicios presentan mayor tasa de bajada no registrada (pérdida GPS)?**

---

## 🗺️ Roadmap

- [x] **Extracción** — Descompresión de ZIPs anidados y archivos `.csv.gz`
- [x] **Capa Bronze** — Particionado Hive-style + `_meta.json` por partición
- [x] **Catálogo** — `lake_catalog.json` con linaje completo (50.5M filas, 15.9 GB)
- [x] **Análisis de columnas** — Null rate, dtype inferido, selección de negocio
- [x] **CSVs slim** — Reducción de 101→21 columnas en viajes (ahorro 67%)
- [ ] **Capa Silver** — Tipado, normalización, Parquet via DuckDB/Polars
- [ ] **Diseño DDL** — Star Schema en SQL Server
- [ ] **Carga Gold** — ETL desde Parquet → SQL Server
- [ ] **Validación** — Queries analíticas sobre el Star Schema
- [ ] **Escalabilidad** — Migración a PySpark + S3 para múltiples meses

---

## 📁 Documentación Adicional

- [`lake/README.md`](lake/README.md) — Catálogo técnico completo del lake (datasets, columnas, metadatos)
- [`lake/lake_catalog.json`](lake/lake_catalog.json) — Catálogo machine-readable

---

## 👤 Autor

Proyecto de portafolio desarrollado como parte de un roadmap de **Data Engineering**, con enfoque en arquitecturas de datos modernas, modelado dimensional y procesamiento de grandes volúmenes a nivel local antes de escalar a cloud.

> *"El transporte público genera datos masivos cada segundo — este proyecto convierte ese caos en inteligencia accionable."*


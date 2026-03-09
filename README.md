# Data Lakehouse — Movilidad Pública Santiago

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFCC00?style=flat&logo=duckdb&logoColor=black)
![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1?style=flat)
![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=flat)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blueviolet?style=flat)
![Modeling](https://img.shields.io/badge/Modeling-Kimball_Star_Schema-0078D4?style=flat)

Pipeline de datos end-to-end sobre los **50.5 millones de registros** mensuales de la Red Metropolitana de Santiago (DTPM). Cubre el ciclo completo Bronze → Silver → Gold: desde ZIPs sin procesar hasta un Data Warehouse dimensional en SQL Server, pasando por transformaciones con DuckDB y validación de calidad con Pydantic v2.

---

<div align="center">
  <img src="docs/arquitectura.png" alt="Arquitectura del Pipeline DTPM" width="900px">
  <p><i>Esquema técnico del pipeline: Desde la ingesta en Bronze hasta el modelo dimensional en Gold.</i></p>
</div>
El proyecto implementa **Medallion Architecture**: tres capas con responsabilidades bien separadas. Bronze guarda los datos exactamente como llegaron. Silver los transforma, tipifica y filtra con DuckDB. Gold carga el resultado en un Star Schema Kimball en SQL Server.

```
ZIP/CSV.GZ/XLSB (DTPM)
        │
        ▼  extract_data.py
┌────────────────────────────────────────────────────────────────┐
│  BRONZE — lake/raw/                                            │
│  Particionado Hive-style: dataset=X/year=YYYY/month=MM/cut=X  │
│  _meta.json por partición  ·  lake_catalog.json centralizado  │
└───────────────────────────┬────────────────────────────────────┘
                            │  transform_silver.py
                            ▼
┌────────────────────────────────────────────────────────────────┐
│  SILVER — lake/processed/                                      │
│  DuckDB · all-VARCHAR read · TRY_CAST · Parquet ZSTD           │
│  Contratos Pydantic v2  ·  quarantine de filas inválidas       │
│  quality.json por partición                                    │
└───────────────────────────┬────────────────────────────────────┘
                            │  load_gold.py
                            ▼
┌────────────────────────────────────────────────────────────────┐
│  GOLD — SQL Server (MovilidadDW)                               │
│  Star Schema Kimball  ·  9 dims + 4 facts                      │
│  SCD2 en dim_stop y dim_service  ·  MERGE idempotente          │
│  Filtered unique indexes por grain                             │
└────────────────────────────────────────────────────────────────┘
```

---

## Modelo Dimensional

> 📄 **[Documentación detallada y justificación de decisiones → `/docs/README.md`](/docs/README.md)**

![Constellation Model](/docs/constelar_model.png)

### Data Marts

El DW está dividido en cuatro Data Marts con dimensiones conformadas entre ellos (cualquier mart puede cruzarse con otro).

**1. Trips & OD — Movilidad y demanda de viajes**
Origen-destino, duración, transbordos, propósito, períodos tarifarios.
👉 [data_mart_trips_od.png](/docs/data_mart_trips_od.png)

**2. Trip Legs — Etapas dentro del viaje**
Secuencias BUS→METRO, puntos de intercambio, carga por servicio por tramo.
👉 [data_mart_trip_legs.png](/docs/data_mart_trip_legs.png)

**3. Stages & Operations — Operación por validación Bip!**
Servicio, modo de transporte, tiempo de espera, bajadas detectadas, performance por paradero.
👉 [data_mart_stages_operations.png](/docs/data_mart_stages_operations.png)

**4. Network Demand — Demanda agregada por paradero/30 min**
Perfiles horarios de subidas promedio por paradero, modo y tipo de día.
👉 [data_mart_network_demand.png](/docs/data_mart_network_demand.png)


## Resultados

| Métrica | Valor |
|---------|-------|
| Registros procesados | **50,508,171** filas |
| Volumen raw | **~15.9 GB** |
| Datasets integrados | 3 (viajes, etapas, subidas_30m) |
| `fct_trip` cargadas | **3,605,891** filas |
| `fct_trip_leg` cargadas | **14,423,564** filas |
| Tasa de datos inválidos | < 0.2% |
| Reducción de columnas (viajes) | 101 → 21 cols — ahorro 67% |
| Smoke tests pasando | 22/22 ✅ |
| Periodo cubierto | Semana 21–27 abril 2025 |

---

## Los Datos

Fuente: **DTPM (Directorio de Transporte Público Metropolitano)**, publicados como datos abiertos. Combinan dos fuentes: 

- **GPS de la flota** — posición y timestamp de cada bus en operación
- **Transacciones Bip!** — cada validación de tarjeta, con bajada inferida por modelo

| Dataset | Granularidad | Filas | Cols raw | Descripción |
|---------|-------------|-------|----------|-------------|
| `viajes` | 1 fila = 1 viaje completo | ~21.3M | 101 | OD, modos, tiempos, propósito |
| `etapas` | 1 fila = 1 validación Bip! | 28.4M | 35 | Detalle por tramo con coords GPS |
| `subidas_30m` | Promedio paradero/30min | 747K | 6 | Demanda agregada por parada y modo |

---

## Decisiones de diseño

### ¿Por qué DuckDB y no pandas?

Con 50.5M filas y 15.9 GB, pandas requiere cargar todo en RAM (estimado ~15-20 GB con los tipos de Python) — inviable en una máquina de desarrollo. DuckDB ejecuta SQL vectorizado en columnas directamente sobre los archivos, usando ~3 GB de RAM independientemente del tamaño del CSV. Sin servidor, sin cluster: es un `.connect()` dentro del mismo proceso Python.

### Estrategia all-VARCHAR con TRY_CAST

Todos los campos se leen como `VARCHAR` en DuckDB y después se castean explícitamente con `TRY_CAST`. Si el motor infiriera los tipos al leer, un valor como `'28:00:00'` (hora inválida en el CSV fuente) haría fallar la columna entera o se convertiría a `NaT` de forma silenciosa. Con all-VARCHAR, cada casteo fallido produce un `NULL` auditable que queda registrado en el `quality.json` de esa partición.

### Escritura atómica (tmp → rename)

Todos los Parquet se escriben primero a un archivo temporal con nombre UUID y después se mueven con `shutil.move()` (que usa `rename` del OS). `rename` es atómica a nivel de sistema de archivos: o el archivo existe completo o no existe. Sin esto, un proceso interrumpido deja un Parquet a medio escribir que el siguiente run lee como válido.

### El grain bug y cómo lo detecté

Al cargar `fct_trip` por primera vez encontré solo **27 filas** en lugar de 3.6 millones. El constraint de unicidad estaba definido como `UNIQUE(id_viaje, cut_sk)`.

El problema: `id_viaje` no es un ID global de viaje — es un **contador diario por tarjeta** que va de 1 a 27 (máximo 27 viajes por día por tarjeta Bip!). Para 520k viajes distintos, los valores de `id_viaje` son `1, 2, 3, ..., 27` repetidos millones de veces. Con el constraint original, la 28ª fila con `id_viaje = 1` violaba la unicidad y se rechazaba.

Lo detecté comparando el count en staging con el count en la fact:

```sql
SELECT COUNT(*) FROM staging.stg_viajes_trip;          -- 520,431
SELECT COUNT(*) FROM dw.fct_trip WHERE cut_sk = 42;    -- 27
```

La corrección fue cambiar el grain a `(cut_sk, id_tarjeta, id_viaje)` con un filtered index que excluye viajes en efectivo (`WHERE id_tarjeta IS NOT NULL`, porque para esos no existe BK que garantice unicidad):

```sql
CREATE UNIQUE NONCLUSTERED INDEX UX_fct_trip_grain
    ON dw.fct_trip (cut_sk, id_tarjeta, id_viaje)
    WHERE id_tarjeta IS NOT NULL;
```

Post-fix: 3,605,891 filas en `fct_trip` y 14,423,564 en `fct_trip_leg`.

### Idempotencia con MERGE

El pipeline puede re-ejecutarse sobre el mismo corte sin duplicar datos. La capa Gold usa `MERGE` de SQL Server con el grain como condición de match: si la fila ya existe, no se inserta. Segunda ejecución: 0 filas insertadas.

---

## Stack

| Categoría | Herramienta | Uso |
|-----------|------------|-----|
| Lenguaje | Python 3.11 | ETL, orquestación, calidad |
| Procesamiento | DuckDB | Transformación Silver (in-process, vectorizado) |
| Contratos | Pydantic v2 | Validación de muestra post-transform |
| Data Warehouse | SQL Server | Capa Gold, Star Schema Kimball |
| Conectividad | pyodbc | Conexión SQL Server (fast_executemany) |
| Formato | Parquet ZSTD | Almacenamiento intermedio Silver |
| Particionado | Hive-style | Compatibilidad futura con Spark/Trino |
| Catálogo | JSON (_meta.json) | Linaje por partición |

---

## Estructura del repositorio

```
Data_Lakehouse_Movilidad_Pública_Santiago/
│
├── extract_data.py          # Descomprime ZIPs anidados y .csv.gz
├── build_lake.py            # Organiza CSVs en particionado Hive + _meta.json
├── build_catalog.py         # Genera lake_catalog.json
├── analyze_columns.py       # Análisis de calidad de columnas
│
├── src/
│   ├── silver/
│   │   ├── catalog.py       # Lee lake_catalog.json, resuelve rutas
│   │   ├── transforms.py    # Transformaciones DuckDB por dataset
│   │   ├── contracts.py     # Modelos Pydantic v2 (ViajesTripRow, etc.)
│   │   ├── transform_silver.py  # CLI: --dataset all|viajes|etapas|subidas_30m
│   │   └── tests_smoke.py   # 22 smoke tests de la capa Silver
│   └── gold/
│       ├── load_gold.py     # GoldLoader: staging → dims → facts con MERGE
│       └── sql_helpers.py   # Conexión pyodbc, bulk_insert, DDL executor
│
├── docs/
│   ├── gold/
│   │   ├── ddl_gold.sql     # DDL completo: 4 staging + 9 dims + 4 facts
│   │   └── cleanup_cut.sql  # Script para borrar un corte de Gold
│   └── README.md            # Documentación del modelo dimensional
│
├── lake/
│   ├── lake_catalog.json    # Catálogo centralizado de todas las particiones
│   ├── column_analysis.json # Análisis de calidad por columna
│   └── raw/dtpm/            # Bronze — particionado Hive
│       ├── dataset=viajes/
│       ├── dataset=etapas/
│       └── dataset=subidas_30m/
│
└── docs/
    ├── ARQUITECTURA_MEDALLION.md
    ├── silver_layer_decisions.md
    ├── DIA_1_BRONZE_Y_PYTHON.md
    ├── DIA_2_SILVER_Y_CALIDAD.md
    └── DIA_3_GOLD_Y_KIMBALL.md
```






---

## Cómo ejecutar

```bash
pip install -r requirements.txt
```

```powershell
# 1. Bronze: extraer fuentes
python extract_data.py

# 2. Bronze: organizar en Hive-style + _meta.json
python build_lake.py

# 3. Bronze: generar catálogo
python build_catalog.py

# 4. Silver: transformar todos los datasets
python -m src.silver.transform_silver --dataset all

# 5. Silver: correr smoke tests
python -m pytest src/silver/tests_smoke.py -v

# 6. Gold: cargar un corte específico
python -m src.gold.load_gold --cut 2025-04-21

# 7. Gold: cargar todos los datasets
python -m src.gold.load_gold --dataset all
```

La capa Gold requiere SQL Server con las variables de entorno configuradas en `.env`:
```
SQLSERVER_HOST=localhost
SQLSERVER_DB=MovilidadDW
# Sin SQLSERVER_USER → usa Windows Authentication automáticamente
```

---

## Preguntas de negocio que responde el DW

- ¿Cuáles son los períodos de mayor demanda por modo de transporte?
- ¿Qué flujos origen-destino (paradero a paradero) concentran más viajes?
- ¿Cuántos transbordos promedio realiza un usuario según la hora del día?
- ¿Cuál es el tiempo promedio de espera en paradero por servicio?
- ¿Qué paraderos tienen mayor concentración de subidas en hora punta?
- ¿Cómo varía el propósito del viaje (trabajo/hogar/educación) por zona de la ciudad?
- ¿Qué servicios presentan mayor tasa de bajada no registrada (pérdida GPS en túnel)?

---

## Roadmap

- [x] Extracción — ZIPs anidados y archivos `.csv.gz`
- [x] Bronze — Particionado Hive + `_meta.json` por partición
- [x] Catálogo — `lake_catalog.json` con linaje completo
- [x] Análisis de columnas — null rate, dtype, selección de negocio
- [x] Silver — DuckDB: tipado, normalización, quarantine, Parquet ZSTD
- [x] Contratos Silver — Pydantic v2 con validación de muestra
- [x] Smoke tests — 22/22 pasando
- [x] DDL Gold — 18 tablas: staging + dims + facts
- [x] Carga Gold — bulk insert + MERGE idempotente
- [x] SCD2 — `dim_stop` y `dim_service` con `valid_from/valid_to/row_hash`
- [x] Grain fix — `UX_fct_trip_grain` filtered index
- [ ] GitHub Actions — CI con smoke tests en cada push
- [ ] Dashboard — Evidence.dev o Streamlit sobre los Parquet Silver
- [ ] Datos históricos — automatizar ingesta de nuevos cortes DTPM

---

## Documentación

- [`docs/DIA_1_BRONZE_Y_PYTHON.md`](docs/DIA_1_BRONZE_Y_PYTHON.md) — Bronze, Python stdlib, Hive partitioning
- [`docs/DIA_2_SILVER_Y_CALIDAD.md`](docs/DIA_2_SILVER_Y_CALIDAD.md) — DuckDB, Pydantic v2, patrones de calidad
- [`docs/DIA_3_GOLD_Y_KIMBALL.md`](docs/DIA_3_GOLD_Y_KIMBALL.md) — Kimball, SCD2, MERGE, grain
- [`lake/README.md`](lake/README.md) — Catálogo técnico del lake
- [`docs/README.md`](docs/README.md) — Modelo dimensional completo

---

*Datos abiertos del DTPM — Directorio de Transporte Público Metropolitano de Santiago.*

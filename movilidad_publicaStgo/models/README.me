# 📊 Data Warehouse (Curated/Gold) — DTPM Movilidad Pública Santiago

Este proyecto transforma datos abiertos del **DTPM** (tarjeta bip! + GPS) en un **Data Warehouse analítico** listo para exploración, dashboards y preguntas de negocio.

En vez de publicar un único diagrama gigante (que suele volverse difícil de leer por la cantidad de relaciones), el modelo se presenta como **Data Marts por áreas temáticas**.  
Esto hace que el diseño sea **más claro**, más fácil de mantener y más fácil de entender incluso para personas no técnicas.

> ✅ Importante (contexto DTPM):  
> - Los datos **no incluyen evasión**.  
> - “Viaje” y “bajada estimada” provienen de una metodología de inferencia (no solo reglas de pago).  
> - El dataset agregado de subidas/bajadas es un **promedio mensual representativo** (en general 5 días hábiles).  

---

## 🧠 ¿Qué es un Data Mart y por qué separé los diagramas?

Un **Data Mart** es una vista del Data Warehouse enfocada en un **tema específico**, por ejemplo:
- *Demanda de viajes y OD*
- *Operación por etapas (validaciones, espera, bajadas detectadas)*
- *Demanda agregada por paradero y franja horaria*
- *Secuencias y transbordos por etapa del viaje*

**Ventajas de esta separación:**
- 📌 Cada diagrama cuenta una historia clara (1 fact principal + dims relevantes).
- 🧭 Se evita el “spaghetti diagram” (líneas cruzadas por todos lados).
- 🔎 Es más fácil entender **qué preguntas responde** cada parte del DW.
- 🧩 Las dimensiones clave se mantienen **conformadas** (mismas dims compartidas), por lo que los marts se pueden cruzar cuando es necesario.

---

## 🏗️ Conceptos clave del modelo (en simple)

### 1) Facts vs Dimensions
- **Fact tables**: registran eventos/mediciones (ej: un viaje, una validación, una etapa).
- **Dimensions**: describen el contexto (fecha, hora, paradero, servicio, modo).

### 2) Role-Playing Dimensions (muy importante)
La misma dimensión puede aparecer varias veces en una fact con roles distintos.
Ejemplo: un paradero puede ser “subida” y “bajada”, pero sigue siendo la misma tabla `dim_stop`.

- `board_stop_sk` → `dim_stop` (paradero de subida)
- `alight_stop_sk` → `dim_stop` (paradero de bajada)

### 3) Degenerate Dimensions
Identificadores que se guardan **directo en la fact** porque no tiene sentido crear una dim aparte.
Ejemplo: `id_viaje`, `id_tarjeta` viven en `fct_trip`.

### 4) SCD Tipo 2 (historia en dimensiones)
Algunas dimensiones cambian con el tiempo (nombres, atributos, correcciones, etc.).  
Con **SCD2** guardamos el historial con:
- `valid_from`, `valid_to`
- `is_current`

Así un evento histórico mantiene el contexto correcto “en su fecha” (as-of), sin reescribir el pasado.

---

## 📚 Diagramas / Data Marts incluidos

> Los diagramas están en: `docs/modeling/`

- `data_mart_trips_od.png`
- `data_mart_trip_legs.png`
- `data_mart_stages_operations.png`
- `data_mart_network_demand.png`

---

# 1) 🧭 Mart “Trips & OD” (Movilidad / Demanda de viajes)

📌 **Objetivo:** entender la movilidad como “viajes completos” (origen-destino), duración, transbordos y propósito.

### Fact principal
- **`fct_trip`**  
  **Grano:** 1 fila = 1 viaje (`id_viaje`)

### Dimensiones clave
- `dim_date` (incluye `tipo_dia`: LABORAL / SABADO / DOMINGO)
- `dim_time_30m` (0–47 media horas)
- `dim_stop` (role-playing: `origin_stop_sk`, `dest_stop_sk`)
- `dim_fare_period`
- `dim_purpose`
- `dim_cut` (linaje del corte/fuente)

### Preguntas que responde
- ¿Cuáles son las **horas punta reales** por tipo de día?
- ¿Qué comunas/paraderos **generan** más viajes y cuáles **reciben** más?
- Top **OD** (origen → destino) por franja horaria.
- ¿Qué porcentaje de viajes son **directos** (1 etapa) vs con **transbordos**?
- ¿Qué tan largos son los viajes (tiempo/distancia) según día y hora?

---

# 2) 🔁 Mart “Trip Legs” (Etapas dentro del viaje)

📌 **Objetivo:** analizar **transbordos**, secuencias (BUS → METRO), puntos de intercambio y demanda por servicio “por etapa”.

### ¿Por qué existe este Mart?
El dataset `viajes` trae muchas columnas repetidas: `..._1..4`.  
Para análisis, eso es incómodo.  
Este Mart convierte esas columnas en filas mediante un **UNPIVOT**.

### Fact principal
- **`fct_trip_leg`**  
  **Grano:** 1 fila = 1 etapa dentro del viaje (`id_viaje` + `leg_seq`)

### Dimensiones clave
- `dim_date`, `dim_time_30m`
- `dim_stop` (role-playing: `board_stop_sk`, `alight_stop_sk`)
- `dim_mode`, `dim_service`
- `dim_operator_contract`
- `dim_fare_period` (en este mart se usa principalmente para `alight`)
- `dim_cut`

### Preguntas que responde
- ¿Cuáles son los **puntos de transbordo** más comunes?
- ¿Qué secuencias son más frecuentes? (BUS→METRO, METRO→BUS, etc.)
- Ranking de **servicios** más usados “por etapa”.
- ¿En qué franjas ocurre más el intercambio modal?
- ¿Cuánto aporta cada etapa al tiempo total del viaje? (tv/te/tc por leg)

---

# 3) 🛠️ Mart “Stages & Operations” (Operación por validación)

📌 **Objetivo:** ver el sistema a nivel “operacional”: validaciones, espera, bajadas detectadas, performance por paradero/servicio.

### Fact principal
- **`fct_validation`**  
  **Grano:** 1 fila = 1 validación / etapa (`id_etapa`)

### Dimensiones clave
- `dim_date`, `dim_time_30m`
- `dim_stop` (role-playing: `board_stop_sk`, `alight_stop_sk`)
- `dim_mode`
- `dim_service` (role-playing: `service_board_sk`, `service_alight_sk`)
- `dim_fare_period` (role-playing: `fare_period_board_sk`, `fare_period_alight_sk`)
- `dim_operator_contract`
- `dim_cut`

### Preguntas que responde
- ¿Cómo varía el **tiempo de espera** estimado por hora/modo/servicio?
- ¿Qué servicios tienen mayor duración de etapa (tiempo_etapa) por franja?
- ¿Cuál es el % de etapas con **bajada detectada** (`tiene_bajada`) por modo/servicio?
- Top paraderos con mayor concentración de validaciones.
- ¿Dónde aparecen más casos “no asignables” o bajadas ausentes? (calidad/metodología)

---

# 4) 🌐 Mart “Network Demand” (Demanda agregada por paradero / 30m)

📌 **Objetivo:** perfiles horarios de subidas promedio por paradero y modo.

Este mart es ideal para:
- perfiles visuales (curvas por media hora),
- ranking de paraderos,
- comparación LABORAL vs fin de semana en forma simple.

### Fact principal
- **`fct_boardings_30m`**  
  **Grano:** (mes, paradero, media hora, modo, tipo_dia)

### Notas importantes
- `tipo_dia` se mantiene como **degenerate dimension** dentro de la fact para simplificar.
- La métrica es **promedio representativo mensual** (no conteo exacto diario).

### Dimensiones clave
- `dim_date` (ancla del mes, ej. 2025-04-01)
- `dim_time_30m`, `dim_stop`, `dim_mode`
- `dim_cut`

### Preguntas que responde
- ¿Cuáles son los paraderos con más subidas promedio en **hora punta**?
- ¿Cómo es el **perfil horario** de un paradero específico?
- ¿Qué paraderos cambian más entre LABORAL vs SÁBADO vs DOMINGO?
- ¿Cómo se distribuye la demanda por modo en el día?

---

## 🔍 ¿Cómo se conectan los marts?
Todos comparten dimensiones conformadas como:
- `dim_date`
- `dim_time_30m`
- `dim_stop`
- `dim_mode`
- `dim_service` (cuando aplica)

Eso permite:
- comparar demanda agregada (`fct_boardings_30m`) vs validaciones (`fct_validation`)
- relacionar transbordos (`fct_trip_leg`) con OD y duración (`fct_trip`)

---

## ✅ Qué gana este modelo
- Medallion: **raw → processed → curated (DW)**
- Conformed dimensions + role-playing
- Degenerate dimensions bien usadas (no “dims de relleno”)
- SCD2 para historial (en dims seleccionadas)
- Facts con grano explícito y consultas analíticas simples
- Diseñado para BI (Power BI) y SQL de entrevistas (window functions, top-N, cohorts, etc.)

---

## 📌 Próximos pasos (roadmap)
- Cargar más cortes (meses/años) para análisis temporal real.
- Agregar tests de calidad (null rates, rangos, duplicados, FK coverage).
- Publicar dashboards (Power BI) usando estas facts/dims.
- Optimización SQL Server: Columnstore en facts + particionamiento por fecha.

---

## 📎 Glossary (rápido)
- **OD**: Origen–Destino
- **Leg/Etapa**: segmento dentro de un viaje (bus/metro/tren)
- **Role-playing dimension**: misma dim usada con distintos roles (subida/bajada)
- **Degenerate dimension**: identificador guardado en la fact
- **SCD2**: dimensión con historial (valid_from/valid_to)
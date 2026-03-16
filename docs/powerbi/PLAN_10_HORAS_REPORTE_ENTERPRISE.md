# Plan de Ejecucion - Reporte Power BI Enterprise (10 Horas)

Objetivo: construir en 10 horas un reporte de nivel Senior/Semi-Senior orientado a decision operativa y revenue assurance para transporte publico metropolitano.

Alcance funcional del dia:
- 3 paginas obligatorias terminadas (Resumen Ejecutivo, Revenue Assurance, Eficiencia Operacional).
- 1 pagina opcional (Intermodalidad) en version MVP.
- 5 medidas DAX de alto impacto funcionando y validadas.
- Storytelling ejecutivo listo para presentar en 3 minutos.

---

## Hora 1 (00:00-01:00) - Base tecnica y control del modelo

### Tareas exactas
1. Abrir Model View y revisar relaciones activas/inactivas.
2. Confirmar estas relaciones activas:
   - `dw_fct_boardings_30m[stop_sk] -> dw_dim_stop[stop_sk]`
   - `dw_fct_boardings_30m[time_30m_sk] -> dw_dim_time_30m[time_30m_sk]`
   - `dw_fct_boardings_30m[month_date_sk] -> dw_dim_date[date_sk]`
   - `dw_fct_boardings_30m[mode_sk] -> dw_dim_mode[mode_sk]`
   - `dw_fct_validation[board_stop_sk] -> dw_dim_stop[stop_sk]`
   - `dw_fct_validation[date_board_sk] -> dw_dim_date[date_sk]`
   - `dw_fct_validation[time_board_30m_sk] -> dw_dim_time_30m[time_30m_sk]`
   - `dw_fct_validation[mode_sk] -> dw_dim_mode[mode_sk]`
3. Confirmar role-playing date en `dw_fct_trip`:
   - `date_start_sk` activa.
   - `date_end_sk` inactiva.
4. Marcar `dw_dim_date` como Date Table con `full_date`.
5. Ocultar columnas tecnicas:
   - Todos los `*_sk` de visualizacion.
   - `loaded_at`, `row_hash`, campos de auditoria.
6. En Report View, desactivar resumen por defecto en IDs y codigos (Do not summarize).

### Entregable de la hora
- Modelo limpio, navegable y sin ambiguedades.

### Check de salida
- No hay relaciones many-to-many inesperadas.
- No hay relaciones bidireccionales innecesarias.

---

## Hora 2 (01:00-02:00) - Medidas DAX core

### Tareas exactas
1. Crear tabla `_Medidas`.
2. Cargar medidas base:
   - `Viajes Total`
   - `Validaciones Total`
   - `Subidas`
   - `Legs`
3. Cargar medidas de valor de negocio:
   - `Pasajeros Estimados Transportados`
   - `Tasa Evasion Estimada %`
   - `Indice Saturacion Horaria`
   - `Tasa Intermodalidad %`
   - `Variacion YTD Validaciones %`
4. Cargar auxiliares de mapa:
   - `Subidas Mapa (Hora Unica)`
   - `Subidas Promedio por Hora (Rango)`
   - `Indice Presion Operativa`
5. Formatear medidas:
   - Porcentajes con 1-2 decimales.
   - Enteros con separador de miles.

### Entregable de la hora
- Libreria DAX funcional para todo el reporte.

### Check de salida
- Cada medida probada en una tabla simple con un slicer de fecha.

---

## Hora 3 (02:00-03:00) - Pagina 1: Resumen Ejecutivo

### Tareas exactas
1. Crear pagina `01_Resumen_Ejecutivo`.
2. Agregar 4 KPI cards arriba:
   - Validaciones Total
   - Pasajeros Estimados Transportados
   - Tasa Evasion Estimada %
   - Indice Saturacion Horaria
3. Agregar linea de demanda por hora (x = `dw_dim_time_30m[hour]`, y = `Subidas`).
4. Agregar mapa principal (Azure Maps):
   - Lat/Lon de `dw_dim_stop`.
   - Tamano = `Subidas Mapa (Hora Unica)`.
   - Leyenda = `dw_dim_mode[mode_code]`.
   - Tooltip = Subidas, Evasion, Saturacion.
5. Agregar slicers a la derecha:
   - `hour`
   - `tipo_dia` (usar la columna correcta segun fact activa)
   - `mode_code`
6. Configurar interacciones para que slicers filtren todos los visuales.

### Entregable de la hora
- Portada ejecutiva usable en demo.

### Check de salida
- Cambiar hora modifica cards, linea y mapa en simultaneo.

---

## Hora 4 (03:00-04:00) - Pagina 2: Revenue Assurance

### Tareas exactas
1. Crear pagina `02_Revenue_Assurance`.
2. Agregar visual Waterfall:
   - Inicio: Pasajeros Estimados
   - Resta: Validaciones
   - Final: Brecha No Validada
3. Agregar heatmap (matriz):
   - Filas: `mode_code`
   - Columnas: `hour`
   - Valor: `Tasa Evasion Estimada %`
4. Agregar matriz operador/comuna:
   - Filas: operador
   - Columnas: comuna
   - Valores: brecha y tasa evasión.
5. Agregar scatter:
   - Eje X: Subidas
   - Eje Y: Tasa Evasion Estimada %
   - Detalle: stop_code
6. Formato condicional:
   - Verde < 10%
   - Ambar 10%-20%
   - Rojo > 20%

### Entregable de la hora
- Pagina de control de recaudacion con foco en riesgo.

### Check de salida
- Top outliers identificables por operador/paradero.

---

## Hora 5 (04:00-05:00) - Pagina 3: Eficiencia Operacional

### Tareas exactas
1. Crear pagina `03_Eficiencia_Operacional`.
2. Agregar ranking Top 20 paraderos:
   - Orden por `Indice Presion Operativa`.
3. Agregar barra por operador:
   - Validaciones, Espera Promedio, Saturacion.
4. Agregar visual de tendencia por hora punta:
   - Curva `Subidas` vs `P95` (si usas medida de saturacion).
5. Agregar segmentador de periodo tarifario (`dw_dim_fare_period`).
6. Agregar tarjeta de alerta:
   - `Paradero Critico #1` (medida/topN).

### Entregable de la hora
- Pagina orientada a decisiones operativas (frecuencia, fiscalizacion, refuerzo).

### Check de salida
- Con filtro Punta cambian ranking y saturacion.

---

## Hora 6 (05:00-06:00) - Pagina 4 (MVP): Intermodalidad

### Tareas exactas
1. Crear pagina `04_Intermodalidad_MVP`.
2. Agregar KPI: `Tasa Intermodalidad %`.
3. Agregar barras de combinaciones modales:
   - BUS->METRO, METRO->BUS, BUS->BUS, etc. (segun disponibilidad).
4. Agregar matriz OD comuna origen-destino (si ya tienes campos listos).
5. Agregar promedio de transbordos y tiempo de transferencia.

### Entregable de la hora
- Version MVP de movilidad intermodal lista para evolucion.

### Check de salida
- Se puede responder en 1 minuto: "Que tan intermodal es el sistema?"

---

## Hora 7 (06:00-07:00) - UX/UI de nivel Enterprise

### Tareas exactas
1. Definir tema visual consistente (colores semanticos):
   - Azul: volumen
   - Naranjo: saturacion
   - Rojo: riesgo/evasion
2. Homogeneizar tipografias y tamano de titulos.
3. Ajustar layout por grilla:
   - Cards arriba
   - Visual principal centro
   - Detalle lateral
4. Crear titulos dinamicos con contexto (hora, modo, tipo_dia).
5. Revisar accesibilidad:
   - Contraste
   - Tamaño de labels
   - No depender solo de color

### Entregable de la hora
- Reporte visualmente profesional y consistente.

### Check de salida
- Todas las paginas parecen parte de un unico producto.

---

## Hora 8 (07:00-08:00) - Tooltips avanzados y Drill-through

### Tareas exactas
1. Crear pagina tooltip `TT_Paradero` con:
   - Subidas
   - Evasion
   - Espera
   - Saturacion
2. Asignar tooltip personalizado al mapa y scatter.
3. Crear drill-through `Detalle_Paradero`:
   - Serie horaria
   - Comparativa laboral/sabado/domingo
   - Desglose por modo
4. Crear drill-through `Detalle_Operador`:
   - Tasa evasión
   - Carga operativa

### Entregable de la hora
- Navegacion analitica profunda desde visuales ejecutivos.

### Check de salida
- Click derecho en mapa abre detalle contextual correcto.

---

## Hora 9 (08:00-09:00) - QA analitico y performance

### Tareas exactas
1. Validar numericamente medidas clave contra SQL de control:
   - Viajes
   - Validaciones
   - Subidas
   - Sabado/domingo
2. Revisar performance:
   - Visuales lentos
   - Medidas con iteradores costosos
3. Optimizar:
   - Reducir campos en tooltips pesados
   - Evitar visuales con cardinalidad excesiva
4. Revisar filtros cruzados problematicos.
5. Test rapido de 5 escenarios:
   - LABORAL + 8 AM
   - SABADO + 12 PM
   - DOMINGO + 18 PM
   - Solo BUS
   - Solo METRO

### Entregable de la hora
- Reporte confiable y performante.

### Check de salida
- Ningun visual principal falla al cambiar slicers.

---

## Hora 10 (09:00-10:00) - Cierre para portafolio

### Tareas exactas
1. Definir 3 hallazgos ejecutivos reales (1 linea cada uno).
2. Capturar screenshots de alta calidad (4 paginas).
3. Redactar texto de impacto para README:
   - problema
   - solucion
   - impacto
4. Preparar pitch de 3 minutos:
   - Contexto
   - Metodo
   - Insights
   - Decision sugerida
5. Checklist final de entrega:
   - nombres de paginas
   - ortografia
   - filtros por defecto
   - orden de tabs

### Entregable de la hora
- Producto final presentable a reclutador tecnico, gerente o area BI.

### Check de salida
- Puedes explicar el tablero sin leer notas en menos de 3 minutos.

---

## Checklist rapido (si te quedan 20 minutos extra)

1. Agregar una pagina "Anexos Tecnicos" con definicion de metricas.
2. Exportar PDF para version ejecutiva.
3. Dejar bookmark de "Hora Punta AM" y "Hora Punta PM".
4. Guardar version con fecha: `Reporte_Movilidad_Enterprise_YYYYMMDD.pbix`.

---

## Criterio de exito del dia

El reporte se considera terminado si cumple:
1. Responde decisiones de negocio (no solo descripcion).
2. Muestra brecha demanda vs validacion con claridad.
3. Identifica cuellos operativos por hora, modo y paradero.
4. Permite deep-dive con tooltip y drill-through.
5. Es defendible tecnicamente en entrevista Senior/Semi-Senior.

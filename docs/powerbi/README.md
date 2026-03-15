# Power BI - Documentacion DAX

Este directorio concentra la documentacion del trabajo analitico realizado en Power BI para el proyecto **Data Lakehouse Movilidad Publica Santiago**.

## Archivos

- [DAX_MEDIDAS_POWERBI.md](DAX_MEDIDAS_POWERBI.md)

## Que contiene `DAX_MEDIDAS_POWERBI.md`

El archivo incluye las medidas DAX usadas para construir la pagina de **Mapa Presion Operativa** y sus variantes:

1. Medidas base de volumen (`Viajes`, `Validaciones`, `Legs`, `Subidas`).
2. Medidas de calidad de servicio (`Espera Promedio`, `P50`, `P90`, `% Bajada Registrada`).
3. Medidas de normalizacion para mapa.
4. Versiones del `Indice Presion Operativa` (simple, ponderada y robusta).
5. Medidas para analisis por hora exacta y por rango horario.
6. Titulos dinamicos para mejorar narrativa ejecutiva.
7. Filtro por tipo de dia con tabla desconectada (`TREATAS`).
8. Medidas de depuracion para validar contexto de filtros.

## Objetivo del documento

Facilitar:

- Reuso rapido de medidas en nuevas paginas de reporte.
- Trazabilidad de decisiones de modelado DAX.
- Estandarizacion para presentaciones tecnicas y entrevistas.

## Recomendaciones de uso

1. Crear una tabla de medidas `_Medidas` y pegar ahi las formulas.
2. Empezar por medidas base, luego agregar indice y medidas dinamicas.
3. Validar contexto con las medidas de debug antes de publicar.
4. En mapas por hora, usar seleccion unica de `hour` para evitar sobreacumulacion.

## Alcance

Estas medidas fueron pensadas para el modelo actual del DW en SQL Server con:

- `fct_trip`
- `fct_trip_leg`
- `fct_validation`
- `fct_boardings_30m`
- `dim_date`, `dim_time_30m`, `dim_mode`, `dim_stop`

Si cambia el modelo (nombres de columnas o relaciones), ajustar las medidas segun corresponda.

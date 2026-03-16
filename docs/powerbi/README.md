# Power BI - Documentacion DAX

Este directorio concentra la documentacion del trabajo analitico realizado en Power BI para el proyecto **Data Lakehouse Movilidad Publica Santiago**.

## Archivos

- [DAX_ENTERPRISE_MEDIDAS.md](DAX_ENTERPRISE_MEDIDAS.md)

## Que contiene DAX_ENTERPRISE_MEDIDAS.md

El archivo incluye el catalogo consolidado de medidas DAX usadas en las paginas ejecutivas y estrategicas:

1. Base de volumen y filtros por tipo de dia.
2. Universo comparable de recaudacion (BUS y METRO).
3. KPI oficial de cobertura, brecha y sobrevalidacion.
4. Estado de comparabilidad y calidad horaria.
5. Medidas de priorizacion estrategica y recuperacion potencial.

## Objetivo del documento

Facilitar:

- Reuso rapido de medidas en nuevas paginas de reporte.
- Trazabilidad de decisiones de modelado DAX.
- Estandarizacion para presentaciones tecnicas y entrevistas.

## Recomendaciones de uso

1. Crear una tabla de medidas `_Medidas` y pegar ahi las formulas.
2. Implementar primero base compatible y luego KPI comparable.
3. Mantener visibles las medidas de estado de comparabilidad.
4. Evitar mezclar medidas legacy en paginas ejecutivas.

## Alcance

Estas medidas fueron pensadas para el modelo actual del DW en SQL Server con:

- `fct_trip`
- `fct_trip_leg`
- `fct_validation`
- `fct_boardings_30m`
- `dim_date`, `dim_time_30m`, `dim_mode`, `dim_stop`

Si cambia el modelo (nombres de columnas o relaciones), ajustar las medidas segun corresponda.

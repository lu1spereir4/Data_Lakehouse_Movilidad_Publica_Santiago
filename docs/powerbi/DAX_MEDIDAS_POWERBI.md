# DAX Measures - Power BI (Mapa Presion Operativa)

Este documento consolida las medidas DAX que fuimos creando durante la construccion del reporte en Power BI.

Notas:
- Ajusta nombres de columnas si en tu modelo aparecen con una variante distinta.
- Recomendacion: crear una tabla de medidas llamada `_Medidas` y guardar todo ahi.

## 1) Medidas base de volumen

```DAX
Viajes Total = COUNTROWS(fct_trip)
```

```DAX
Validaciones Total = COUNTROWS(fct_validation)
```

```DAX
Validaciones = COUNTROWS(fct_validation)
```

```DAX
Legs = COUNTROWS(fct_trip_leg)
```

```DAX
Subidas Total = SUM(fct_boardings_30m[boarding_count])
```

```DAX
Subidas = SUM(fct_boardings_30m[subidas_promedio])
```

## 2) Medidas de calidad/servicio

```DAX
Espera Promedio Min = AVERAGE(fct_validation[t_espera_media_min])
```

```DAX
Espera P50 =
PERCENTILEX.INC(
    FILTER(fct_validation, NOT ISBLANK(fct_validation[t_espera_media_min])),
    fct_validation[t_espera_media_min],
    0.5
)
```

```DAX
Espera P90 Min =
PERCENTILEX.INC(
    FILTER(fct_validation, NOT ISBLANK(fct_validation[t_espera_media_min])),
    fct_validation[t_espera_media_min],
    0.9
)
```

```DAX
Espera P90 =
PERCENTILEX.INC(
    FILTER(fct_validation, NOT ISBLANK(fct_validation[t_espera_media_min])),
    fct_validation[t_espera_media_min],
    0.9
)
```

```DAX
% Bajada Registrada =
DIVIDE(
    CALCULATE(COUNTROWS(fct_validation), fct_validation[tiene_bajada] = 1),
    COUNTROWS(fct_validation)
)
```

```DAX
Brecha Espera L-D =
CALCULATE([Espera Promedio Min], dim_date[is_laboral] = 1)
- CALCULATE([Espera Promedio Min], dim_date[day_type] = "DOMINGO")
```

## 3) Medidas de comportamiento de viaje

```DAX
Etapas Promedio = AVERAGE(fct_trip[n_etapas])
```

```DAX
% Con Transbordo =
DIVIDE(
    COUNTROWS(FILTER(fct_trip, fct_trip[n_etapas] > 1)),
    COUNTROWS(fct_trip)
)
```

```DAX
Viajes por Fecha Fin =
CALCULATE(
    [Viajes Total],
    USERELATIONSHIP(fct_trip[date_end_sk], dim_date[date_sk])
)
```

## 4) Medidas de mapa (normalizacion e indice)

### 4.1 Version ponderada (Subidas + Espera)

```DAX
Subidas Normalizado =
DIVIDE(
    [Subidas],
    CALCULATE(MAXX(ALLSELECTED(dim_stop[stop_sk]), [Subidas]))
)
```

```DAX
Espera Normalizado =
DIVIDE(
    [Espera Promedio Min],
    CALCULATE(MAXX(ALLSELECTED(dim_stop[stop_sk]), [Espera Promedio Min]))
)
```

```DAX
Indice Presion Operativa =
0.65 * [Subidas Normalizado] + 0.35 * [Espera Normalizado]
```

### 4.2 Version simple (100 = max contexto)

```DAX
Subidas Max Contexto =
MAXX(
    ALLSELECTED(dim_stop[stop_sk]),
    [Subidas]
)
```

```DAX
Indice Presion Operativa =
ROUND(
    100 * DIVIDE([Subidas], [Subidas Max Contexto]),
    1
)
```

### 4.3 Version robusta para evitar todos en 100

```DAX
Subidas Max Mapa =
VAR TStops =
    ADDCOLUMNS(
        ALLSELECTED(dim_stop[stop_sk]),
        "@SubidasStop", CALCULATE([Subidas], REMOVEFILTERS(dim_stop[stop_sk]))
    )
RETURN
    MAXX(TStops, [@SubidasStop])
```

```DAX
Indice Presion Operativa =
ROUND(
    100 * DIVIDE([Subidas], [Subidas Max Mapa]),
    1
)
```

```DAX
Indice Presion Categoria =
SWITCH(
    TRUE(),
    [Indice Presion Operativa] >= 80, "Alta",
    [Indice Presion Operativa] >= 50, "Media",
    "Baja"
)
```

```DAX
Banda Espera =
SWITCH(
    TRUE(),
    [Espera Promedio Min] >= 20, "Alta",
    [Espera Promedio Min] >= 12, "Media",
    "Baja"
)
```

## 5) Medidas para filtro horario

```DAX
Subidas por Hora =
CALCULATE(
    SUM(fct_boardings_30m[subidas_promedio]),
    KEEPFILTERS(VALUES(dim_time_30m[hour]))
)
```

```DAX
Subidas Mapa (Hora Unica) =
IF(
    HASONEVALUE(dim_time_30m[hour]),
    [Subidas],
    BLANK()
)
```

```DAX
Subidas Promedio por Hora (Rango) =
DIVIDE(
    [Subidas],
    DISTINCTCOUNT(dim_time_30m[hour])
)
```

## 6) Medidas de titulos dinamicos

```DAX
Titulo Hora =
VAR h = SELECTEDVALUE(dim_time_30m[hour])
RETURN
IF(
    ISBLANK(h),
    "Mapa de Presion Operativa - Todas las horas",
    "Mapa de Presion Operativa - Hora " & FORMAT(h, "00") & ":00"
)
```

```DAX
Titulo Mapa Operativo =
VAR h = SELECTEDVALUE(dim_time_30m[hour])
VAR td = SELECTEDVALUE(dim_date[tipo_dia], "Todos")
VAR m = SELECTEDVALUE(dim_mode[mode_desc], "Todos los modos")
RETURN
IF(
    ISBLANK(h),
    "Presion operativa por paradero | Selecciona una hora",
    "Presion operativa por paradero | Hora " & FORMAT(h,"00") & ":00 | " & td & " | " & m
)
```

## 7) Medidas de filtro por tipo de dia (tabla desconectada)

### 7.1 Tabla desconectada

```DAX
DimTipoDia =
DATATABLE(
    "tipo_dia", STRING,
    { {"LABORAL"}, {"SABADO"}, {"DOMINGO"} }
)
```

### 7.2 Medidas con TREATAS

```DAX
Subidas (TipoDia) =
CALCULATE(
    SUM(fct_boardings_30m[subidas_promedio]),
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_boardings_30m[tipo_dia])
)
```

```DAX
Validaciones (TipoDia) =
CALCULATE(
    COUNTROWS(fct_validation),
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_validation[tipo_dia])
)
```

```DAX
Espera Promedio Min (TipoDia) =
CALCULATE(
    AVERAGE(fct_validation[t_espera_media_min]),
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_validation[tipo_dia])
)
```

## 8) Medidas de depuracion

```DAX
Debug Stops En Contexto = COUNTROWS(ALLSELECTED(dim_stop[stop_sk]))
```

```DAX
Modos Con Datos =
VAR v = [Validaciones] + [Subidas] + [Legs]
RETURN IF(v > 0, 1, 0)
```

## 9) Nota de uso recomendado

Para visuales finales del mapa:
- Tamano: `Subidas Mapa (Hora Unica)` (si quieres hora exacta)
- Leyenda: `mode_desc` o `Indice Presion Categoria`
- Tooltips: `Subidas`, `Indice Presion Operativa`, `Espera Promedio Min`, `Espera P90 Min`

Para evitar errores de contexto:
- Mantener slicer de `hour` en seleccion unica cuando el objetivo sea analisis por hora.
- Si usas rango de horas, preferir `Subidas Promedio por Hora (Rango)`.

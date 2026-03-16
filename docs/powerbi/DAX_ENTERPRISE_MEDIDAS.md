# DAX Enterprise Pack - Transporte Publico (Power BI)

Este pack esta pensado para tu modelo constelacion con estas tablas:
- Facts: `fct_trip`, `fct_trip_leg`, `fct_validation`, `fct_boardings_30m`
- Dims: `dim_date`, `dim_time_30m`, `dim_stop`, `dim_mode`, `dim_service`, `dim_operator`, `dim_fare_period`, `dim_purpose`

Recomendacion de implementacion:
1. Crear tabla de medidas `_Medidas`.
2. Pegar en el orden indicado (base -> revenue -> operacion -> intermodal -> tiempo -> debug).
3. Formatear porcentajes y enteros al final.

---

## 0) Slicer global de tipo de dia (tabla desconectada)

> Usa esta tabla para evitar conflictos entre facts con grano distinto.

```DAX
DimTipoDia =
DATATABLE(
    "tipo_dia", STRING,
    {
        {"LABORAL"},
        {"SABADO"},
        {"DOMINGO"}
    }
)
```

---

## 1) Medidas base (volumen)

```DAX
Viajes Total = COUNTROWS(fct_trip)
```

```DAX
Legs Total = COUNTROWS(fct_trip_leg)
```

```DAX
Validaciones Total = COUNTROWS(fct_validation)
```

```DAX
Subidas Total = SUM(fct_boardings_30m[subidas_promedio])
```

```DAX
Tarjetas Unicas = DISTINCTCOUNT(fct_trip[id_tarjeta])
```

```DAX
Pasajeros Expandidos = SUM(fct_trip[factor_expansion])
```

```DAX
Tiempo Viaje Promedio Min = AVERAGE(fct_trip[tviaje_min])
```

```DAX
Etapas Promedio = AVERAGE(fct_trip[n_etapas])
```

```DAX
% Con Transbordo =
DIVIDE(
    COUNTROWS(FILTER(fct_trip, fct_trip[n_etapas] > 1)),
    [Viajes Total]
)
```

---

## 2) Filtros por tipo de dia (aplicables por fact)

```DAX
Viajes (TipoDia) =
CALCULATE(
    [Viajes Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_trip[tipo_dia])
)
```

```DAX
Legs (TipoDia) =
CALCULATE(
    [Legs Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_trip_leg[tipo_dia])
)
```

```DAX
Validaciones (TipoDia) =
CALCULATE(
    [Validaciones Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_validation[tipo_dia])
)
```

```DAX
Subidas (TipoDia) =
CALCULATE(
    [Subidas Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_boardings_30m[tipo_dia])
)
```

---

## 3) Revenue Assurance

```DAX
Pasajeros Estimados Transportados =
VAR _boardings = [Subidas (TipoDia)]
VAR _legs = [Legs (TipoDia)]
RETURN
IF(NOT ISBLANK(_boardings) && _boardings > 0, _boardings, _legs)
```

```DAX
Brecha No Validada =
VAR _transportados = [Pasajeros Estimados Transportados]
VAR _validados = [Validaciones (TipoDia)]
RETURN
MAX(_transportados - _validados, 0)
```

```DAX
Tasa Evasion Estimada % =
DIVIDE([Brecha No Validada], [Pasajeros Estimados Transportados])
```

```DAX
Ratio Validacion vs Demanda % =
DIVIDE([Validaciones (TipoDia)], [Pasajeros Estimados Transportados])
```

```DAX
Brecha No Validada YTD =
TOTALYTD([Brecha No Validada], dim_date[full_date])
```

```DAX
Validaciones YTD =
TOTALYTD([Validaciones Total], dim_date[full_date])
```

```DAX
Variacion YTD Validaciones % =
VAR _ytd = [Validaciones YTD]
VAR _ytd_ly =
    CALCULATE(
        [Validaciones YTD],
        DATEADD(dim_date[full_date], -1, YEAR)
    )
RETURN
DIVIDE(_ytd - _ytd_ly, _ytd_ly)
```

---

## 4) Eficiencia Operacional

```DAX
Espera Promedio Min = AVERAGE(fct_validation[t_espera_media_min])
```

```DAX
Espera P50 Min =
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
% Bajada Registrada =
DIVIDE(
    CALCULATE(COUNTROWS(fct_validation), fct_validation[tiene_bajada] = 1),
    [Validaciones Total]
)
```

```DAX
Indice Saturacion Horaria =
VAR _actual = [Pasajeros Estimados Transportados]
VAR _base_horas =
    ADDCOLUMNS(
        ALL(dim_time_30m[time_30m_sk]),
        "__demanda", CALCULATE([Pasajeros Estimados Transportados])
    )
VAR _p95 = PERCENTILEX.INC(_base_horas, [__demanda], 0.95)
RETURN
DIVIDE(_actual, _p95)
```

```DAX
Brecha Espera Laboral vs Domingo =
VAR _lab =
    CALCULATE(
        [Espera Promedio Min],
        TREATAS({"LABORAL"}, fct_validation[tipo_dia])
    )
VAR _dom =
    CALCULATE(
        [Espera Promedio Min],
        TREATAS({"DOMINGO"}, fct_validation[tipo_dia])
    )
RETURN
_lab - _dom
```

---

## 5) Mapa de Presion Operativa

```DAX
Subidas Mapa (Hora Unica) =
IF(
    HASONEVALUE(dim_time_30m[hour]),
    [Subidas (TipoDia)],
    BLANK()
)
```

```DAX
Subidas Promedio por Hora (Rango) =
DIVIDE(
    [Subidas (TipoDia)],
    DISTINCTCOUNT(dim_time_30m[hour])
)
```

```DAX
Subidas Max Mapa =
VAR TStops =
    ADDCOLUMNS(
        ALLSELECTED(dim_stop[stop_sk]),
        "@SubidasStop", CALCULATE([Subidas (TipoDia)], REMOVEFILTERS(dim_stop[stop_sk]))
    )
RETURN
MAXX(TStops, [@SubidasStop])
```

```DAX
Subidas Normalizado = DIVIDE([Subidas (TipoDia)], [Subidas Max Mapa])
```

```DAX
Espera Max Mapa =
VAR TStops =
    ADDCOLUMNS(
        ALLSELECTED(dim_stop[stop_sk]),
        "@EsperaStop", CALCULATE([Espera Promedio Min], REMOVEFILTERS(dim_stop[stop_sk]))
    )
RETURN
MAXX(TStops, [@EsperaStop])
```

```DAX
Espera Normalizado = DIVIDE([Espera Promedio Min], [Espera Max Mapa])
```

```DAX
Indice Presion Operativa =
ROUND(100 * (0.70 * [Subidas Normalizado] + 0.30 * [Espera Normalizado]), 1)
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

---

## 6) Intermodalidad

```DAX
Trips Unicos (Leg) = DISTINCTCOUNT(fct_trip_leg[trip_sk])
```

```DAX
Trips Intermodales =
SUMX(
    VALUES(fct_trip_leg[trip_sk]),
    VAR _modos = CALCULATE(DISTINCTCOUNT(fct_trip_leg[mode_sk]))
    RETURN IF(_modos >= 2, 1, 0)
)
```

```DAX
Tasa Intermodalidad % = DIVIDE([Trips Intermodales], [Trips Unicos (Leg)])
```

```DAX
Transfer Promedio Min = AVERAGE(fct_trip_leg[tc_transfer_min])
```

```DAX
Legs Promedio por Viaje = DIVIDE([Legs Total], [Trips Unicos (Leg)])
```

---

## 7) Medidas Top N (cuellos de botella)

```DAX
Rank Paradero Presion =
RANKX(
    ALLSELECTED(dim_stop[stop_code]),
    [Indice Presion Operativa],
    ,
    DESC,
    Dense
)
```

```DAX
Mostrar Top 20 Paraderos = IF([Rank Paradero Presion] <= 20, 1, 0)
```

```DAX
Paradero Critico #1 =
VAR _top1 =
    TOPN(
        1,
        ADDCOLUMNS(VALUES(dim_stop[stop_code]), "@idx", [Indice Presion Operativa]),
        [@idx], DESC
    )
RETURN
CONCATENATEX(_top1, dim_stop[stop_code], ", ")
```

---

## 8) Titulo dinamico y storytelling

```DAX
Titulo Mapa Operativo =
VAR hMin = MIN(dim_time_30m[hour])
VAR hMax = MAX(dim_time_30m[hour])
VAR modo = SELECTEDVALUE(dim_mode[mode_code], "Todos los modos")
VAR td = SELECTEDVALUE(DimTipoDia[tipo_dia], "Todos los tipos de dia")
RETURN
"Presion operativa por paradero | Hora: "
& FORMAT(hMin, "00") & ":00"
& IF(hMin <> hMax, " - " & FORMAT(hMax, "00") & ":59", "")
& " | Modo: " & modo
& " | Tipo dia: " & td
```

```DAX
Mensaje Ejecutivo =
VAR _ev = [Tasa Evasion Estimada %]
VAR _sat = [Indice Saturacion Horaria]
RETURN
IF(
    _ev >= 0.20 && _sat >= 1,
    "Riesgo alto: alta evasion y saturacion en el contexto filtrado.",
    IF(
        _ev >= 0.20,
        "Riesgo de recaudacion: evasion elevada.",
        IF(
            _sat >= 1,
            "Riesgo operacional: saturacion sobre P95.",
            "Operacion en rango controlado."
        )
    )
)
```

---

## 9) Debug / QA del modelo

```DAX
Debug Stops en Contexto = COUNTROWS(ALLSELECTED(dim_stop[stop_sk]))
```

```DAX
Debug Horas en Contexto = DISTINCTCOUNT(dim_time_30m[hour])
```

```DAX
Debug Modo en Contexto = DISTINCTCOUNT(dim_mode[mode_code])
```

```DAX
Debug Tiene Datos =
VAR v = [Viajes Total] + [Validaciones Total] + [Subidas Total] + [Legs Total]
RETURN IF(v > 0, 1, 0)
```

---

## 10) Formato recomendado

- Porcentajes:
  - `Tasa Evasion Estimada %`
  - `Ratio Validacion vs Demanda %`
  - `Variacion YTD Validaciones %`
  - `Tasa Intermodalidad %`
- 1 decimal:
  - `Indice Presion Operativa`
  - `Indice Saturacion Horaria`
- Entero con separador de miles:
  - `Viajes Total`, `Validaciones Total`, `Subidas Total`, `Legs Total`

---

## 11) Orden de implementacion (rapido)

1. Bloque 1 (base)
2. Bloque 2 (tipo de dia)
3. Bloque 3 (revenue)
4. Bloque 4 y 5 (operacion + mapa)
5. Bloque 6 (intermodal)
6. Bloques 7-9 (topN, narrativa y debug)


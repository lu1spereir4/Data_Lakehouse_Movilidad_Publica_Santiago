# DAX Enterprise Medidas - Reporte Ejecutivo y Estrategico

Catalogo unico de formulas DAX en uso para:

1. Pagina 1 - Resumen Ejecutivo.
2. Pagina 2 - Oportunidades Estrategicas.
3. KPI de Cobertura Recaudacion Comparable.

## Alcance funcional

Este documento reemplaza versiones duplicadas y consolida solo formulas vigentes para produccion.

## 1) Tabla desconectada de tipo de dia

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

## 2) Medidas base de volumen

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

## 3) Filtros por tipo de dia

```DAX
Validaciones (TipoDia) =
CALCULATE(
    [Validaciones Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), fct_validation[tipo_dia])
)
```

```DAX
Legs (TipoDia) =
CALCULATE(
    [Legs Total],
    TREATAS(VALUES(DimTipoDia[tipo_dia]), dim_date[tipo_dia])
)
```

## 4) Base compatible mensual para boardings

```DAX
Subidas (TipoDia) (Fecha Compatible) =
VAR MesesSeleccionadosSK =
    DISTINCT(
        SELECTCOLUMNS(
            ALLSELECTED(dim_date[year], dim_date[month]),
            "month_date_sk", dim_date[year] * 10000 + dim_date[month] * 100 + 1
        )
    )
RETURN
CALCULATE(
    SUM(fct_boardings_30m[subidas_promedio]),
    REMOVEFILTERS(dim_date),
    TREATAS(MesesSeleccionadosSK, fct_boardings_30m[month_date_sk]),
    FILTER(
        ALL(fct_boardings_30m[tipo_dia]),
        NOT ISFILTERED(DimTipoDia[tipo_dia])
            || fct_boardings_30m[tipo_dia] IN VALUES(DimTipoDia[tipo_dia])
    )
)
```

```DAX
Dias TipoDia Seleccionados =
CALCULATE(
    DISTINCTCOUNT(dim_date[full_date]),
    FILTER(
        ALL(dim_date[tipo_dia]),
        NOT ISFILTERED(DimTipoDia[tipo_dia])
            || dim_date[tipo_dia] IN VALUES(DimTipoDia[tipo_dia])
    )
)
```

## 5) Universo comparable de recaudacion

```DAX
Validaciones (TipoDia) Comparable =
CALCULATE(
    [Validaciones (TipoDia)],
    KEEPFILTERS(dim_mode[mode_code] IN {"BUS", "METRO"})
)
```

```DAX
Subidas (TipoDia) (Fecha Compatible) Comparable =
CALCULATE(
    [Subidas (TipoDia) (Fecha Compatible)],
    KEEPFILTERS(dim_mode[mode_code] IN {"BUS", "METRO"})
)
```

```DAX
Subidas Escaladas TipoDia Comparable (Ponderada TD) =
VAR MesesSeleccionadosSK =
    DISTINCT(
        SELECTCOLUMNS(
            ALLSELECTED(dim_date[year], dim_date[month]),
            "month_date_sk", dim_date[year] * 10000 + dim_date[month] * 100 + 1
        )
    )
VAR TiposDiaEnUso =
    SELECTCOLUMNS(
        FILTER(
            VALUES(dim_date[tipo_dia]),
            NOT ISFILTERED(DimTipoDia[tipo_dia])
                || dim_date[tipo_dia] IN VALUES(DimTipoDia[tipo_dia])
        ),
        "tipo_dia", dim_date[tipo_dia]
    )
RETURN
SUMX(
    TiposDiaEnUso,
    VAR _td = [tipo_dia]
    VAR _subidas_td =
        CALCULATE(
            SUM(fct_boardings_30m[subidas_promedio]),
            REMOVEFILTERS(dim_date),
            KEEPFILTERS(dim_mode[mode_code] IN {"BUS", "METRO"}),
            TREATAS(MesesSeleccionadosSK, fct_boardings_30m[month_date_sk]),
            FILTER(
                ALL(fct_boardings_30m[tipo_dia]),
                fct_boardings_30m[tipo_dia] = _td
            )
        )
    VAR _dias_td =
        CALCULATE(
            DISTINCTCOUNT(dim_date[full_date]),
            FILTER(
                ALL(dim_date[tipo_dia]),
                dim_date[tipo_dia] = _td
            )
        )
    RETURN COALESCE(_subidas_td, 0) * COALESCE(_dias_td, 0)
)
```

## 6) KPI principal de recaudacion comparable

```DAX
Pasajeros Estimados Recaudacion Comparable =
VAR s = [Subidas Escaladas TipoDia Comparable (Ponderada TD)]
RETURN IF(NOT ISBLANK(s) && s > 0, s, BLANK())
```

```DAX
Cobertura Recaudacion Comparable % =
DIVIDE([Validaciones (TipoDia) Comparable], [Pasajeros Estimados Recaudacion Comparable])
```

```DAX
Brecha Signed Recaudacion Comparable =
[Pasajeros Estimados Recaudacion Comparable] - [Validaciones (TipoDia) Comparable]
```

```DAX
Brecha No Validada Recaudacion Comparable =
MAX([Brecha Signed Recaudacion Comparable], 0)
```

```DAX
Sobrevalidacion Recaudacion Comparable =
MAX(-[Brecha Signed Recaudacion Comparable], 0)
```

## 7) Comparabilidad y calidad

```DAX
Modos Comparables Seleccionados =
VAR _comp = DATATABLE("mode_code", STRING, {{"BUS"}, {"METRO"}})
VAR _sel = VALUES(dim_mode[mode_code])
RETURN
IF(
    ISFILTERED(dim_mode[mode_code]),
    COUNTROWS(INTERSECT(_comp, _sel)),
    COUNTROWS(_comp)
)
```

```DAX
Modos No Comparables en Filtro =
COUNTROWS(
    FILTER(
        VALUES(dim_mode[mode_code]),
        NOT (dim_mode[mode_code] IN {"BUS", "METRO"})
    )
)
```

```DAX
Estado Comparabilidad Recaudacion =
VAR p = [Pasajeros Estimados Recaudacion Comparable]
VAR c = [Cobertura Recaudacion Comparable %]
VAR nc = [Modos No Comparables en Filtro]
VAR mc = [Modos Comparables Seleccionados]
RETURN
SWITCH(
    TRUE(),
    mc = 0, "No comparable (sin modos comparables)",
    ISBLANK(p) || p <= 0, "No comparable (sin base)",
    ISBLANK(c), "No comparable",
    nc > 0, "Comparable (solo BUS/METRO; hay modos no comparables en filtro)",
    c < 0.30, "No comparable (base sobredimensionada)",
    c > 1.50, "No comparable (base subdimensionada)",
    "Comparable"
)
```

```DAX
Bandera Calidad Horaria =
IF([Pasajeros Estimados Recaudacion Comparable] < 500, 0, 1)
```

```DAX
Cobertura Recaudacion Comparable % (Calidad Horaria) =
IF([Bandera Calidad Horaria] = 1, [Cobertura Recaudacion Comparable %], BLANK())
```

## 8) Medidas de oportunidad estrategica

```DAX
Recuperacion Potencial 10 % =
0.10 * [Brecha No Validada Recaudacion Comparable]
```

```DAX
Recuperacion Potencial 20 % =
0.20 * [Brecha No Validada Recaudacion Comparable]
```

```DAX
Brecha Relativa % =
DIVIDE(
    [Brecha No Validada Recaudacion Comparable],
    [Pasajeros Estimados Recaudacion Comparable]
)
```

```DAX
Impacto Prioridad Score =
VAR wBrecha = 0.50
VAR wVolumen = 0.30
VAR wCobertura = 0.20
VAR brechaNorm =
    DIVIDE(
        [Brecha No Validada Recaudacion Comparable],
        CALCULATE(
            MAXX(
                ALLSELECTED(dim_stop[stop_code]),
                [Brecha No Validada Recaudacion Comparable]
            )
        )
    )
VAR volumenNorm =
    DIVIDE(
        [Pasajeros Estimados Recaudacion Comparable],
        CALCULATE(
            MAXX(
                ALLSELECTED(dim_stop[stop_code]),
                [Pasajeros Estimados Recaudacion Comparable]
            )
        )
    )
VAR coberturaGap = 1 - [Cobertura Recaudacion Comparable %]
RETURN
100 * (
    wBrecha * COALESCE(brechaNorm, 0)
    + wVolumen * COALESCE(volumenNorm, 0)
    + wCobertura * COALESCE(coberturaGap, 0)
)
```

```DAX
Accion Recomendada =
VAR c = [Cobertura Recaudacion Comparable %]
VAR b = [Brecha No Validada Recaudacion Comparable]
RETURN
SWITCH(
    TRUE(),
    ISBLANK(c) || ISBLANK(b), "Revisar calidad de base",
    c < 0.75 && b > 0, "Fiscalizacion focalizada",
    c >= 0.75 && c < 0.90 && b > 0, "Ajuste operativo y control",
    c >= 0.90 && b > 0, "Micro-optimizacion",
    "Monitoreo"
)
```

```DAX
Top 10 Foco =
VAR r =
    RANKX(
        ALLSELECTED(dim_stop[stop_code]),
        [Impacto Prioridad Score],
        ,
        DESC,
        DENSE
    )
RETURN IF(r <= 10, 1, 0)
```

```DAX
Meta Cobertura % = 0.95
```

```DAX
Gap vs Meta Cobertura % =
[Meta Cobertura %] - [Cobertura Recaudacion Comparable %]
```

## 9) Nota de operacion

1. No usar medidas legacy de cobertura y brecha en paginas ejecutivas.
2. Mantener visible Estado Comparabilidad Recaudacion en vistas gerenciales.
3. Validar periodicamente con SQL en mismo contexto de filtros.

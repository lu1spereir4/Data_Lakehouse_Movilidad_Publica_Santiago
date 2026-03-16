# Pagina 1 - Resumen Ejecutivo

## Proposito

Esta pagina entrega una lectura ejecutiva del estado de demanda, validacion y riesgo de recaudacion en un solo vistazo. Esta orientada a jefaturas operativas, gerencia y analistas BI que necesitan priorizar decisiones en minutos.

## Preguntas de negocio que responde

1. Cual es el volumen estimado de pasajeros en el contexto filtrado.
2. Cuantas validaciones efectivas se registran.
3. Cual es la brecha de recaudacion estimada.
4. En que horarios y zonas se concentra la presion operativa.

## Definicion funcional de la pagina

La pagina combina tres capas de lectura:

1. Estado ejecutivo: tarjetas KPI con volumen, validacion, cobertura y brecha.
2. Comportamiento temporal: curva por hora para detectar ventanas criticas.
3. Foco territorial: mapa y ranking de paraderos para accion operativa.

## Indicadores principales

1. Pasajeros Estimados Recaudacion Comparable.
2. Validaciones (TipoDia) Comparable.
3. Cobertura Recaudacion Comparable %.
4. Brecha No Validada Recaudacion Comparable.
5. Estado Comparabilidad Recaudacion.

Interpretacion recomendada:

1. Cobertura en rango 0.90 a 1.00 sugiere calibracion razonable en universo comparable.
2. Cobertura bajo 0.90 identifica oportunidades de intervencion prioritaria.
3. Cobertura sobre 1.00 requiere revisar calibracion local de la base estimada.

## Alcance de comparabilidad

La lectura ejecutiva de recaudacion se reporta sobre universo comparable BUS y METRO. Cuando el filtro incluye modos no comparables, la pagina mantiene transparencia mediante Estado Comparabilidad Recaudacion.

## Reglas de uso para gerencia

1. Interpretar la cobertura como indicador de validacion estimada, no como evasión observada directa.
2. Priorizar focos usando brecha absoluta y no solo porcentaje.
3. Confirmar comparabilidad antes de usar el KPI como insumo de decision.

## Criterios de calidad

La pagina se considera en estado productivo cuando:

1. No presenta blancos injustificados al cambiar tipo de dia o modo.
2. Mantiene consistencia entre tarjetas, curva horaria y ranking territorial.
3. Explicita estado comparable o no comparable en todo contexto relevante.

## Dependencias de documentacion

1. Modelo de metricas y formulas: ver DAX Enterprise Medidas.
2. Diseno de oportunidad y priorizacion: ver Pagina 2.
3. Fundamento tecnico de cobertura: ver Solucion Problema Cobertura Recaudacion.
- Causa: interaccion desactivada o ruta de filtro rota.
- Solucion: `Editar interacciones` + validar relacion activa.

4. Medida `Legs (TipoDia)` falla por columna inexistente.
- Solucion: filtrar via `dim_date[tipo_dia]` en lugar de `fct_trip_leg[tipo_dia]`.

---

## 9) Definicion de hecho principal de la pagina

Hecho principal sugerido para pagina 01:
- `fct_boardings_30m` para demanda temporal del sistema.

Hechos de soporte:
- `fct_validation` para pagos/validaciones.
- `fct_trip_leg` para proxy operacional cuando no hay boardings en cierto corte.

Regla:
- No mezclar granos en un mismo visual sin medida controlada.

---

## 10) Cierre de pagina (criterio de aceptacion)

La pagina 01 esta terminada si:
1. Tiene los 4 KPI funcionando.
2. Tiene linea horaria comparativa demanda vs validacion.
3. Tiene mapa con tooltips de negocio.
4. Tiene tabla top de priorizacion accionable.
5. Reacciona correctamente a `tipo_dia`, `hour`, `mode_code`, `fecha`.

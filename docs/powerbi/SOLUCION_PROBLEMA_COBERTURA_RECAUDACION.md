# Solucion Tecnica - Cobertura de Recaudacion Comparable

## Resumen ejecutivo

Se estabilizo el KPI de cobertura de recaudacion bajo una logica comparable y auditable. La solucion elimina sesgos por mezcla de granos, controla la comparabilidad por modo y evita lecturas engañosas en ventanas horarias de baja base.

## Diagnostico consolidado

El analisis SQL y la validacion en Power BI mostraron cuatro causas principales:

1. Mezcla de grano mensual y diario entre fuentes.
2. Contaminacion del KPI por modos sin base comparable.
3. Sensibilidad alta en horas de bajo denominador.
4. Dependencia de filtros de tipo de dia no homogenea entre hechos.

## Decision metodologica

Se adopto una metodologia de comparabilidad operativa:

1. Universo comparable de recaudacion: BUS y METRO.
2. Modos fuera de universo: declarados como no comparables para este KPI.
3. Base estimada escalada por tipo de dia y rango de fechas compatibles.
4. Control de calidad horaria para ocultar lecturas sin masa critica.

## Diseno final de metricas

El bloque oficial de recaudacion queda compuesto por:

1. Pasajeros Estimados Recaudacion Comparable.
2. Validaciones (TipoDia) Comparable.
3. Cobertura Recaudacion Comparable %.
4. Brecha Signed Recaudacion Comparable.
5. Brecha No Validada Recaudacion Comparable.
6. Sobrevalidacion Recaudacion Comparable.
7. Estado Comparabilidad Recaudacion.
8. Cobertura Recaudacion Comparable % (Calidad Horaria).

Todas las formulas oficiales se documentan en DAX Enterprise Medidas.

## Regla de interpretacion de negocio

1. El KPI representa cobertura de validacion estimada, no evasión observada directa.
2. Coberturas fuera de rango esperado deben revisarse con estado de comparabilidad.
3. La brecha se interpreta junto con volumen y contexto territorial para priorizacion.

## Criterios de aceptacion

La solucion se considera cerrada cuando:

1. Las paginas ejecutivas usan solo el bloque comparable.
2. Las medidas legacy quedan ocultas para consumo directivo.
3. SQL y Power BI convergen en los contextos de validacion.
4. Los visuales horarios usan la bandera de calidad.

## Gobernanza y trazabilidad

1. Cambios de logica deben versionarse en el repositorio de documentacion Power BI.
2. Todo ajuste de cobertura debe incluir validacion SQL reproducible.
3. El estado de comparabilidad debe permanecer visible en reportes de negocio.

# Consultas de Inteligencia — Movilidad Pública Santiago

Consultas SQL sobre el DW Kimball de DTPM. Cada archivo agrupa queries
por dominio de negocio. Todas corren directamente sobre las tablas `dw.*`.

## Organización

| Archivo | Dominio | Queries |
|---|---|---|
| `01_demanda_horaria.sql` | Patrones de demanda en el tiempo | Q1, Q6, Q14 |
| `02_analisis_od.sql` | Flujos origen-destino y eficiencia | Q3, Q10, Q15 |
| `03_infraestructura.sql` | Paraderos y equidad territorial | Q2, Q8 |
| `04_calidad_servicio.sql` | Niveles de servicio y cobertura | Q4, Q5, Q11, Q12, Q13 |
| `05_usuarios.sql` | Segmentación y comportamiento | Q7, Q9 |

## Esquema del DW

```
dw.fct_trip          — 1 fila = 1 viaje completo
dw.fct_trip_leg      — 1 fila = 1 etapa del viaje (leg)
dw.fct_validation    — 1 fila = 1 validación/embarque (etapas DTPM)
dw.fct_boardings_30m — 1 fila = promedio de subidas por paradero/franja/tipodía

dw.dim_date          — Calendario (YYYYMMDD)
dw.dim_time_30m      — 48 franjas de 30 min (0..47)
dw.dim_stop          — Paraderos SCD2 (stop_code, comuna, coords UTM)
dw.dim_service       — Servicios SCD2 (service_code, mode_code)
dw.dim_mode          — 5 modos: BUS, METRO, ZP, METROTREN, UNKNOWN
dw.dim_fare_period   — Períodos tarifarios (Punta, Valle, Nocturno…)
dw.dim_purpose       — Propósito del viaje (Trabajo, Estudio…)
dw.dim_cut           — Metadatos de cada partición Silver cargada
```

## Notas

- `factor_expansion` → expande la muestra al universo total de pasajeros.
  Usar `SUM(factor_expansion)` para cifras de política pública.
- `tipo_dia` → `'LABORAL'`, `'SABADO'`, `'DOMINGO'`.
- Los `*_sk` son surrogate keys; los `*_code` son las llaves de negocio.
- `dim_stop.stop_name` y `dim_service.service_name` están NULL en la versión
  actual (requieren el Maestro de Paraderos/Servicios DTPM como fuente adicional).

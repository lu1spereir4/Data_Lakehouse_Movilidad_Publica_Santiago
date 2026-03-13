-- =============================================================================
-- 04_calidad_servicio.sql
-- Dominio: Niveles de servicio, eficiencia operacional y cobertura de datos
--
-- Preguntas que responden:
--   ¿Los usuarios sufren muchos transbordos? ¿En qué horario es peor?
--   ¿Qué servicios son los más lentos? ¿Cuánto esperan los pasajeros?
--   ¿Con qué confiabilidad se registra el dato de bajada?
--
-- Uso típico: contratos de operación, KPIs de nivel de servicio,
--             auditoría de calidad de datos, benchmarking por operador.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q4: Segmentación por complejidad de viaje
-- -----------------------------------------------------------------------------
-- Cuántos viajes se hacen con 1, 2, 3 o 4 etapas y qué significa en tiempo.
-- Un sistema bien diseñado debería concentrar la mayoría de viajes en 1-2 etapas.
-- Etapas adicionales implican transbordos: tiempo y esfuerzo extra para el usuario.
-- La columna min_por_km combina ambas dimensiones en un KPI de eficiencia.
-- -----------------------------------------------------------------------------
SELECT
    ft.n_etapas                                                 AS num_etapas,
    COUNT(*)                                                    AS viajes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_total,
    ROUND(SUM(ft.factor_expansion), 0)                          AS demanda_expandida,
    ROUND(AVG(ft.tviaje_min), 1)                                AS tviaje_promedio_min,
    ROUND(AVG(ft.distancia_ruta_m / 1000.0), 2)                AS dist_ruta_promedio_km,
    ROUND(
        AVG(ft.tviaje_min)
        / NULLIF(AVG(ft.distancia_ruta_m / 1000.0), 0)
        , 2)                                                    AS min_por_km
FROM dw.fct_trip ft
WHERE ft.n_etapas IS NOT NULL
GROUP BY ft.n_etapas
ORDER BY ft.n_etapas;


-- -----------------------------------------------------------------------------
-- Q5: Velocidad comercial por servicio — ranking de servicios lentos
-- -----------------------------------------------------------------------------
-- Estima la velocidad de cada servicio de bus usando la distancia euclidiana
-- entre paradero de subida y bajada (proxy de la distancia recorrida).
-- Los servicios con más minutos por km son candidatos a rediseño de recorrido
-- o ajuste de frecuencia. Solo considera legs con datos de coordenadas válidos
-- y un mínimo de 1.000 registros para robustez estadística.
-- -----------------------------------------------------------------------------
SELECT TOP 20
    ds.service_code,
    dm.mode_code,
    COUNT(*)                                                    AS legs_registrados,
    ROUND(AVG(tl.tv_leg_min), 1)                                AS tiempo_promedio_min,
    ROUND(AVG(CAST(
        SQRT(POWER(CAST(sb.x_utm - sa.x_utm AS FLOAT), 2)
           + POWER(CAST(sb.y_utm - sa.y_utm AS FLOAT), 2))
        AS FLOAT) / 1000.0), 2)                                 AS dist_eucl_km_estimada,
    ROUND(
        AVG(tl.tv_leg_min)
        / NULLIF(AVG(
            SQRT(POWER(CAST(sb.x_utm - sa.x_utm AS FLOAT), 2)
               + POWER(CAST(sb.y_utm - sa.y_utm AS FLOAT), 2)) / 1000.0
        ), 0)
        , 2)                                                    AS min_por_km
FROM dw.fct_trip_leg tl
JOIN dw.dim_service ds ON ds.service_sk = tl.service_sk
JOIN dw.dim_mode    dm ON dm.mode_sk    = tl.mode_sk
JOIN dw.dim_stop    sb ON sb.stop_sk    = tl.board_stop_sk
JOIN dw.dim_stop    sa ON sa.stop_sk    = tl.alight_stop_sk
WHERE tl.tv_leg_min  > 0
  AND sb.x_utm IS NOT NULL AND sa.x_utm IS NOT NULL
  AND dm.mode_code = 'BUS'
GROUP BY ds.service_code, dm.mode_code
HAVING COUNT(*) > 1000          -- mínimo estadístico para evitar valores extremos
ORDER BY min_por_km DESC;


-- -----------------------------------------------------------------------------
-- Q11: Propensión al transbordo por período tarifario
-- -----------------------------------------------------------------------------
-- ¿En hora punta los pasajeros hacen más transbordos porque el bus viene lleno?
-- Una correlación entre "período punta" y "más etapas por viaje" sugeriría
-- que el sistema expulsa usuarios al bus desde el Metro saturado.
-- Lectura clave para tarificación dinámica y gestión de carga.
-- -----------------------------------------------------------------------------
SELECT
    fp.fare_period_name                                         AS periodo_tarifario,
    COUNT(*)                                                    AS viajes,
    ROUND(AVG(CAST(ft.n_etapas AS FLOAT)), 3)                   AS etapas_promedio,
    ROUND(AVG(ft.tviaje_min), 1)                                AS tviaje_promedio_min,
    ROUND(AVG(ft.distancia_ruta_m / 1000.0), 2)                AS dist_promedio_km,
    ROUND(
        100.0 * SUM(CASE WHEN ft.n_etapas > 1 THEN 1 ELSE 0 END)
              / COUNT(*)
        , 2)                                                    AS pct_con_transbordo
FROM dw.fct_trip ft
JOIN dw.dim_fare_period fp ON fp.fare_period_sk = ft.fare_period_start_sk
WHERE fp.fare_period_name IS NOT NULL
GROUP BY fp.fare_period_name
ORDER BY pct_con_transbordo DESC;


-- -----------------------------------------------------------------------------
-- Q12: Tiempo de espera por modo — benchmark de nivel de servicio
-- -----------------------------------------------------------------------------
-- El tiempo de espera en paradero (tEsperaMediaIntervalo) es el KPI de
-- nivel de servicio más directo para el pasajero. Se calcula promedio,
-- mediana y percentil 90 por modo, y se compara laboral vs domingo.
-- Diferencias grandes entre laboral y domingo indican frecuencias muy bajas
-- en fin de semana — un argumento directo para aumentar oferta dominical.
-- Los percentiles requieren SQL Server 2012+.
-- -----------------------------------------------------------------------------
SELECT
    dm.mode_code,
    COUNT(*)                                                    AS validaciones,
    ROUND(AVG(fv.t_espera_media_min), 2)                        AS espera_promedio_min,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY fv.t_espera_media_min)
          OVER (PARTITION BY dm.mode_code), 2)                  AS espera_mediana_min,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
          (ORDER BY fv.t_espera_media_min)
          OVER (PARTITION BY dm.mode_code), 2)                  AS espera_p90_min,
    ROUND(AVG(CASE WHEN fv.tipo_dia = 'LABORAL'
                   THEN fv.t_espera_media_min END), 2)          AS espera_laboral,
    ROUND(AVG(CASE WHEN fv.tipo_dia = 'DOMINGO'
                   THEN fv.t_espera_media_min END), 2)          AS espera_domingo
FROM dw.fct_validation fv
JOIN dw.dim_mode dm ON dm.mode_sk = fv.mode_sk
WHERE fv.t_espera_media_min IS NOT NULL
  AND fv.t_espera_media_min BETWEEN 0 AND 120     -- excluir outliers (>2h son errores)
GROUP BY dm.mode_code
ORDER BY espera_promedio_min DESC;


-- -----------------------------------------------------------------------------
-- Q13: Cobertura del dato de bajada — auditoría por contrato
-- -----------------------------------------------------------------------------
-- El DTPM exige que los operadores registren el paradero de bajada para
-- calcular la tarifa y el OD. Si tiene_bajada=0, no hay OD para ese viaje.
-- Esta query mide el cumplimiento por modo y contrato: contratos con <90%
-- de bajada registrada pueden estar incumpliendo el contrato de concesión.
-- Es el tipo de análisis que un analista presenta al regulador.
-- -----------------------------------------------------------------------------
SELECT
    dm.mode_code,
    oc.contract_code,
    COUNT(*)                                                    AS total_validaciones,
    SUM(CAST(fv.tiene_bajada AS INT))                           AS con_bajada,
    COUNT(*) - SUM(CAST(fv.tiene_bajada AS INT))                AS sin_bajada,
    ROUND(
        100.0 * SUM(CAST(fv.tiene_bajada AS INT)) / COUNT(*)
        , 2)                                                    AS pct_con_bajada,
    ROUND(AVG(CASE WHEN fv.tiene_bajada = 1
                   THEN fv.tiempo_etapa_sec / 60.0 END), 1)    AS tiempo_etapa_promedio_min
FROM dw.fct_validation fv
JOIN dw.dim_mode dm ON dm.mode_sk = fv.mode_sk
LEFT JOIN dw.dim_operator_contract oc ON oc.operator_sk = fv.operator_sk
WHERE fv.tiene_bajada IS NOT NULL
GROUP BY dm.mode_code, oc.contract_code
ORDER BY pct_con_bajada;

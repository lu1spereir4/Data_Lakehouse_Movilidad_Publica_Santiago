-- =============================================================================
-- 02_analisis_od.sql
-- Dominio: Flujos Origen-Destino, eficiencia de recorridos, propósito de viaje
--
-- Preguntas que responden:
--   ¿Entre qué zonas se producen los mayores flujos de demanda?
--   ¿Cuán directas son las rutas que ofrece el sistema?
--   ¿Quién usa el transporte y para qué?
--
-- Uso típico: planificación de nuevas líneas, extensiones de servicios,
--             segmentación de política tarifaria.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q3: Matriz Origen-Destino — corredores con mayor demanda
-- -----------------------------------------------------------------------------
-- Agrega viajes por par de zonas (origin→dest), expandidos al universo real.
-- Las zonas son la unidad de planificación del DTPM (zona_inicio/fin_viaje).
-- Los corredores con alta demanda y tiempo elevado son candidatos directos
-- a nuevas líneas o extensiones.
-- Excluye viajes intrazonales (mismo origen y destino) para enfocarse en
-- flujos de desplazamiento real entre sectores.
-- -----------------------------------------------------------------------------
SELECT TOP 30
    ft.zone_origin_txt                                          AS zona_origen,
    ft.zone_dest_txt                                            AS zona_destino,
    COUNT(*)                                                    AS viajes_totales,
    ROUND(SUM(ft.factor_expansion), 0)                          AS demanda_expandida,
    ROUND(AVG(ft.tviaje_min), 1)                                AS tiempo_promedio_min,
    ROUND(AVG(ft.distancia_ruta_m) / 1000.0, 2)                AS dist_ruta_km,
    ROUND(AVG(CAST(ft.n_etapas AS FLOAT)), 2)                   AS etapas_promedio
FROM dw.fct_trip ft
WHERE ft.zone_origin_txt IS NOT NULL
  AND ft.zone_dest_txt   IS NOT NULL
  AND ft.zone_origin_txt <> ft.zone_dest_txt     -- excluir viajes intrazonales
GROUP BY ft.zone_origin_txt, ft.zone_dest_txt
ORDER BY demanda_expandida DESC;


-- -----------------------------------------------------------------------------
-- Q10: Factor de desvío — eficiencia geométrica del recorrido
-- -----------------------------------------------------------------------------
-- Compara la distancia real de ruta con la distancia en línea recta (euclidiana).
-- Un factor >2.0 significa que el bus recorre el doble de la distancia ideal:
-- puede indicar una ruta ineficiente, necesidad de variante, o un problema
-- de calidad de datos.
-- Factor típico aceptable en transporte urbano: 1.2x–1.5x.
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 1.2 THEN 'Muy directo (<1.2x)'
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 1.5 THEN 'Directo (1.2–1.5x)'
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 2.0 THEN 'Moderado (1.5–2x)'
        ELSE                                                                    'Alto desvío (>2x)'
    END                                                         AS categoria_desvio,
    COUNT(*)                                                    AS viajes,
    ROUND(AVG(ft.tviaje_min), 1)                                AS tiempo_promedio_min,
    ROUND(AVG(ft.distancia_ruta_m) / 1000.0, 2)                AS dist_ruta_km,
    ROUND(AVG(ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0)), 3) AS factor_desvio_promedio
FROM dw.fct_trip ft
WHERE ft.distancia_eucl_m  > 500        -- al menos 500 m para evitar ruido estadístico
  AND ft.distancia_ruta_m IS NOT NULL
GROUP BY
    CASE
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 1.2 THEN 'Muy directo (<1.2x)'
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 1.5 THEN 'Directo (1.2–1.5x)'
        WHEN ft.distancia_ruta_m / NULLIF(ft.distancia_eucl_m, 0) < 2.0 THEN 'Moderado (1.5–2x)'
        ELSE                                                                    'Alto desvío (>2x)'
    END
ORDER BY factor_desvio_promedio;


-- -----------------------------------------------------------------------------
-- Q15: Eficiencia por propósito — ¿quién aprovecha mejor el sistema?
-- -----------------------------------------------------------------------------
-- Compara los viajes al Trabajo, Estudio, Ocio, etc. en términos de
-- tiempo, distancia y velocidad comercial "puerta a puerta".
-- Responde: ¿los trabajadores sufren más que los estudiantes?
-- Permite priorizar qué segmento se beneficiaría más de una mejora concreta.
-- La columna min_por_km a nivel de propósito es un KPI de equidad de servicio.
-- -----------------------------------------------------------------------------
SELECT
    dp.purpose_name                                             AS proposito,
    COUNT(*)                                                    AS viajes,
    ROUND(SUM(ft.factor_expansion), 0)                          AS demanda_expandida,
    ROUND(AVG(ft.tviaje_min), 1)                                AS tviaje_promedio_min,
    ROUND(AVG(ft.distancia_ruta_m) / 1000.0, 2)                AS dist_ruta_promedio_km,
    ROUND(AVG(CAST(ft.n_etapas AS FLOAT)), 3)                   AS etapas_promedio,
    -- Velocidad comercial total del viaje puerta a puerta (km/h)
    ROUND(
        AVG(ft.distancia_ruta_m / 1000.0)
        / NULLIF(AVG(ft.tviaje_min) / 60.0, 0)
        , 1)                                                    AS velocidad_puerta_kmh,
    ROUND(100.0 * SUM(CASE WHEN ft.tipo_dia = 'LABORAL' THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                  AS pct_laboral,
    ROUND(100.0 * SUM(CASE WHEN ft.tipo_dia = 'DOMINGO' THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                  AS pct_domingo
FROM dw.fct_trip ft
JOIN dw.dim_purpose dp ON dp.purpose_sk = ft.purpose_sk
GROUP BY dp.purpose_name
ORDER BY demanda_expandida DESC;

-- =============================================================================
-- 01_demanda_horaria.sql
-- Dominio: Patrones de demanda a lo largo del tiempo
--
-- Preguntas que responden:
--   ¿Cuándo viaja Santiago? ¿Cuándo el sistema está bajo máxima presión?
--   ¿Cómo evolucionó la demanda día a día en la semana?
--
-- Uso típico: definir horarios de refuerzo de flota, detectar anomalías,
--             construir series temporales para forecasting.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1: Curva de demanda diaria — comparativa laboral vs fin de semana
-- -----------------------------------------------------------------------------
-- Muestra cuántas subidas ocurren en cada franja de 30 minutos según tipo de día.
-- La columna pct_laboral_vs_domingo cuantifica cuánto más se usa el sistema
-- un día laboral respecto al domingo — base para justificar frecuencias distintas.
-- -----------------------------------------------------------------------------
SELECT
    t.label                                                    AS franja_horaria,
    t.hour,
    SUM(CASE WHEN f.tipo_dia = 'LABORAL' THEN f.subidas_promedio ELSE 0 END) AS subidas_laboral,
    SUM(CASE WHEN f.tipo_dia = 'SABADO'  THEN f.subidas_promedio ELSE 0 END) AS subidas_sabado,
    SUM(CASE WHEN f.tipo_dia = 'DOMINGO' THEN f.subidas_promedio ELSE 0 END) AS subidas_domingo,
    ROUND(
        100.0 * SUM(CASE WHEN f.tipo_dia = 'LABORAL' THEN f.subidas_promedio ELSE 0 END)
              / NULLIF(SUM(CASE WHEN f.tipo_dia = 'DOMINGO' THEN f.subidas_promedio ELSE 0 END), 0)
        - 100, 1
    )                                                          AS pct_laboral_vs_domingo
FROM dw.fct_boardings_30m f
JOIN dw.dim_time_30m t ON t.time_30m_sk = f.time_30m_sk
GROUP BY t.label, t.hour
ORDER BY t.hour;


-- -----------------------------------------------------------------------------
-- Q6: Hora punta real — pasajeros simultáneamente en tránsito
-- -----------------------------------------------------------------------------
-- A diferencia de "subidas", esta query mide cuántos pasajeros están viajando
-- activamente en cada franja (embarques en curso). Es la presión real sobre
-- la capacidad del sistema, no solo el flujo de entrada.
-- Métrica clave para dimensionar flota y gestión de congestión.
-- -----------------------------------------------------------------------------
WITH viajes_en_transito AS (
    SELECT
        tl.time_board_30m_sk,
        SUM(ft.factor_expansion)   AS pasajeros_en_transito
    FROM dw.fct_trip_leg tl
    JOIN dw.fct_trip     ft ON ft.trip_sk = tl.trip_sk
    JOIN dw.dim_mode     dm ON dm.mode_sk  = tl.mode_sk
    WHERE ft.tipo_dia = 'LABORAL'
    GROUP BY tl.time_board_30m_sk
)
SELECT
    t.label                                                    AS franja,
    ROUND(vt.pasajeros_en_transito, 0)                         AS pasajeros_en_transito,
    ROUND(
        100.0 * vt.pasajeros_en_transito
              / MAX(vt.pasajeros_en_transito) OVER ()
        , 1)                                                   AS pct_del_pico
FROM viajes_en_transito vt
JOIN dw.dim_time_30m t ON t.time_30m_sk = vt.time_board_30m_sk
ORDER BY t.time_30m_sk;


-- -----------------------------------------------------------------------------
-- Q14: Evolución diaria de la demanda — serie temporal semanal
-- -----------------------------------------------------------------------------
-- Muestra la demanda expandida, tiempo promedio de viaje y número de
-- tarjetas únicas por cada día del período cargado.
-- La columna delta_vs_dia_anterior (función LAG) detecta caídas bruscas
-- que pueden indicar eventos, feriados o problemas de servicio.
-- Base directa para modelos de forecasting y alertas automáticas.
-- -----------------------------------------------------------------------------
SELECT
    dd.full_date,
    dd.day_of_week,
    dd.tipo_dia,
    COUNT(DISTINCT ft.id_tarjeta)                              AS tarjetas_unicas,
    COUNT(*)                                                   AS viajes_muestra,
    ROUND(SUM(ft.factor_expansion), 0)                         AS demanda_expandida,
    ROUND(AVG(ft.tviaje_min), 1)                               AS tviaje_promedio_min,
    ROUND(AVG(CAST(ft.n_etapas AS FLOAT)), 3)                  AS etapas_promedio,
    ROUND(
        SUM(ft.factor_expansion)
        - LAG(SUM(ft.factor_expansion)) OVER (ORDER BY dd.full_date)
        , 0)                                                   AS delta_vs_dia_anterior
FROM dw.fct_trip ft
JOIN dw.dim_date dd ON dd.date_sk = ft.date_start_sk
GROUP BY dd.full_date, dd.day_of_week, dd.tipo_dia
ORDER BY dd.full_date;

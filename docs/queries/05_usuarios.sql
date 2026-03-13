-- =============================================================================
-- 05_usuarios.sql
-- Dominio: Segmentación de usuarios y análisis de intermodalidad
--
-- Preguntas que responden:
--   ¿Quiénes son los usuarios cautivos del sistema?
--   ¿Cuántos pasajeros combinan realmente Metro con Bus?
--   ¿Qué tiempo adicional supone usar el Metro respecto al bus directo?
--
-- Uso típico: segmentación de demanda, modelo de tarificación, subsidios,
--             evaluación de proyectos de infraestructura, estudios de movilidad.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q7: Usuarios cautivos — tarjetas con alta frecuencia de uso diario
-- -----------------------------------------------------------------------------
-- Un "usuario cautivo" es aquel que depende del transporte público sin alternativa.
-- La proxy usada aquí es el número de viajes por día laboral:
--   >=4 viajes sugiere uso en los 4 extremos del día (casa→trabajo→almuerzo→trabajo→casa)
--      o turno partido, es decir, usuario que no tiene auto ni puede pagar taxi.
--   >=6 viajes es extremadamente cautivo (trabajo por turnos o múltiples traslados).
-- Este segmento debe ser protegido en cualquier ajuste tarifario.
--
-- El resultado identifica la tarjeta más activa y el percentil de la distribución.
-- Para anonimizar se puede agregar sin id_tarjeta en la consulta final.
-- -----------------------------------------------------------------------------
WITH viajes_diarios AS (
    -- Contar viajes por tarjeta por día laboral
    SELECT
        ft.id_tarjeta,
        dd.date_sk,
        COUNT(*)                AS viajes_en_dia
    FROM dw.fct_trip ft
    JOIN dw.dim_date dd ON dd.date_sk = ft.date_start_sk
    WHERE ft.id_tarjeta IS NOT NULL
      AND dd.is_laboral = 1
    GROUP BY ft.id_tarjeta, dd.date_sk
),
media_por_tarjeta AS (
    -- Media de viajes-por-día-laboral para cada tarjeta
    SELECT
        id_tarjeta,
        ROUND(AVG(CAST(viajes_en_dia AS FLOAT)), 2)  AS viajes_x_dia_laboral,
        COUNT(DISTINCT date_sk)                       AS dias_con_actividad
    FROM viajes_diarios
    GROUP BY id_tarjeta
    HAVING COUNT(DISTINCT date_sk) >= 3   -- filtrar tarjetas con al menos 3 días activos
)
SELECT
    CASE
        WHEN viajes_x_dia_laboral >= 6 THEN '>=6 (extremo cautivo)'
        WHEN viajes_x_dia_laboral >= 4 THEN '4-5 (cautivo)'
        WHEN viajes_x_dia_laboral >= 2 THEN '2-3 (regular)'
        ELSE '1 (esporádico)'
    END                                                         AS segmento_uso,
    COUNT(*)                                                    AS tarjetas,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_tarjetas,
    ROUND(AVG(viajes_x_dia_laboral), 2)                         AS media_viajes_dia,
    ROUND(AVG(CAST(dias_con_actividad AS FLOAT)), 1)            AS dias_activos_promedio
FROM media_por_tarjeta
GROUP BY
    CASE
        WHEN viajes_x_dia_laboral >= 6 THEN '>=6 (extremo cautivo)'
        WHEN viajes_x_dia_laboral >= 4 THEN '4-5 (cautivo)'
        WHEN viajes_x_dia_laboral >= 2 THEN '2-3 (regular)'
        ELSE '1 (esporádico)'
    END
ORDER BY media_viajes_dia DESC;


-- Detalle de las 10 tarjetas más activas (útil para validar que no son errores)
-- NOTA: en producción reemplazar por un hash del id_tarjeta antes de exponer.
SELECT TOP 10
    m.id_tarjeta,
    m.viajes_x_dia_laboral,
    m.dias_con_actividad
FROM (
    SELECT
        id_tarjeta,
        ROUND(AVG(CAST(viajes_en_dia AS FLOAT)), 2) AS viajes_x_dia_laboral,
        COUNT(DISTINCT date_sk)                      AS dias_con_actividad
    FROM (
        SELECT ft.id_tarjeta, dd.date_sk, COUNT(*) AS viajes_en_dia
        FROM dw.fct_trip ft
        JOIN dw.dim_date dd ON dd.date_sk = ft.date_start_sk
        WHERE ft.id_tarjeta IS NOT NULL AND dd.is_laboral = 1
        GROUP BY ft.id_tarjeta, dd.date_sk
    ) sub
    GROUP BY id_tarjeta
) m
ORDER BY m.viajes_x_dia_laboral DESC;


-- -----------------------------------------------------------------------------
-- Q9: Intermodalidad real — combinaciones Metro + Bus
-- -----------------------------------------------------------------------------
-- La intermodalidad es el principal argumento de inversión en infraestructura:
-- conectar Metro con buses de acercamiento. Para medirla hay que observar qué
-- modos aparecen dentro de un mismo viaje (fct_trip → fct_trip_leg).
--
-- Se clasifica cada viaje según los modos usados en sus etapas:
--   "Metro + Bus"   → integración real entre red troncal y alimentación
--   "Solo Metro"    → usuario que vive y trabaja cerca de estaciones
--   "Solo Bus"      → sin acceso relevante a Metro
--   "Metro + Tren"  → integración larga distancia
--   "Otro"          → combinaciones mixtas (BIP raro, cargo, etc.)
--
-- El tiempo de transferencia (t_transbordo_min en fct_trip_leg leg_seq >1) es
-- clave para evaluar la experiencia real de integración.
-- -----------------------------------------------------------------------------
WITH modos_por_viaje AS (
    -- Para cada viaje obtener todos los modos distintos usados
    SELECT
        tl.trip_sk,
        MAX(CASE WHEN dm.mode_code = 'METRO'     THEN 1 ELSE 0 END) AS tiene_metro,
        MAX(CASE WHEN dm.mode_code = 'BUS'       THEN 1 ELSE 0 END) AS tiene_bus,
        MAX(CASE WHEN dm.mode_code = 'METROTREN' THEN 1 ELSE 0 END) AS tiene_tren,
        AVG(CASE WHEN tl.leg_seq > 1
                  AND tl.t_transbordo_min IS NOT NULL
                 THEN tl.t_transbordo_min END)                       AS espera_transbordo_prom_min,
        COUNT(DISTINCT tl.leg_seq)                                   AS n_etapas
    FROM dw.fct_trip_leg tl
    JOIN dw.dim_mode dm ON dm.mode_sk = tl.mode_sk
    GROUP BY tl.trip_sk
)
SELECT
    CASE
        WHEN tiene_metro = 1 AND tiene_bus  = 1 AND tiene_tren = 0 THEN 'Metro + Bus'
        WHEN tiene_metro = 1 AND tiene_bus  = 0 AND tiene_tren = 0 THEN 'Solo Metro'
        WHEN tiene_metro = 0 AND tiene_bus  = 1 AND tiene_tren = 0 THEN 'Solo Bus'
        WHEN tiene_metro = 1 AND tiene_tren = 1                     THEN 'Metro + Metrotren'
        WHEN tiene_metro = 0 AND tiene_tren = 1                     THEN 'Solo Metrotren'
        ELSE 'Combinación otra'
    END                                                             AS tipo_viaje,
    COUNT(*)                                                        AS viajes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)              AS pct_viajes,
    ROUND(AVG(CAST(n_etapas AS FLOAT)), 2)                          AS etapas_promedio,
    ROUND(AVG(espera_transbordo_prom_min), 2)                       AS espera_transbordo_prom_min,
    -- El tiempo de transbordo es la "penalización" de la intermodalidad:
    -- si supera los 10 min el diseño de la interfaz modal puede mejorarse.
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY espera_transbordo_prom_min) OVER (
          PARTITION BY
              CASE
                  WHEN tiene_metro=1 AND tiene_bus=1 AND tiene_tren=0 THEN 'Metro + Bus'
                  WHEN tiene_metro=1 AND tiene_bus=0 AND tiene_tren=0 THEN 'Solo Metro'
                  WHEN tiene_metro=0 AND tiene_bus=1 AND tiene_tren=0 THEN 'Solo Bus'
                  WHEN tiene_metro=1 AND tiene_tren=1                  THEN 'Metro + Metrotren'
                  WHEN tiene_metro=0 AND tiene_tren=1                  THEN 'Solo Metrotren'
                  ELSE 'Combinación otra'
              END
          ), 2)                                                     AS mediana_espera_transbordo_min
FROM modos_por_viaje
GROUP BY
    CASE
        WHEN tiene_metro = 1 AND tiene_bus  = 1 AND tiene_tren = 0 THEN 'Metro + Bus'
        WHEN tiene_metro = 1 AND tiene_bus  = 0 AND tiene_tren = 0 THEN 'Solo Metro'
        WHEN tiene_metro = 0 AND tiene_bus  = 1 AND tiene_tren = 0 THEN 'Solo Bus'
        WHEN tiene_metro = 1 AND tiene_tren = 1                     THEN 'Metro + Metrotren'
        WHEN tiene_metro = 0 AND tiene_tren = 1                     THEN 'Solo Metrotren'
        ELSE 'Combinación otra'
    END,
    tiene_metro, tiene_bus, tiene_tren           -- necesarios para window function
ORDER BY viajes DESC;

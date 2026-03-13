-- =============================================================================
-- 03_infraestructura.sql
-- Dominio: Paraderos críticos y equidad territorial
--
-- Preguntas que responden:
--   ¿Qué paraderos concentran más demanda y deben priorizarse?
--   ¿Qué comunas están sub-servidas respecto a su infraestructura?
--
-- Uso típico: argumentar inversiones en infraestructura, priorizar
--             paraderos para techado/ampliación, evidenciar brechas
--             territoriales ante autoridades.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q2: Top 20 paraderos más críticos del sistema
-- -----------------------------------------------------------------------------
-- Rankea los paraderos por promedio de subidas diarias en días laborales,
-- incluyendo una proyección anual simple (×365).
-- Las coordenadas UTM permiten llevar este resultado directamente a un mapa.
-- Utlidad: priorizar paraderos para mejoras físicas, cámaras de seguridad,
-- ampliación de refugios o puntos de carga de tarjeta Bip.
-- -----------------------------------------------------------------------------
SELECT TOP 20
    ds.stop_code,
    ds.comuna,
    ds.zone_code,
    dm.mode_code,
    ROUND(SUM(f.subidas_promedio), 0)               AS subidas_promedio_dia,
    ROUND(SUM(f.subidas_promedio) * 365, 0)         AS proyeccion_anual,
    ds.x_utm,
    ds.y_utm
FROM dw.fct_boardings_30m f
JOIN dw.dim_stop  ds ON ds.stop_sk = f.stop_sk
JOIN dw.dim_mode  dm ON dm.mode_sk = f.mode_sk
WHERE f.tipo_dia = 'LABORAL'
GROUP BY ds.stop_code, ds.comuna, ds.zone_code, dm.mode_code, ds.x_utm, ds.y_utm
ORDER BY subidas_promedio_dia DESC;


-- -----------------------------------------------------------------------------
-- Q8: Desigualdad territorial — comunas sub-servidas
-- -----------------------------------------------------------------------------
-- Calcula la relación subidas/paradero por comuna. Una comuna con muchos
-- paraderos pero pocas subidas tiene baja utilización de su infraestructura
-- (puede ser subcobertura de red, demografía o mala conectividad).
-- La función NTILE(4) clasifica automáticamente las comunas en cuartiles:
-- el primer cuartil son las más sub-servidas → candidatas prioritarias
-- a inversión o rediseño de red.
-- Esta query es evidencia directa para política pública territorial.
-- -----------------------------------------------------------------------------
WITH subidas_x_comuna AS (
    SELECT
        ds.comuna,
        COUNT(DISTINCT ds.stop_code)                AS num_paradas,
        ROUND(SUM(f.subidas_promedio), 0)           AS subidas_promedio_dia,
        ROUND(
            SUM(f.subidas_promedio)
            / NULLIF(COUNT(DISTINCT ds.stop_code), 0)
            , 1)                                    AS subidas_por_parada
    FROM dw.fct_boardings_30m f
    JOIN dw.dim_stop ds ON ds.stop_sk = f.stop_sk
    WHERE f.tipo_dia    = 'LABORAL'
      AND ds.comuna IS NOT NULL
    GROUP BY ds.comuna
)
SELECT
    comuna,
    num_paradas,
    subidas_promedio_dia,
    subidas_por_parada,
    NTILE(4) OVER (ORDER BY subidas_por_parada)     AS cuartil_cobertura,
    CASE NTILE(4) OVER (ORDER BY subidas_por_parada)
        WHEN 1 THEN 'Sub-servida'
        WHEN 2 THEN 'Baja cobertura'
        WHEN 3 THEN 'Cobertura media'
        WHEN 4 THEN 'Bien servida'
    END                                             AS categoria
FROM subidas_x_comuna
ORDER BY subidas_por_parada;

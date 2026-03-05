-- ─────────────────────────────────────────────────────────────
-- cleanup_cut.sql  —  Borrar datos de un corte específico de facts
--
-- Uso:
--   Reemplazar @dataset y @cut_id con los valores deseados antes de ejecutar.
--   Este script NO borra dims (dim_stop, dim_service, dim_cut, etc.),
--   solo limpia las filas de fact tables para el corte indicado.
--
-- Ejemplo:
--   DECLARE @dataset VARCHAR(30) = 'viajes';
--   DECLARE @cut_id  VARCHAR(40) = '2025-04-21';
-- ─────────────────────────────────────────────────────────────

DECLARE @dataset VARCHAR(30) = 'viajes';       -- 'viajes', 'etapas', 'subidas_30m'
DECLARE @cut_id  VARCHAR(40) = '2025-04-21';   -- el cut a limpiar

-- 1. Resolución de cut_sk
DECLARE @cut_sk INT;
SELECT @cut_sk = cut_sk
FROM dw.dim_cut
WHERE dataset_name = @dataset AND cut_id = @cut_id;

IF @cut_sk IS NULL
BEGIN
    RAISERROR(N'No se encontró cut_sk para dataset=%s cut=%s — abort.', 16, 1, @dataset, @cut_id);
    RETURN;
END;

PRINT CONCAT(N'Limpiando cut_sk=', @cut_sk, N' (dataset=', @dataset, N' cut=', @cut_id, N')');

-- 2. viajes: fct_trip_leg primero (FK a fct_trip)
IF @dataset = 'viajes'
BEGIN
    DECLARE @leg_del INT;
    DELETE dw.fct_trip_leg WHERE cut_sk = @cut_sk;
    SET @leg_del = @@ROWCOUNT;
    PRINT CONCAT(N'  dw.fct_trip_leg eliminadas: ', @leg_del);

    DECLARE @trip_del INT;
    DELETE dw.fct_trip WHERE cut_sk = @cut_sk;
    SET @trip_del = @@ROWCOUNT;
    PRINT CONCAT(N'  dw.fct_trip eliminadas: ', @trip_del);
END;

-- 3. etapas: fct_validation
IF @dataset = 'etapas'
BEGIN
    DECLARE @val_del INT;
    DELETE dw.fct_validation WHERE cut_sk = @cut_sk;
    SET @val_del = @@ROWCOUNT;
    PRINT CONCAT(N'  dw.fct_validation eliminadas: ', @val_del);
END;

-- 4. subidas_30m: fct_boardings_30m
IF @dataset = 'subidas_30m'
BEGIN
    DECLARE @b_del INT;
    DELETE dw.fct_boardings_30m WHERE cut_sk = @cut_sk;
    SET @b_del = @@ROWCOUNT;
    PRINT CONCAT(N'  dw.fct_boardings_30m eliminadas: ', @b_del);
END;

PRINT N'Cleanup completado.';

-- =============================================================================
-- ddl_gold.sql  —  Capa Gold DTPM Movilidad Santiago
-- Motor: SQL Server (Azure SQL / SQL Server 2019+)
-- Schemas: staging (tablas de paso para bulk load), dw (DW Kimball)
--
-- Convenciones:
--   - Dims: PK identity + BK natural key con UQ constraint
--   - SCD2: dim_stop, dim_service → valid_from/valid_to/is_current/row_hash
--   - Facts: PK identity (trip_sk, etc.) + UQ por grain natural + FKs
--   - Staging: sin FKs, truncate+reload por cut antes de cada merge
--   - Índices: clustered en PK, nonclustered en FKs de join frecuente
--   - Columnstore: sugerido como índice adicional en facts (comentado);
--     incompatible con FKs enforced → usar NONCLUSTERED COLUMNSTORE
-- =============================================================================

-- ─────────────────────────────────────────────────────────────
-- 0. SCHEMAS
-- ─────────────────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC sp_executesql N'CREATE SCHEMA staging';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC sp_executesql N'CREATE SCHEMA dw';

-- ─────────────────────────────────────────────────────────────
-- 1. STAGING — tablas de paso (DROP + CREATE para idempotencia total)
--    Truncadas antes de cada carga de cut.  Sin FK, sin constraints.
--    Los tipos son los más amplios para absorber cualquier registro Silver.
-- ─────────────────────────────────────────────────────────────

-- 1.1  stg_viajes_trip  (de viajes_trip.parquet)
IF OBJECT_ID(N'staging.stg_viajes_trip', N'U') IS NOT NULL
    DROP TABLE staging.stg_viajes_trip;

CREATE TABLE staging.stg_viajes_trip (
    cut                     DATE         NULL,
    year                    SMALLINT     NULL,
    month                   TINYINT      NULL,
    id_viaje                NVARCHAR(80) NULL,
    id_tarjeta              NVARCHAR(40) NULL,
    tipo_dia                VARCHAR(10)  NULL,
    proposito               NVARCHAR(60) NULL,
    contrato                NVARCHAR(40) NULL,
    factor_expansion        FLOAT        NULL,
    n_etapas                TINYINT      NULL,
    distancia_eucl          FLOAT        NULL,   -- metros
    distancia_ruta          FLOAT        NULL,   -- metros
    tiempo_inicio_viaje     DATETIME2(0) NULL,
    tiempo_fin_viaje        DATETIME2(0) NULL,
    date_start_sk           INT          NULL,   -- YYYYMMDD
    time_start_30m_sk       TINYINT      NULL,   -- 0..47
    date_end_sk             INT          NULL,
    time_end_30m_sk         TINYINT      NULL,
    paradero_inicio_viaje   NVARCHAR(40) NULL,   -- stop BK
    paradero_fin_viaje      NVARCHAR(40) NULL,   -- stop BK
    comuna_inicio_viaje     NVARCHAR(80) NULL,
    comuna_fin_viaje        NVARCHAR(80) NULL,
    zona_inicio_viaje       INT          NULL,
    zona_fin_viaje          INT          NULL,
    periodo_inicio_viaje    VARCHAR(40)  NULL,   -- fare period BK
    periodo_fin_viaje       VARCHAR(40)  NULL,
    tviaje_min              FLOAT        NULL,
    stg_loaded_at           DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 1.2  stg_viajes_leg  (de viajes_leg.parquet)
IF OBJECT_ID(N'staging.stg_viajes_leg', N'U') IS NOT NULL
    DROP TABLE staging.stg_viajes_leg;

CREATE TABLE staging.stg_viajes_leg (
    cut                     DATE         NULL,
    year                    SMALLINT     NULL,
    month                   TINYINT      NULL,
    id_viaje                NVARCHAR(80) NULL,
    id_tarjeta              NVARCHAR(40) NULL,
    leg_seq                 TINYINT      NULL,   -- 1..4
    mode_code               VARCHAR(15)  NULL,
    service_code            NVARCHAR(40) NULL,
    operator_code           NVARCHAR(40) NULL,
    board_stop_code         NVARCHAR(40) NULL,
    alight_stop_code        NVARCHAR(40) NULL,
    ts_board                DATETIME2(0) NULL,
    ts_alight               DATETIME2(0) NULL,
    date_board_sk           INT          NULL,
    time_board_30m_sk       TINYINT      NULL,
    date_alight_sk          INT          NULL,
    time_alight_30m_sk      TINYINT      NULL,
    fare_period_alight_code VARCHAR(40)  NULL,
    zone_board              INT          NULL,
    zone_alight             INT          NULL,
    tv_leg_min              FLOAT        NULL,
    tc_transfer_min         FLOAT        NULL,
    te_wait_min             FLOAT        NULL,
    stg_loaded_at           DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 1.3  stg_etapas_validation  (de etapas_validation.parquet)
IF OBJECT_ID(N'staging.stg_etapas_validation', N'U') IS NOT NULL
    DROP TABLE staging.stg_etapas_validation;

CREATE TABLE staging.stg_etapas_validation (
    cut                         VARCHAR(40)  NULL,
    year                        SMALLINT     NULL,
    month                       TINYINT      NULL,
    id_etapa                    NVARCHAR(80) NULL,
    operador                    NVARCHAR(40) NULL,      -- operator BK
    contrato                    NVARCHAR(40) NULL,      -- contract BK
    tipo_dia                    VARCHAR(10)  NULL,
    tipo_transporte             VARCHAR(15)  NULL,      -- mode BK
    fExpansionServicioPeriodoTS FLOAT        NULL,
    tiene_bajada                BIT          NULL,
    tiempo_subida               DATETIME2(0) NULL,
    tiempo_bajada               DATETIME2(0) NULL,
    tiempo_etapa                INT          NULL,      -- segundos
    date_board_sk               INT          NULL,
    time_board_30m_sk           TINYINT      NULL,
    date_alight_sk              INT          NULL,
    time_alight_30m_sk          TINYINT      NULL,
    x_subida                    INT          NULL,
    y_subida                    INT          NULL,
    x_bajada                    INT          NULL,
    y_bajada                    INT          NULL,
    dist_ruta_paraderos         INT          NULL,
    dist_eucl_paraderos         INT          NULL,
    servicio_subida             NVARCHAR(40) NULL,      -- service BK (board)
    servicio_bajada             NVARCHAR(40) NULL,      -- service BK (alight)
    parada_subida               NVARCHAR(40) NULL,      -- stop BK (board)
    parada_bajada               NVARCHAR(40) NULL,      -- stop BK (alight)
    comuna_subida               NVARCHAR(80) NULL,
    comuna_bajada               NVARCHAR(80) NULL,
    zona_subida                 INT          NULL,
    zona_bajada                 INT          NULL,
    tEsperaMediaIntervalo       FLOAT        NULL,
    periodoSubida               VARCHAR(40)  NULL,      -- fare period BK
    periodoBajada               VARCHAR(40)  NULL,
    stg_loaded_at               DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 1.4  stg_subidas_30m  (de subidas_30m.parquet)
IF OBJECT_ID(N'staging.stg_subidas_30m', N'U') IS NOT NULL
    DROP TABLE staging.stg_subidas_30m;

CREATE TABLE staging.stg_subidas_30m (
    cut               VARCHAR(40)  NULL,
    year              SMALLINT     NULL,
    month             TINYINT      NULL,
    tipo_dia          VARCHAR(10)  NULL,
    mode_code         VARCHAR(15)  NULL,
    stop_code         NVARCHAR(40) NULL,
    comuna            NVARCHAR(80) NULL,
    time_30m_sk       TINYINT      NULL,
    subidas_promedio  FLOAT        NULL,
    stg_loaded_at     DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ─────────────────────────────────────────────────────────────
-- 2. DIMENSIONES CONFORMADAS
-- ─────────────────────────────────────────────────────────────

-- 2.1  dim_date  —  Calendario (grain: 1 fila = 1 día)
--      PK = date_sk (INT YYYYMMDD) — no identity; es el surrogate key natural.
--      Cargada por el loader Python al detectar nuevas fechas en staging.
IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_date (
        date_sk         INT          NOT NULL,          -- YYYYMMDD; PK natural, no identity
        full_date       DATE         NOT NULL,
        year            SMALLINT     NOT NULL,
        month           TINYINT      NOT NULL,
        day             TINYINT      NOT NULL,
        iso_week        TINYINT      NOT NULL,
        day_of_week     VARCHAR(10)  NOT NULL,           -- 'Lunes', 'Martes', …
        month_name      VARCHAR(10)  NOT NULL,           -- 'Enero', 'Febrero', …
        is_weekend      BIT          NOT NULL,
        year_month      CHAR(7)      NOT NULL,           -- 'YYYY-MM'
        tipo_dia        VARCHAR(10)  NULL,               -- 'LABORAL','SABADO','DOMINGO'
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_sk)
    );
END;

-- 2.2  dim_time_30m  —  Slots de 30 minutos (grain: 48 filas, 0..47)
--      Cargada una sola vez; no cambia.
IF OBJECT_ID(N'dw.dim_time_30m', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_time_30m (
        time_30m_sk     TINYINT      NOT NULL,   -- 0=00:00, 1=00:30, …, 47=23:30
        start_time      TIME(0)      NOT NULL,
        end_time        TIME(0)      NOT NULL,
        hour            TINYINT      NOT NULL,
        minute          TINYINT      NOT NULL,   -- 0 o 30
        label           CHAR(5)      NOT NULL,   -- 'HH:MM'
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_time_30m PRIMARY KEY CLUSTERED (time_30m_sk)
    );
END;

-- 2.3  dim_mode  —  Modo de transporte (5 filas: BUS/METRO/METROTREN/ZP/UNKNOWN)
IF OBJECT_ID(N'dw.dim_mode', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_mode (
        mode_sk         TINYINT      NOT NULL IDENTITY(1,1),
        mode_code       VARCHAR(15)  NOT NULL,
        mode_desc       VARCHAR(50)  NULL,
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_mode           PRIMARY KEY CLUSTERED (mode_sk),
        CONSTRAINT UQ_dim_mode_code      UNIQUE NONCLUSTERED (mode_code)
    );
END;

-- 2.4  dim_stop  —  Paraderos/Estaciones  [SCD Tipo 2]
--      BK: stop_code  |  tracked attrs: comuna, zone_code, x_utm, y_utm
--      row_hash = SHA2_256 de los atributos concatenados
--      Índice filtrado en is_current=1 acelera los as-of joins en facts.
IF OBJECT_ID(N'dw.dim_stop', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_stop (
        stop_sk         BIGINT        NOT NULL IDENTITY(1,1),
        stop_code       NVARCHAR(40)  NOT NULL,        -- BK: código DTPM del paradero
        stop_name       NVARCHAR(120) NULL,            -- TODO: no disponible en Silver actual
        stop_type       VARCHAR(20)   NULL,            -- TODO: no disponible en Silver actual
        comuna          NVARCHAR(80)  NULL,
        zone_code       VARCHAR(20)   NULL,            -- zona como string (ej. '13')
        x_utm           INT           NULL,            -- coordenada UTM WGS84
        y_utm           INT           NULL,
        -- SCD2
        row_hash        CHAR(64)      NOT NULL,        -- HEX del SHA2_256 de atributos
        valid_from      DATE          NOT NULL,
        valid_to        DATE          NULL,            -- NULL = fila actualmente vigente
        is_current      BIT           NOT NULL DEFAULT 1,
        is_active       BIT           NOT NULL DEFAULT 1,
        record_source   VARCHAR(30)   NOT NULL DEFAULT 'DTPM',
        loaded_at       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_stop              PRIMARY KEY CLUSTERED (stop_sk)
    );

    -- BK + valid_from: garantiza unicidad dentro del ciclo SCD2
    CREATE UNIQUE NONCLUSTERED INDEX UX_dim_stop_bk_from
        ON dw.dim_stop (stop_code, valid_from);

    -- Índice filtrado para lookup as-of eficiente (join con facts)
    CREATE NONCLUSTERED INDEX IX_dim_stop_current
        ON dw.dim_stop (stop_code, stop_sk)
        WHERE is_current = 1;
END;

-- 2.5  dim_service  —  Servicios de transporte  [SCD Tipo 2]
--      BK: service_code  |  tracked attrs: mode_code, service_name
IF OBJECT_ID(N'dw.dim_service', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_service (
        service_sk      INT           NOT NULL IDENTITY(1,1),
        service_code    NVARCHAR(40)  NOT NULL,        -- BK: código de servicio DTPM
        service_name    NVARCHAR(120) NULL,            -- TODO: no disponible en Silver actual
        mode_code       VARCHAR(15)   NULL,            -- denormalizado para consultas rápidas
        -- SCD2
        row_hash        CHAR(64)      NOT NULL,
        valid_from      DATE          NOT NULL,
        valid_to        DATE          NULL,
        is_current      BIT           NOT NULL DEFAULT 1,
        is_active       BIT           NOT NULL DEFAULT 1,
        record_source   VARCHAR(30)   NOT NULL DEFAULT 'DTPM',
        loaded_at       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_service          PRIMARY KEY CLUSTERED (service_sk)
    );

    CREATE UNIQUE NONCLUSTERED INDEX UX_dim_service_bk_from
        ON dw.dim_service (service_code, valid_from);

    CREATE NONCLUSTERED INDEX IX_dim_service_current
        ON dw.dim_service (service_code, service_sk)
        WHERE is_current = 1;
END;

-- 2.6  dim_operator_contract  —  Operadores y contratos
--      BK: contract_code  (operador puede aparecer en varios contratos)
IF OBJECT_ID(N'dw.dim_operator_contract', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_operator_contract (
        operator_sk     INT          NOT NULL IDENTITY(1,1),
        contract_code   NVARCHAR(40) NOT NULL,        -- BK: código de contrato
        operator_code   NVARCHAR(30) NULL,            -- código de operador (si aplica)
        operator_name   NVARCHAR(120) NULL,           -- TODO: no disponible en Silver actual
        record_source   VARCHAR(30)  NOT NULL DEFAULT 'DTPM',
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_operator_contract  PRIMARY KEY CLUSTERED (operator_sk),
        CONSTRAINT UQ_dim_operator_contract  UNIQUE NONCLUSTERED (contract_code)
    );
END;

-- 2.7  dim_fare_period  —  Periodos tarifarios (Punta, Valle, Nocturno, …)
IF OBJECT_ID(N'dw.dim_fare_period', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_fare_period (
        fare_period_sk  INT          NOT NULL IDENTITY(1,1),
        fare_period_name VARCHAR(40) NOT NULL,        -- BK: nombre del período
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_fare_period  PRIMARY KEY CLUSTERED (fare_period_sk),
        CONSTRAINT UQ_dim_fare_period  UNIQUE NONCLUSTERED (fare_period_name)
    );
END;

-- 2.8  dim_purpose  —  Propósito del viaje (Trabajo, Estudio, …)
IF OBJECT_ID(N'dw.dim_purpose', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_purpose (
        purpose_sk      INT          NOT NULL IDENTITY(1,1),
        purpose_name    VARCHAR(60)  NOT NULL,        -- BK: nombre del propósito
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_purpose  PRIMARY KEY CLUSTERED (purpose_sk),
        CONSTRAINT UQ_dim_purpose  UNIQUE NONCLUSTERED (purpose_name)
    );
END;

-- 2.9  dim_cut  —  Metadatos de cada corte Silver (partition metadata)
--      Equivale a la dimensión de snapshot/batch.  Alimentada desde quality.json.
IF OBJECT_ID(N'dw.dim_cut', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_cut (
        cut_sk          INT          NOT NULL IDENTITY(1,1),
        dataset_name    VARCHAR(30)  NOT NULL,        -- 'viajes', 'etapas', 'subidas_30m'
        cut_id          VARCHAR(40)  NOT NULL,        -- '2025-04-21', '2025-04'
        year            SMALLINT     NOT NULL,
        month           TINYINT      NOT NULL,
        source_file     VARCHAR(200) NULL,
        extracted_at    DATETIME2(0) NULL,           -- generated_at del quality.json
        row_count       BIGINT       NULL,           -- read_row_count
        file_size_bytes BIGINT       NULL,           -- TODO: add in future Silver pipeline
        separator       CHAR(1)      NULL DEFAULT '|',
        encoding        VARCHAR(20)  NULL DEFAULT 'utf-8',
        loaded_at       DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_cut        PRIMARY KEY CLUSTERED (cut_sk),
        CONSTRAINT UQ_dim_cut        UNIQUE NONCLUSTERED (dataset_name, cut_id)
    );
END;

-- ─────────────────────────────────────────────────────────────
-- 3. TABLAS DE HECHOS
--    Orden de creación respeta dependencias FK:
--    dims → fct_trip → fct_trip_leg (FK→fct_trip)
-- ─────────────────────────────────────────────────────────────

-- 3.1  fct_trip  —  Mart 'Trips & OD'  (grain: 1 fila = 1 viaje)
IF OBJECT_ID(N'dw.fct_trip', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fct_trip (
        trip_sk                 BIGINT       NOT NULL IDENTITY(1,1),

        -- ── Llaves de tiempo ───────────────────────────────────
        date_start_sk           INT          NOT NULL,
        time_start_30m_sk       TINYINT      NULL,
        date_end_sk             INT          NULL,
        time_end_30m_sk         TINYINT      NULL,

        -- ── FKs a dimensiones ──────────────────────────────────
        -- Role-playing stop: origin = paradero_inicio, dest = paradero_fin
        origin_stop_sk          BIGINT       NULL,
        dest_stop_sk            BIGINT       NULL,
        -- Role-playing fare period: inicio/fin del viaje
        fare_period_start_sk    INT          NULL,
        fare_period_end_sk      INT          NULL,
        operator_sk             INT          NULL,   -- de contrato
        purpose_sk              INT          NULL,
        cut_sk                  INT          NOT NULL,

        -- ── Dimensiones degeneradas ────────────────────────────
        id_viaje                NVARCHAR(80) NOT NULL,   -- BK del viaje
        id_tarjeta              NVARCHAR(40) NULL,       -- NULL en viajes en efectivo
        tipo_dia                VARCHAR(10)  NULL,
        zone_origin_txt         VARCHAR(20)  NULL,
        zone_dest_txt           VARCHAR(20)  NULL,

        -- ── Medidas ────────────────────────────────────────────
        n_etapas                TINYINT      NULL,       -- 1..4
        tviaje_min              FLOAT        NULL,
        distancia_eucl_m        FLOAT        NULL,       -- de distancia_eucl
        distancia_ruta_m        FLOAT        NULL,       -- de distancia_ruta
        factor_expansion        FLOAT        NULL,

        -- TODO: los siguientes campos no están en Silver actual
        -- miembro_expedicion   INT          NULL,
        -- utilizacion_expedi   FLOAT        NULL,
        -- servicio_zs          VARCHAR(20)  NULL,

        loaded_at               DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fct_trip              PRIMARY KEY CLUSTERED (trip_sk),
        -- Grain real: (cut_sk, id_tarjeta, id_viaje) — ver UX_fct_trip_grain (sección 4b)
        -- id_viaje es contador por tarjeta/día (1..27), NO un ID global.
        CONSTRAINT FK_fct_trip_date_start   FOREIGN KEY (date_start_sk)        REFERENCES dw.dim_date (date_sk),
        CONSTRAINT FK_fct_trip_time_start   FOREIGN KEY (time_start_30m_sk)    REFERENCES dw.dim_time_30m (time_30m_sk),
        CONSTRAINT FK_fct_trip_origin       FOREIGN KEY (origin_stop_sk)        REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_trip_dest         FOREIGN KEY (dest_stop_sk)          REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_trip_fp_start     FOREIGN KEY (fare_period_start_sk)  REFERENCES dw.dim_fare_period (fare_period_sk),
        CONSTRAINT FK_fct_trip_fp_end       FOREIGN KEY (fare_period_end_sk)    REFERENCES dw.dim_fare_period (fare_period_sk),
        CONSTRAINT FK_fct_trip_operator     FOREIGN KEY (operator_sk)           REFERENCES dw.dim_operator_contract (operator_sk),
        CONSTRAINT FK_fct_trip_purpose      FOREIGN KEY (purpose_sk)            REFERENCES dw.dim_purpose (purpose_sk),
        CONSTRAINT FK_fct_trip_cut          FOREIGN KEY (cut_sk)                REFERENCES dw.dim_cut (cut_sk)
    );

    -- Índices de acceso frecuente en análisis OD
    CREATE NONCLUSTERED INDEX IX_fct_trip_date_start
        ON dw.fct_trip (date_start_sk, cut_sk) INCLUDE (origin_stop_sk, dest_stop_sk, n_etapas);

    CREATE NONCLUSTERED INDEX IX_fct_trip_origin_stop
        ON dw.fct_trip (origin_stop_sk, date_start_sk);

    CREATE NONCLUSTERED INDEX IX_fct_trip_cut
        ON dw.fct_trip (cut_sk);

    -- SUGERENCIA COLUMNSTORE (descomentar si no se usan FKs enforced):
    -- CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_fct_trip
    --     ON dw.fct_trip (date_start_sk, origin_stop_sk, dest_stop_sk,
    --                     n_etapas, tviaje_min, distancia_ruta_m, factor_expansion);
END;

-- 3.2  fct_trip_leg  —  Mart 'Trip Legs'  (grain: id_viaje + leg_seq + cut)
IF OBJECT_ID(N'dw.fct_trip_leg', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fct_trip_leg (
        trip_leg_sk             BIGINT       NOT NULL IDENTITY(1,1),
        trip_sk                 BIGINT       NULL,       -- FK a fct_trip (NULL si aún no cargado)

        -- ── Grain ──────────────────────────────────────────────
        id_viaje                NVARCHAR(80) NOT NULL,   -- BK degenerada
        leg_seq                 TINYINT      NOT NULL,   -- 1..4
        cut_sk                  INT          NOT NULL,

        -- ── Tiempo del embarque (rol principal para análisis) ──
        date_board_sk           INT          NULL,
        time_board_30m_sk       TINYINT      NULL,
        date_alight_sk          INT          NULL,
        time_alight_30m_sk      TINYINT      NULL,

        -- ── FKs a dimensiones ──────────────────────────────────
        board_stop_sk           BIGINT       NULL,
        alight_stop_sk          BIGINT       NULL,
        mode_sk                 TINYINT      NULL,
        service_sk              INT          NULL,
        operator_sk             INT          NULL,
        fare_period_alight_sk   INT          NULL,

        -- ── Degeneradas ────────────────────────────────────────
        id_tarjeta              NVARCHAR(40) NULL,
        zone_board_txt          VARCHAR(20)  NULL,
        zone_alight_txt         VARCHAR(20)  NULL,
        ts_board                DATETIME2(0) NULL,
        ts_alight               DATETIME2(0) NULL,

        -- ── Medidas ────────────────────────────────────────────
        tv_leg_min              FLOAT        NULL,
        tc_transfer_min         FLOAT        NULL,
        te_wait_min             FLOAT        NULL,

        loaded_at               DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fct_trip_leg              PRIMARY KEY CLUSTERED (trip_leg_sk),
        -- Grain real: (cut_sk, id_tarjeta, id_viaje, leg_seq) — ver UX_fct_trip_leg_grain (sección 4b)
        CONSTRAINT FK_fct_trip_leg_trip         FOREIGN KEY (trip_sk)               REFERENCES dw.fct_trip (trip_sk),
        CONSTRAINT FK_fct_trip_leg_date         FOREIGN KEY (date_board_sk)          REFERENCES dw.dim_date (date_sk),
        CONSTRAINT FK_fct_trip_leg_time         FOREIGN KEY (time_board_30m_sk)      REFERENCES dw.dim_time_30m (time_30m_sk),
        CONSTRAINT FK_fct_trip_leg_board_stop   FOREIGN KEY (board_stop_sk)          REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_trip_leg_alight_stop  FOREIGN KEY (alight_stop_sk)         REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_trip_leg_mode         FOREIGN KEY (mode_sk)                REFERENCES dw.dim_mode (mode_sk),
        CONSTRAINT FK_fct_trip_leg_service      FOREIGN KEY (service_sk)             REFERENCES dw.dim_service (service_sk),
        CONSTRAINT FK_fct_trip_leg_operator     FOREIGN KEY (operator_sk)            REFERENCES dw.dim_operator_contract (operator_sk),
        CONSTRAINT FK_fct_trip_leg_fp           FOREIGN KEY (fare_period_alight_sk)  REFERENCES dw.dim_fare_period (fare_period_sk),
        CONSTRAINT FK_fct_trip_leg_cut          FOREIGN KEY (cut_sk)                 REFERENCES dw.dim_cut (cut_sk)
    );

    CREATE NONCLUSTERED INDEX IX_fct_trip_leg_viaje
        ON dw.fct_trip_leg (id_viaje, leg_seq, cut_sk);

    CREATE NONCLUSTERED INDEX IX_fct_trip_leg_board_date
        ON dw.fct_trip_leg (date_board_sk, board_stop_sk);

    -- SUGERENCIA COLUMNSTORE:
    -- CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_fct_trip_leg
    --     ON dw.fct_trip_leg (date_board_sk, board_stop_sk, mode_sk, tv_leg_min, tc_transfer_min);
END;

-- 3.3  fct_validation  —  Mart 'Stages & Operations'  (grain: id_etapa + tiempo_boarding + cut)
-- id_etapa NO es único: para ZP/Metro es código de estación/zona repetido por pasajero.
-- La combinación (id_etapa, tiempo_boarding) identifica unívocamente cada validación.
IF OBJECT_ID(N'dw.fct_validation', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fct_validation (
        validation_sk               BIGINT       NOT NULL IDENTITY(1,1),

        -- ── Grain ─────────────────────────────────
        id_etapa                    NVARCHAR(80) NOT NULL,
        tiempo_boarding             DATETIME2(0) NOT NULL,   -- tiempo_subida; discrimina pasajeros con mismo id_etapa
        cut_sk                      INT          NOT NULL,

        -- ── Tiempo ─────────────────────────────────────────────
        date_board_sk               INT          NULL,
        time_board_30m_sk           TINYINT      NULL,
        date_alight_sk              INT          NULL,
        time_alight_30m_sk          TINYINT      NULL,

        -- ── FKs a dimensiones ──────────────────────────────────
        board_stop_sk               BIGINT       NULL,
        alight_stop_sk              BIGINT       NULL,
        mode_sk                     TINYINT      NULL,
        -- Role-playing service: subida vs bajada
        service_board_sk            INT          NULL,
        service_alight_sk           INT          NULL,
        operator_sk                 INT          NULL,
        -- Role-playing fare period: subida vs bajada
        fare_period_board_sk        INT          NULL,
        fare_period_alight_sk       INT          NULL,

        -- ── Degeneradas ────────────────────────────────────────
        tipo_dia                    VARCHAR(10)  NULL,
        tiene_bajada                BIT          NULL,
        -- TODO: correlativo_viajes / correlativo_etapas (no en Silver actual)

        -- ── Medidas ────────────────────────────────────────────
        tiempo_bajada               DATETIME2(0) NULL,
        tiempo_etapa_sec            INT          NULL,   -- de tiempo_etapa
        t_espera_media_min          FLOAT        NULL,   -- de tEsperaMediaIntervalo
        dist_ruta_m                 INT          NULL,   -- de dist_ruta_paraderos
        dist_eucl_m                 INT          NULL,   -- de dist_eucl_paraderos
        x_subida                    INT          NULL,
        y_subida                    INT          NULL,
        x_bajada                    INT          NULL,
        y_bajada                    INT          NULL,
        fexp_servicio               FLOAT        NULL,   -- de fExpansionServicioPeriodoTS

        -- TODO: campos no disponibles en Silver actual
        -- fexp_zona                FLOAT        NULL,
        -- stm_subida               VARCHAR(20)  NULL,
        -- stm_bajada               VARCHAR(20)  NULL,
        -- tiempo_in_expedicion     FLOAT        NULL,

        loaded_at                   DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fct_validation              PRIMARY KEY CLUSTERED (validation_sk),
        CONSTRAINT UQ_fct_validation_grain        UNIQUE NONCLUSTERED (id_etapa, cut_sk, tiempo_boarding),
        CONSTRAINT FK_fct_val_date                FOREIGN KEY (date_board_sk)         REFERENCES dw.dim_date (date_sk),
        CONSTRAINT FK_fct_val_time                FOREIGN KEY (time_board_30m_sk)     REFERENCES dw.dim_time_30m (time_30m_sk),
        CONSTRAINT FK_fct_val_board_stop          FOREIGN KEY (board_stop_sk)         REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_val_alight_stop         FOREIGN KEY (alight_stop_sk)        REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_val_mode                FOREIGN KEY (mode_sk)               REFERENCES dw.dim_mode (mode_sk),
        CONSTRAINT FK_fct_val_svc_board           FOREIGN KEY (service_board_sk)      REFERENCES dw.dim_service (service_sk),
        CONSTRAINT FK_fct_val_svc_alight          FOREIGN KEY (service_alight_sk)     REFERENCES dw.dim_service (service_sk),
        CONSTRAINT FK_fct_val_operator            FOREIGN KEY (operator_sk)           REFERENCES dw.dim_operator_contract (operator_sk),
        CONSTRAINT FK_fct_val_fp_board            FOREIGN KEY (fare_period_board_sk)  REFERENCES dw.dim_fare_period (fare_period_sk),
        CONSTRAINT FK_fct_val_fp_alight           FOREIGN KEY (fare_period_alight_sk) REFERENCES dw.dim_fare_period (fare_period_sk),
        CONSTRAINT FK_fct_val_cut                 FOREIGN KEY (cut_sk)                REFERENCES dw.dim_cut (cut_sk)
    );

    CREATE NONCLUSTERED INDEX IX_fct_val_board_stop
        ON dw.fct_validation (board_stop_sk, date_board_sk);

    CREATE NONCLUSTERED INDEX IX_fct_val_service
        ON dw.fct_validation (service_board_sk, date_board_sk);

    CREATE NONCLUSTERED INDEX IX_fct_val_cut
        ON dw.fct_validation (cut_sk);

    -- SUGERENCIA COLUMNSTORE:
    -- CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_fct_validation
    --     ON dw.fct_validation (date_board_sk, board_stop_sk, mode_sk,
    --                           dist_ruta_m, t_espera_media_min, fexp_servicio);
END;

-- 3.4  fct_boardings_30m  —  Mart 'Network Demand'
--      (grain: month_date_sk + time_30m_sk + stop_sk + mode_sk + tipo_dia + cut_sk)
IF OBJECT_ID(N'dw.fct_boardings_30m', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fct_boardings_30m (
        boardings_30m_sk    BIGINT       NOT NULL IDENTITY(1,1),

        -- ── Grain ──────────────────────────────────────────────
        -- month_date_sk = date_sk del primer día del mes (YYYY MM 01)
        month_date_sk       INT          NOT NULL,
        time_30m_sk         TINYINT      NOT NULL,
        stop_sk             BIGINT       NOT NULL,
        mode_sk             TINYINT      NOT NULL,
        cut_sk              INT          NOT NULL,

        -- ── Degenerada ─────────────────────────────────────────
        tipo_dia            VARCHAR(10)  NOT NULL,   -- 'LABORAL','SABADO','DOMINGO'
        comuna_txt          NVARCHAR(80) NULL,

        -- ── Medida ─────────────────────────────────────────────
        subidas_promedio    FLOAT        NOT NULL,

        loaded_at           DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fct_boardings         PRIMARY KEY CLUSTERED (boardings_30m_sk),
        CONSTRAINT UQ_fct_boardings_grain   UNIQUE NONCLUSTERED
            (month_date_sk, time_30m_sk, stop_sk, mode_sk, tipo_dia, cut_sk),
        CONSTRAINT FK_fct_boardings_date    FOREIGN KEY (month_date_sk)   REFERENCES dw.dim_date (date_sk),
        CONSTRAINT FK_fct_boardings_time    FOREIGN KEY (time_30m_sk)     REFERENCES dw.dim_time_30m (time_30m_sk),
        CONSTRAINT FK_fct_boardings_stop    FOREIGN KEY (stop_sk)         REFERENCES dw.dim_stop (stop_sk),
        CONSTRAINT FK_fct_boardings_mode    FOREIGN KEY (mode_sk)         REFERENCES dw.dim_mode (mode_sk),
        CONSTRAINT FK_fct_boardings_cut     FOREIGN KEY (cut_sk)          REFERENCES dw.dim_cut (cut_sk)
    );

    CREATE NONCLUSTERED INDEX IX_fct_boardings_stop_time
        ON dw.fct_boardings_30m (stop_sk, time_30m_sk, month_date_sk);

    -- SUGERENCIA COLUMNSTORE (ideal para este mart: alta cardinalidad, pocas medidas):
    -- CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_fct_boardings
    --     ON dw.fct_boardings_30m (month_date_sk, time_30m_sk, stop_sk, mode_sk,
    --                              tipo_dia, subidas_promedio);
END;

-- ─────────────────────────────────────────────────────────────
-- 4. AUDITABILIDAD — ETL Run Log
--    Un registro por ejecución (dataset + cut).
--    status: 'RUNNING' → 'OK' | 'FAILED'
-- ─────────────────────────────────────────────────────────────
IF OBJECT_ID(N'dw.etl_run_log', N'U') IS NULL
BEGIN
    CREATE TABLE dw.etl_run_log (
        run_id          INT           NOT NULL IDENTITY(1,1),
        started_at      DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at     DATETIME2(3)  NULL,
        dataset         VARCHAR(30)   NOT NULL,   -- 'viajes', 'etapas', 'subidas_30m'
        cut             VARCHAR(40)   NOT NULL,   -- '2025-04-21', '2025-04'
        status          VARCHAR(10)   NOT NULL DEFAULT 'RUNNING',  -- 'OK','FAILED','RUNNING'
        rows_staged     BIGINT        NULL,       -- filas insertadas en staging
        rows_inserted   BIGINT        NULL,       -- filas insertadas en fact
        rows_updated      BIGINT        NOT NULL DEFAULT 0,  -- siempre 0 (solo INSERT NOT MATCHED)
        ignored_cash_rows BIGINT        NULL,                -- viajes en efectivo excluidos de facts
        error_message     NVARCHAR(MAX) NULL,
        loader_version    VARCHAR(20)   NULL,       -- semver del loader Python

        CONSTRAINT PK_etl_run_log PRIMARY KEY CLUSTERED (run_id)
    );

    CREATE NONCLUSTERED INDEX IX_etl_run_log_dataset_cut
        ON dw.etl_run_log (dataset, cut, started_at DESC);
END;

-- ─────────────────────────────────────────────────────────────
-- 4b. CORRECCIÓN DE GRAIN: fct_trip / fct_trip_leg
--     El grain real es (cut_sk, id_tarjeta, id_viaje) porque id_viaje
--     es un contador secuencial por tarjeta/día (1..27), no un ID global.
--     Viajes en efectivo (id_tarjeta IS NULL) se excluyen de facts:
--     no tienen BK único → no se pueden deduplicar individualmente.
--
--     Idempotente: ejecutar N veces sin error.
-- ─────────────────────────────────────────────────────────────

-- fct_trip: drop constraint incorrecto si existe
IF EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'dw.fct_trip')
      AND name = N'UQ_fct_trip_grain'
)
    ALTER TABLE dw.fct_trip DROP CONSTRAINT UQ_fct_trip_grain;

-- fct_trip: índice único filtrado con grain correcto
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fct_trip')
      AND name = N'UX_fct_trip_grain'
)
    CREATE UNIQUE NONCLUSTERED INDEX UX_fct_trip_grain
        ON dw.fct_trip (cut_sk, id_tarjeta, id_viaje)
        WHERE id_tarjeta IS NOT NULL;

-- fct_trip_leg: drop constraint incorrecto si existe
IF EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'dw.fct_trip_leg')
      AND name = N'UQ_fct_trip_leg_grain'
)
    ALTER TABLE dw.fct_trip_leg DROP CONSTRAINT UQ_fct_trip_leg_grain;

-- fct_trip_leg: índice único filtrado con grain correcto
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fct_trip_leg')
      AND name = N'UX_fct_trip_leg_grain'
)
    CREATE UNIQUE NONCLUSTERED INDEX UX_fct_trip_leg_grain
        ON dw.fct_trip_leg (cut_sk, id_tarjeta, id_viaje, leg_seq)
        WHERE id_tarjeta IS NOT NULL;

-- etl_run_log: agregar ignored_cash_rows si la columna no existe
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'dw.etl_run_log')
      AND name = N'ignored_cash_rows'
)
    ALTER TABLE dw.etl_run_log ADD ignored_cash_rows BIGINT NULL;

-- ─────────────────────────────────────────────────────────────
-- 5. ÍNDICES AS-OF PARA SCD2
--    Creados de forma idempotente fuera del bloque IF NOT EXISTS
--    para aplicarse también cuando la tabla ya existe.
--
--    Patrón as-of:
--      JOIN dim_stop ON stop_code = X
--        AND valid_from <= event_date
--        AND (valid_to IS NULL OR event_date <= valid_to)
--    El índice cubre (stop_code, valid_from, valid_to) e INCLUDE stop_sk
--    para un index seek puro sin Key Lookup.
-- ─────────────────────────────────────────────────────────────

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.dim_stop')
      AND name = N'IX_dim_stop_asof'
)
    CREATE NONCLUSTERED INDEX IX_dim_stop_asof
        ON dw.dim_stop (stop_code, valid_from, valid_to)
        INCLUDE (stop_sk);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.dim_service')
      AND name = N'IX_dim_service_asof'
)
    CREATE NONCLUSTERED INDEX IX_dim_service_asof
        ON dw.dim_service (service_code, valid_from, valid_to)
        INCLUDE (service_sk);

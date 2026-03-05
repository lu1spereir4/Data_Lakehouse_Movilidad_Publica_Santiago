-- =============================================================================
-- ddl_sqlite.sql  —  Capa Gold portable SQLite  (DTPM Movilidad Santiago)
-- Motor: SQLite 3.37+
-- Notas:
--   - Sin schemas; todos los objetos en la misma DB.
--   - FKs requieren PRAGMA foreign_keys = ON en cada conexión.
--   - SCD2: dim_stop, dim_service (valid_from/valid_to/is_current/row_hash).
--   - NULL en columnas UNIQUE SQLite es distinto de NULL → comportamiento
--     diferente a SQL Server; documentado en los comentarios de cada tabla.
--   - Índices nombrados; idempotentes vía CREATE INDEX IF NOT EXISTS.
-- =============================================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. DIMENSIONES CONFORMADAS
-- ─────────────────────────────────────────────────────────────────────────────

-- 1.1  dim_cut ─ una fila por (dataset, cut)
CREATE TABLE IF NOT EXISTS dim_cut (
    cut_sk       INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset      TEXT    NOT NULL,
    cut          TEXT    NOT NULL,              -- e.g. "2025-04-21"
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    extracted_at TEXT    NULL,                  -- ISO8601 desde quality.json
    loaded_at    TEXT    NOT NULL,
    UNIQUE(dataset, cut)
);

-- 1.2  dim_date ─ calendario (date_sk = YYYYMMDD integer)
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk      INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date    TEXT    NOT NULL UNIQUE,        -- 'YYYY-MM-DD'
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    day          INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,              -- 0=Mon … 6=Sun (Python weekday())
    day_name     TEXT    NOT NULL,              -- 'Lunes' … 'Domingo'
    month_name   TEXT    NOT NULL,
    is_weekend   INTEGER NOT NULL DEFAULT 0     -- 0/1
);

-- 1.3  dim_time_30m ─ 48 franjas horarias (time_30m_sk = 0..47)
CREATE TABLE IF NOT EXISTS dim_time_30m (
    time_30m_sk  INTEGER PRIMARY KEY,           -- 0..47
    hour         INTEGER NOT NULL,              -- 0..23
    minute       INTEGER NOT NULL,              -- 0 o 30
    period_label TEXT    NOT NULL               -- '00:00-00:30', etc.
);

-- 1.4  dim_mode ─ estática (BUS / METRO / METROTREN / ZP / UNKNOWN)
CREATE TABLE IF NOT EXISTS dim_mode (
    mode_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
    mode_code    TEXT    NOT NULL UNIQUE,
    mode_name    TEXT    NOT NULL
);

-- 1.5  dim_stop ─ SCD2 por paradero
--   Atributos detectados en los parquets:
--     stop_code : viajes_trip.paradero_*, viajes_leg.board/alight_stop_code,
--                 etapas.parada_*, subidas_30m.stop_code
--     comuna    : viajes_trip.comuna_*, etapas.comuna_*, subidas_30m.comuna
--     zona      : viajes_trip.zona_*, viajes_leg.zone_*, etapas.zona_*
--                 (TODO: subidas_30m no tiene zona → NULL)
--   NOTA SQLite: UNIQUE(stop_code, valid_from) ─ valid_from nunca es NULL
--                por construcción, por lo que el UNIQUE es fiable.
CREATE TABLE IF NOT EXISTS dim_stop (
    stop_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
    stop_code    TEXT    NOT NULL,
    comuna       TEXT    NULL,
    zona         INTEGER NULL,                  -- zona tarifaria
    valid_from   TEXT    NOT NULL,              -- ISO8601 date 'YYYY-MM-DD'
    valid_to     TEXT    NULL,                  -- NULL = vigente
    is_current   INTEGER NOT NULL DEFAULT 1,    -- 0/1
    row_hash     TEXT    NOT NULL,
    UNIQUE(stop_code, valid_from)
);

-- 1.6  dim_service ─ SCD2 por servicio
--   Atributos detectados:
--     service_code : viajes_leg.service_code, etapas.servicio_subida/bajada
--     mode_code    : viajes_leg.mode_code; etapas.tipo_transporte (para subida)
--                    (TODO: servicio_bajada en etapas no tiene modo asociado → NULL)
CREATE TABLE IF NOT EXISTS dim_service (
    service_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
    service_code TEXT    NOT NULL,
    mode_code    TEXT    NULL,
    valid_from   TEXT    NOT NULL,
    valid_to     TEXT    NULL,
    is_current   INTEGER NOT NULL DEFAULT 1,
    row_hash     TEXT    NOT NULL,
    UNIQUE(service_code, valid_from)
);

-- 1.7  dim_fare_period ─ períodos tarifarios
--   Fuentes: viajes_trip.periodo_inicio/fin_viaje,
--            viajes_leg.fare_period_alight_code,
--            etapas.periodoSubida/periodoBajada
CREATE TABLE IF NOT EXISTS dim_fare_period (
    fare_period_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
    fare_period_code TEXT    NOT NULL UNIQUE
);

-- 1.8  dim_purpose ─ propósito de viaje
--   Fuente: viajes_trip.proposito
CREATE TABLE IF NOT EXISTS dim_purpose (
    purpose_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose_code TEXT    NOT NULL UNIQUE
);

-- 1.9  dim_operator_contract ─ operador + contrato (grain natural compuesto)
--   Fuentes: viajes_trip.contrato, etapas.operador + etapas.contrato,
--            viajes_leg.operator_code
--   NOTA: operator_code puede ser independiente del contrato en viajes_leg;
--         si no hay contrato, contract_code = 'UNKNOWN'.
CREATE TABLE IF NOT EXISTS dim_operator_contract (
    operator_contract_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_code        TEXT    NOT NULL,
    contract_code        TEXT    NOT NULL,
    UNIQUE(operator_code, contract_code)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. TABLAS DE HECHO
-- ─────────────────────────────────────────────────────────────────────────────

-- 2.1  fct_trip ─ grano: 1 viaje (viajes_trip.parquet)
--   UNIQUE grain: (cut, id_viaje)
--   Idempotencia: INSERT OR IGNORE sobre UNIQUE key.
--   FKs a dim_stop son NULLABLE (stop no resuelto → NULL).
CREATE TABLE IF NOT EXISTS fct_trip (
    trip_sk              INTEGER PRIMARY KEY AUTOINCREMENT,
    cut_sk               INTEGER NOT NULL REFERENCES dim_cut(cut_sk),
    date_start_sk        INTEGER NULL     REFERENCES dim_date(date_sk),
    time_start_30m_sk    INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    date_end_sk          INTEGER NULL     REFERENCES dim_date(date_sk),
    time_end_30m_sk      INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    origin_stop_sk       INTEGER NULL     REFERENCES dim_stop(stop_sk),
    dest_stop_sk         INTEGER NULL     REFERENCES dim_stop(stop_sk),
    purpose_sk           INTEGER NULL     REFERENCES dim_purpose(purpose_sk),
    operator_contract_sk INTEGER NULL     REFERENCES dim_operator_contract(operator_contract_sk),
    -- medidas y atributos degenerados
    cut                  TEXT    NOT NULL,
    id_viaje             TEXT    NOT NULL,
    id_tarjeta           TEXT    NULL,
    tipo_dia             TEXT    NULL,
    factor_expansion     REAL    NULL,
    n_etapas             INTEGER NULL,
    distancia_eucl       REAL    NULL,
    distancia_ruta       REAL    NULL,
    tviaje_min           REAL    NULL,
    -- grain UNIQUE
    UNIQUE(cut, id_viaje)
);

-- 2.2  fct_trip_leg ─ grano: 1 etapa de viaje (viajes_leg.parquet)
--   UNIQUE grain: (cut, id_viaje, leg_seq)
CREATE TABLE IF NOT EXISTS fct_trip_leg (
    leg_sk               INTEGER PRIMARY KEY AUTOINCREMENT,
    cut_sk               INTEGER NOT NULL REFERENCES dim_cut(cut_sk),
    date_board_sk        INTEGER NULL     REFERENCES dim_date(date_sk),
    time_board_30m_sk    INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    date_alight_sk       INTEGER NULL     REFERENCES dim_date(date_sk),
    time_alight_30m_sk   INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    mode_sk              INTEGER NULL     REFERENCES dim_mode(mode_sk),
    service_sk           INTEGER NULL     REFERENCES dim_service(service_sk),
    board_stop_sk        INTEGER NULL     REFERENCES dim_stop(stop_sk),
    alight_stop_sk       INTEGER NULL     REFERENCES dim_stop(stop_sk),
    fare_period_sk       INTEGER NULL     REFERENCES dim_fare_period(fare_period_sk),
    -- atributos degenerados
    cut                  TEXT    NOT NULL,
    id_viaje             TEXT    NOT NULL,
    id_tarjeta           TEXT    NULL,
    leg_seq              INTEGER NOT NULL,
    operator_code        TEXT    NULL,
    zone_board           INTEGER NULL,
    zone_alight          INTEGER NULL,
    tv_leg_min           REAL    NULL,
    tc_transfer_min      REAL    NULL,
    te_wait_min          REAL    NULL,
    UNIQUE(cut, id_viaje, leg_seq)
);

-- 2.3  fct_validation ─ grano: 1 etapa de transacción (etapas_validation.parquet)
--   UNIQUE grain: (cut, id_etapa)
CREATE TABLE IF NOT EXISTS fct_validation (
    validation_sk          INTEGER PRIMARY KEY AUTOINCREMENT,
    cut_sk                 INTEGER NOT NULL REFERENCES dim_cut(cut_sk),
    date_board_sk          INTEGER NULL     REFERENCES dim_date(date_sk),
    time_board_30m_sk      INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    date_alight_sk         INTEGER NULL     REFERENCES dim_date(date_sk),
    time_alight_30m_sk     INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    board_stop_sk          INTEGER NULL     REFERENCES dim_stop(stop_sk),
    alight_stop_sk         INTEGER NULL     REFERENCES dim_stop(stop_sk),
    board_service_sk       INTEGER NULL     REFERENCES dim_service(service_sk),
    alight_service_sk      INTEGER NULL     REFERENCES dim_service(service_sk),
    fare_period_board_sk   INTEGER NULL     REFERENCES dim_fare_period(fare_period_sk),
    fare_period_alight_sk  INTEGER NULL     REFERENCES dim_fare_period(fare_period_sk),
    operator_contract_sk   INTEGER NULL     REFERENCES dim_operator_contract(operator_contract_sk),
    -- atributos degenerados
    cut                    TEXT    NOT NULL,
    id_etapa               TEXT    NOT NULL,
    tipo_dia               TEXT    NULL,
    tipo_transporte        TEXT    NULL,
    factor_expansion       REAL    NULL,    -- fExpansionServicioPeriodoTS en parquet
    tiene_bajada           INTEGER NULL,    -- BOOLEAN mapeado a 0/1
    tiempo_etapa           INTEGER NULL,
    dist_ruta_paraderos    INTEGER NULL,
    dist_eucl_paraderos    INTEGER NULL,
    t_espera_media         REAL    NULL,    -- tEsperaMediaIntervalo en parquet
    UNIQUE(cut, id_etapa)
);

-- 2.4  fct_boardings_30m ─ grano: (cut, month_date_sk, time_30m_sk, stop_code, mode_code, tipo_dia)
--   NOTA SQLite: la grain UNIQUE usa stop_code y mode_code (TEXT) en vez de
--   sus SKs para evitar el problema de NULL en UNIQUE de SQLite.
--   Los SKs se almacenan como medidas lookupadas (pueden ser NULL si miss).
CREATE TABLE IF NOT EXISTS fct_boardings_30m (
    boarding_sk    INTEGER PRIMARY KEY AUTOINCREMENT,
    cut_sk         INTEGER NOT NULL REFERENCES dim_cut(cut_sk),
    time_30m_sk    INTEGER NULL     REFERENCES dim_time_30m(time_30m_sk),
    stop_sk        INTEGER NULL     REFERENCES dim_stop(stop_sk),
    mode_sk        INTEGER NULL     REFERENCES dim_mode(mode_sk),
    -- grain degenerado (natural keys para UNIQUE fiable con SQLite)
    cut            TEXT    NOT NULL,
    month_date_sk  INTEGER NOT NULL,           -- YYYYMM01
    stop_code      TEXT    NOT NULL,
    mode_code      TEXT    NOT NULL,
    tipo_dia       TEXT    NOT NULL,
    -- medidas
    subidas_promedio REAL  NULL,
    UNIQUE(cut, month_date_sk, stop_code, mode_code, tipo_dia, time_30m_sk)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. ÍNDICES
-- ─────────────────────────────────────────────────────────────────────────────

-- dim_stop: as-of lookup (stop_code + valid_from DESC para MAX())
CREATE INDEX IF NOT EXISTS ix_dim_stop_asof
    ON dim_stop(stop_code, valid_from DESC);

CREATE INDEX IF NOT EXISTS ix_dim_stop_current
    ON dim_stop(stop_code, is_current);

-- dim_service: as-of lookup
CREATE INDEX IF NOT EXISTS ix_dim_service_asof
    ON dim_service(service_code, valid_from DESC);

CREATE INDEX IF NOT EXISTS ix_dim_service_current
    ON dim_service(service_code, is_current);

-- fct_trip: búsqueda por cut
CREATE INDEX IF NOT EXISTS ix_fct_trip_cut
    ON fct_trip(cut, date_start_sk);

-- fct_trip_leg: búsqueda por cut + viaje
CREATE INDEX IF NOT EXISTS ix_fct_trip_leg_viaje
    ON fct_trip_leg(cut, id_viaje);

-- fct_trip_leg: análisis por servicio
CREATE INDEX IF NOT EXISTS ix_fct_trip_leg_service
    ON fct_trip_leg(service_sk, date_board_sk);

-- fct_validation: búsqueda por cut
CREATE INDEX IF NOT EXISTS ix_fct_validation_cut
    ON fct_validation(cut, date_board_sk);

-- fct_boardings_30m: análisis por parada
CREATE INDEX IF NOT EXISTS ix_fct_boardings_stop
    ON fct_boardings_30m(stop_sk, cut);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. TABLA DE AUDIT (cargada por load_sqlite.py)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at  TEXT    NULL,
    dataset      TEXT    NOT NULL,
    cut          TEXT    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'RUNNING',   -- RUNNING / OK / FAILED
    rows_read    INTEGER NULL,
    rows_inserted INTEGER NULL,
    rows_ignored  INTEGER NULL,
    error_message TEXT   NULL,
    loader_version TEXT  NULL
);

CREATE INDEX IF NOT EXISTS ix_etl_run_log_dataset
    ON etl_run_log(dataset, cut, started_at DESC);

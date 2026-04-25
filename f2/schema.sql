-- Schema productivo para historico-congreso-union
-- SQLite portable — usa solo features estándar de SQLite.
-- Uso: ejecutar vía db_init.py o sqlite3 directamente.

-- ---------------------------------------------------------------------------
-- Tabla central de almacenamiento raw
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_asset (
    asset_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_tag             TEXT NOT NULL,  -- Sin CHECK: se acepta cualquier string para soportar nuevas fuentes dinámicamente
    url                    TEXT NOT NULL,
    method                 TEXT NOT NULL DEFAULT 'GET'
                             CHECK(method IN ('GET', 'POST')),
    request_payload_hash   TEXT,
    response_body_hash     TEXT NOT NULL,
    response_headers_hash  TEXT,
    status_code            INTEGER,
    content_type           TEXT,
    encoding               TEXT,
    captured_at            TEXT,  -- ISO 8601 timestamp
    waf_detected           INTEGER NOT NULL DEFAULT 0
                             CHECK(waf_detected IN (0, 1)),
    cache_detected         INTEGER NOT NULL DEFAULT 0
                             CHECK(cache_detected IN (0, 1)),
    repetition_num         INTEGER NOT NULL DEFAULT 1,
    run_id                 TEXT,
    raw_body_path          TEXT NOT NULL,

    UNIQUE(source_tag, url, response_body_hash)
);

-- ---------------------------------------------------------------------------
-- Votación individual oficial (unidad mínima)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_vote_event (
    vote_event_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    chamber        TEXT NOT NULL
                       CHECK(chamber IN ('diputados', 'senado')),
    legislature    TEXT NOT NULL
                       CHECK(legislature IN ('LXVI','LXV','LXIV','LXIII','LXII','LXI','LX')),
    vote_date      TEXT,  -- ISO 8601 date (nullable)
    title          TEXT,
    subject        TEXT,
    source_url     TEXT,
    metadata_json  TEXT,  -- JSON/Text con metadata cruda
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(chamber, legislature, source_url)
);

-- ---------------------------------------------------------------------------
-- Unión many-to-many entre votación y asset
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vote_event_asset (
    vote_event_id  INTEGER NOT NULL
                       REFERENCES raw_vote_event(vote_event_id)
                       ON DELETE CASCADE,
    asset_id       INTEGER NOT NULL
                       REFERENCES source_asset(asset_id)
                       ON DELETE CASCADE,
    asset_role     TEXT NOT NULL
                       CHECK(asset_role IN (
                           'primary_nominal',
                           'primary_aggregate',
                           'metadata',
                           'triangulation'
                       )),

    PRIMARY KEY (vote_event_id, asset_id)
);

-- ---------------------------------------------------------------------------
-- Voto nominal individual (condicional por fuente)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_vote_cast (
    cast_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    vote_event_id      INTEGER NOT NULL
                           REFERENCES raw_vote_event(vote_event_id)
                           ON DELETE CASCADE,
    asset_id           INTEGER NOT NULL
                           REFERENCES source_asset(asset_id)
                           ON DELETE CASCADE,
    legislator_name    TEXT NOT NULL,
    legislator_group   TEXT,
    sentido            TEXT NOT NULL
                           CHECK(sentido IN (
                               'a_favor',
                               'en_contra',
                               'abstencion',
                               'ausente',
                               'novoto',
                               'presente'
                           )),
    region             TEXT,
    raw_row_json       TEXT,

    UNIQUE(vote_event_id, asset_id, legislator_name)
);

-- ---------------------------------------------------------------------------
-- Agregados por grupo o totales
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vote_counts (
    count_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    vote_event_id  INTEGER NOT NULL
                       REFERENCES raw_vote_event(vote_event_id)
                       ON DELETE CASCADE,
    asset_id       INTEGER NOT NULL
                       REFERENCES source_asset(asset_id)
                       ON DELETE CASCADE,
    group_name     TEXT,
    a_favor        INTEGER NOT NULL DEFAULT 0,
    en_contra      INTEGER NOT NULL DEFAULT 0,
    abstencion     INTEGER NOT NULL DEFAULT 0,
    ausente        INTEGER NOT NULL DEFAULT 0,
    novoto         INTEGER NOT NULL DEFAULT 0,
    presente       INTEGER NOT NULL DEFAULT 0,
    total          INTEGER,

    UNIQUE(vote_event_id, asset_id, group_name)
);

-- ---------------------------------------------------------------------------
-- Índices obligatorios
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_source_asset_tag   ON source_asset(source_tag);
CREATE INDEX IF NOT EXISTS idx_source_asset_url   ON source_asset(url);
CREATE INDEX IF NOT EXISTS idx_vote_event_chamber ON raw_vote_event(chamber);
CREATE INDEX IF NOT EXISTS idx_vote_event_legislature ON raw_vote_event(legislature);
CREATE INDEX IF NOT EXISTS idx_vote_event_date    ON raw_vote_event(vote_date);
CREATE INDEX IF NOT EXISTS idx_vote_event_asset_asset ON vote_event_asset(asset_id);
CREATE INDEX IF NOT EXISTS idx_vote_cast_event    ON raw_vote_cast(vote_event_id);
CREATE INDEX IF NOT EXISTS idx_vote_cast_asset    ON raw_vote_cast(asset_id);
CREATE INDEX IF NOT EXISTS idx_vote_counts_event  ON vote_counts(vote_event_id);
CREATE INDEX IF NOT EXISTS idx_vote_counts_asset  ON vote_counts(asset_id);

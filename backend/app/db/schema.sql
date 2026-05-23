-- PostgreSQL schema for MOEX News Assistant
-- Tables are ordered to satisfy FK dependencies (event_clusters before seen_articles).


-- ─── rss_sources ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rss_sources (
    id              SERIAL  PRIMARY KEY,
    name            TEXT    NOT NULL UNIQUE,
    url             TEXT    NOT NULL UNIQUE,
    enabled         INTEGER NOT NULL DEFAULT 1,

    -- 'rss' or 'telegram'
    source_type     TEXT    NOT NULL DEFAULT 'rss',

    -- HTTP conditional GET (RSS only)
    etag            TEXT,
    last_modified   TEXT,

    -- Telegram: last seen message ID (telegram only)
    tg_last_msg_id  INTEGER NOT NULL DEFAULT 0,

    -- backoff
    error_count     INTEGER NOT NULL DEFAULT 0,
    last_error_at   TEXT,
    next_retry_at   TEXT,
    status          TEXT    NOT NULL DEFAULT 'ok'
        CHECK (status IN ('ok', 'backoff', 'dead')),

    last_fetched_at TEXT,
    created_at      TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);


-- ─── event_clusters ────────────────────────────────────────────────────────────
-- Defined before seen_articles due to FK reference from seen_articles.cluster_id.
CREATE TABLE IF NOT EXISTS event_clusters (
    id              SERIAL  PRIMARY KEY,

    -- anchor data (set from first article, never updated)
    canonical_title TEXT    NOT NULL,
    title_tokens    TEXT    NOT NULL,

    -- evolving metadata (updated as more articles arrive)
    keywords        TEXT    NOT NULL,
    best_score      INTEGER NOT NULL DEFAULT 0,
    source_count    INTEGER NOT NULL DEFAULT 1,
    article_count   INTEGER NOT NULL DEFAULT 1,

    -- tickers extracted from article titles (comma-separated, e.g. "GAZP,SBER")
    tickers         TEXT    DEFAULT NULL,

    -- sentence-transformer embedding of canonical_title (float32 bytes, L2-normalised)
    embedding       BYTEA   DEFAULT NULL,

    -- publish decision state
    status          TEXT    NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'published', 'updated', 'silenced')),

    first_seen_at   TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    last_updated_at TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    last_sent_at    TEXT,
    cooldown_until  TEXT,
    published_score INTEGER
);

CREATE INDEX IF NOT EXISTS idx_clusters_status          ON event_clusters(status);
CREATE INDEX IF NOT EXISTS idx_clusters_last_updated_at ON event_clusters(last_updated_at);


-- ─── pg_trgm extension (required for GIN trigram index on title_tokens) ────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- ─── seen_articles ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS seen_articles (
    id          SERIAL  PRIMARY KEY,
    source_id   INTEGER NOT NULL REFERENCES rss_sources(id) ON DELETE CASCADE,

    raw_hash        TEXT    NOT NULL UNIQUE,
    title_tokens    TEXT    NOT NULL,
    url             TEXT,
    published_at    TEXT    NOT NULL,

    cluster_id  INTEGER REFERENCES event_clusters(id) ON DELETE SET NULL,

    seen_at     TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

CREATE INDEX IF NOT EXISTS idx_seen_raw_hash   ON seen_articles(raw_hash);
CREATE INDEX IF NOT EXISTS idx_seen_seen_at    ON seen_articles(seen_at);
CREATE INDEX IF NOT EXISTS idx_seen_cluster_id ON seen_articles(cluster_id);
-- GIN trigram index for near-dedup candidate pre-filtering (replaces O(N) Python scan)
CREATE INDEX IF NOT EXISTS idx_seen_title_trgm ON seen_articles USING GIN (title_tokens gin_trgm_ops);


-- ─── telegram_sends ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telegram_sends (
    id          SERIAL  PRIMARY KEY,
    cluster_id  INTEGER NOT NULL REFERENCES event_clusters(id) ON DELETE CASCADE,

    decision    TEXT    NOT NULL CHECK (decision IN ('NEW_EVENT', 'UPDATE')),
    score       INTEGER NOT NULL,
    source_count INTEGER NOT NULL,
    headline    TEXT    NOT NULL,
    tg_message_id INTEGER,
    ok          INTEGER NOT NULL DEFAULT 1,
    error_text  TEXT,

    sent_at     TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

CREATE INDEX IF NOT EXISTS idx_sends_sent_at    ON telegram_sends(sent_at);
CREATE INDEX IF NOT EXISTS idx_sends_cluster_id ON telegram_sends(cluster_id);


-- ─── digest_sends ──────────────────────────────────────────────────────────────
-- One row per digest label+date. UNIQUE prevents double-send from concurrent
-- service instances (e.g. Docker + local uvicorn running simultaneously).
CREATE TABLE IF NOT EXISTS digest_sends (
    id        SERIAL PRIMARY KEY,
    label     TEXT   NOT NULL,
    sent_date TEXT   NOT NULL,
    sent_at   TEXT   NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    UNIQUE (label, sent_date)
);


-- ─── portfolio_subscriptions ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_subscriptions (
    id         SERIAL  PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    ticker     TEXT    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    UNIQUE(user_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_user_id ON portfolio_subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_ticker  ON portfolio_subscriptions(ticker);


-- ─── users ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id          SERIAL  PRIMARY KEY,
    telegram_id BIGINT  NOT NULL UNIQUE,
    username    TEXT,
    first_name  TEXT    NOT NULL DEFAULT '',
    plan        TEXT    NOT NULL DEFAULT 'free'
        CHECK (plan IN ('free', 'pro')),
    created_at  TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);


-- ─── subscriptions ─────────────────────────────────────────────────────────────
-- Tracks paid subscription billing cycles.
-- users.plan is the authoritative plan gate — this table manages expiry and payment history.
CREATE TABLE IF NOT EXISTS subscriptions (
    id          SERIAL  PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan        TEXT    NOT NULL CHECK (plan IN ('monthly', 'annual')),
    status      TEXT    NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'grace', 'expired')),
    expires_at  TEXT    NOT NULL,
    payment_id  TEXT    UNIQUE,
    created_at  TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

CREATE INDEX IF NOT EXISTS idx_subs_user_id    ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_subs_status     ON subscriptions(status);
CREATE INDEX IF NOT EXISTS idx_subs_expires_at ON subscriptions(expires_at);


-- ─── user_settings ─────────────────────────────────────────────────────────────
-- One row per user, created lazily on first write. Row may not exist — callers
-- treat a missing row as all-defaults (min_score=30, quiet hours off, no filter).
CREATE TABLE IF NOT EXISTS user_settings (
    id           SERIAL  PRIMARY KEY,
    user_id      INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    min_score    INTEGER NOT NULL DEFAULT 30
        CHECK (min_score BETWEEN 0 AND 100),
    quiet_from   INTEGER CHECK (quiet_from IS NULL OR quiet_from BETWEEN 0 AND 23),
    quiet_to     INTEGER CHECK (quiet_to   IS NULL OR quiet_to   BETWEEN 0 AND 23),
    updated_at   TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    calendar_notify_days_before INTEGER NOT NULL DEFAULT 3
        CHECK (calendar_notify_days_before BETWEEN 1 AND 30)
);

CREATE INDEX IF NOT EXISTS idx_user_settings_user_id ON user_settings(user_id);


-- ─── corporate_events ─────────────────────────────────────────────────────────
-- Synced from MOEX ISS API daily. One row per (ticker, event_type, event_date).
-- event_type values: 'dividend_cutoff' | 'dividend_payment' | 'earnings' | 'buyback' | 'offer'
-- details JSONB shape:
--   dividend_cutoff / payment : {amount: 33.58, currency: "RUB"}
--   earnings                  : {report_type: "МСФО"}
--   buyback / offer           : {price: 250.0}
CREATE TABLE IF NOT EXISTS corporate_events (
    id          SERIAL  PRIMARY KEY,
    ticker      TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    event_date  DATE    NOT NULL,
    details     JSONB,
    source      TEXT    NOT NULL DEFAULT 'moex_iss',
    synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(ticker, event_type, event_date)
);
-- Composite index covers the dominant query: WHERE ticker = ANY($1) AND event_date BETWEEN $2 AND $3
CREATE INDEX IF NOT EXISTS idx_corp_events_ticker_date ON corporate_events(ticker, event_date);
CREATE INDEX IF NOT EXISTS idx_corp_events_date        ON corporate_events(event_date);


-- ─── calendar_notifications_sent ──────────────────────────────────────────────
-- Dedup guard: one row per (user, event) once the DM is sent.
CREATE TABLE IF NOT EXISTS calendar_notifications_sent (
    id       SERIAL  PRIMARY KEY,
    user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_id INTEGER NOT NULL REFERENCES corporate_events(id) ON DELETE CASCADE,
    sent_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_cal_notif_user ON calendar_notifications_sent(user_id);


-- ─── price_snapshots ───────────────────────────────────────────────────────────
-- Records MOEX prices at publish time and 1h/24h later for historical correlation.
CREATE TABLE IF NOT EXISTS price_snapshots (
    id               SERIAL PRIMARY KEY,
    cluster_id       INTEGER NOT NULL REFERENCES event_clusters(id) ON DELETE CASCADE,
    ticker           TEXT    NOT NULL,
    event_type       TEXT    NOT NULL,
    published_at     TEXT    NOT NULL,

    price_at_publish REAL,
    price_1h         REAL,
    snapshot_1h_at   TEXT,
    price_24h        REAL,
    snapshot_24h_at  TEXT,

    created_at       TEXT    NOT NULL DEFAULT to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
);

CREATE INDEX IF NOT EXISTS idx_price_snap_cluster  ON price_snapshots(cluster_id);
CREATE INDEX IF NOT EXISTS idx_price_snap_evt_tick ON price_snapshots(event_type, ticker);
CREATE INDEX IF NOT EXISTS idx_price_snap_pub_at   ON price_snapshots(published_at);

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;


-- ─── rss_sources ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rss_sources (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    url         TEXT    NOT NULL UNIQUE,
    enabled     INTEGER NOT NULL DEFAULT 1,

    -- HTTP conditional GET
    etag            TEXT,
    last_modified   TEXT,

    -- backoff
    error_count     INTEGER NOT NULL DEFAULT 0,
    last_error_at   TEXT,
    next_retry_at   TEXT,
    status          TEXT NOT NULL DEFAULT 'ok'
        CHECK (status IN ('ok', 'backoff', 'dead')),

    last_fetched_at TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);


-- ─── seen_articles ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS seen_articles (
    id          INTEGER PRIMARY KEY,
    source_id   INTEGER NOT NULL REFERENCES rss_sources(id) ON DELETE CASCADE,

    raw_hash        TEXT NOT NULL UNIQUE,
    title_tokens    TEXT NOT NULL,
    url             TEXT,
    published_at    TEXT NOT NULL,

    cluster_id  INTEGER REFERENCES event_clusters(id) ON DELETE SET NULL,

    seen_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_seen_raw_hash   ON seen_articles(raw_hash);
CREATE INDEX IF NOT EXISTS idx_seen_seen_at    ON seen_articles(seen_at);
CREATE INDEX IF NOT EXISTS idx_seen_cluster_id ON seen_articles(cluster_id);


-- ─── event_clusters ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_clusters (
    id              INTEGER PRIMARY KEY,

    -- anchor data (set from first article, never updated)
    canonical_title TEXT    NOT NULL,   -- display title of the event
    title_tokens    TEXT    NOT NULL,   -- sorted tokens of anchor article for matching

    -- evolving metadata (updated as more articles arrive)
    keywords        TEXT    NOT NULL,   -- union of significant tokens across all articles
    best_score      INTEGER NOT NULL DEFAULT 0,
    source_count    INTEGER NOT NULL DEFAULT 1,
    article_count   INTEGER NOT NULL DEFAULT 1,

    -- publish decision state
    status          TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'published', 'updated', 'silenced')),

    first_seen_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_sent_at    TEXT,
    cooldown_until  TEXT,
    published_score INTEGER
);

CREATE INDEX IF NOT EXISTS idx_clusters_status          ON event_clusters(status);
CREATE INDEX IF NOT EXISTS idx_clusters_last_updated_at ON event_clusters(last_updated_at);


-- ─── telegram_sends ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telegram_sends (
    id          INTEGER PRIMARY KEY,
    cluster_id  INTEGER NOT NULL REFERENCES event_clusters(id) ON DELETE CASCADE,

    decision    TEXT NOT NULL CHECK (decision IN ('NEW_EVENT', 'UPDATE')),
    score       INTEGER NOT NULL,
    source_count INTEGER NOT NULL,
    headline    TEXT NOT NULL,
    tg_message_id INTEGER,
    ok          INTEGER NOT NULL DEFAULT 1,
    error_text  TEXT,

    sent_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_sends_sent_at    ON telegram_sends(sent_at);
CREATE INDEX IF NOT EXISTS idx_sends_cluster_id ON telegram_sends(cluster_id);

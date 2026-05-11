"""
All SQL operations in one place.
Each function receives an open sqlite3.Connection and is responsible
for committing only what it touches. The caller owns the connection lifecycle.
"""

import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── seed data ─────────────────────────────────────────────────────────────────

_RSS_SEEDS = [
    # ── tier 1: financial news agencies ──────────────────────────────────────
    ("TASS",       "https://tass.ru/rss/v2.xml"),
    ("Interfax",   "https://www.interfax.ru/rss"),
    ("Prime",      "https://1prime.ru/export/rss2/index.xml"),
    ("RIA",        "https://ria.ru/export/rss2/index.xml"),

    # ── tier 2: business newspapers ──────────────────────────────────────────
    ("Vedomosti",  "https://www.vedomosti.ru/rss/news"),
    ("Kommersant", "https://www.kommersant.ru/rss/news.xml"),
    ("BFM",        "https://www.bfm.ru/news.rss"),
    ("Izvestia",   "https://iz.ru/xml/rss/finances.xml"),
    ("Gazeta",     "https://www.gazeta.ru/export/rss/business.xml"),

    # ── tier 3: general + market data ────────────────────────────────────────
    ("RG",         "https://rg.ru/xml/index.xml"),            # Rossiyskaya Gazeta
    ("Lenta",      "https://lenta.ru/rss/articles/economics"),
    ("Investing",  "https://ru.investing.com/rss/news.rss"),
    ("MOEX",       "https://www.moex.com/export/news.aspx"),  # exchange bulletins

    # ── tier 4: market community + official policy ────────────────────────────
    ("Smartlab",   "https://smart-lab.ru/news/rss"),          # retail trader news/market events
    ("Government", "http://government.ru/news/rss/"),          # official economic policy decisions

    # ── disabled (unreachable as of 2026-05) ─────────────────────────────────
    # ("RBC",     "https://rbc.ru/rss/news"),           # 404
    # ("Forbes",  "https://www.forbes.ru/rss"),         # 400
    # ("Finam",   "https://www.finam.ru/..."),          # 403
    # ("CBR",     "https://www.cbr.ru/rss/"),           # 404
]

_TG_SEEDS = [
    ("TG:markettwits",   "tg://markettwits"),    # market commentary, popular
    ("TG:russianmacro",  "tg://russianmacro"),   # macro & CBR
    ("TG:cbrstocks",     "tg://cbrstocks"),       # CBR/stocks news
    ("TG:moexnews",      "tg://moexnews"),        # official MOEX channel
]

_BACKOFF_MAX_MINUTES = 120
_DEAD_AFTER_ERRORS   = 10


def seed_sources(db: sqlite3.Connection) -> None:
    db.executemany(
        "INSERT OR IGNORE INTO rss_sources (name, url) VALUES (?, ?)",
        _RSS_SEEDS,
    )
    db.executemany(
        "INSERT OR IGNORE INTO rss_sources (name, url, source_type) VALUES (?, ?, 'telegram')",
        _TG_SEEDS,
    )
    db.commit()
    logger.info("Sources seeded: %d RSS, %d Telegram", len(_RSS_SEEDS), len(_TG_SEEDS))


# ── rss_sources reads ─────────────────────────────────────────────────────────

def get_active_sources(db: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Return RSS sources that are enabled, not dead, and either:
    - have no retry delay (status='ok'), or
    - are in backoff but next_retry_at has passed.
    """
    now = _utcnow_iso()
    return db.execute(
        """
        SELECT *
        FROM   rss_sources
        WHERE  enabled = 1
          AND  source_type = 'rss'
          AND  status  != 'dead'
          AND  (next_retry_at IS NULL OR next_retry_at <= ?)
        ORDER  BY id
        """,
        (now,),
    ).fetchall()


def get_active_tg_channels(db: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return Telegram channels that are enabled and not dead."""
    now = _utcnow_iso()
    return db.execute(
        """
        SELECT *
        FROM   rss_sources
        WHERE  enabled = 1
          AND  source_type = 'telegram'
          AND  status  != 'dead'
          AND  (next_retry_at IS NULL OR next_retry_at <= ?)
        ORDER  BY id
        """,
        (now,),
    ).fetchall()


def update_tg_channel_ok(
    db: sqlite3.Connection,
    source_id: int,
    last_msg_id: int,
) -> None:
    db.execute(
        """
        UPDATE rss_sources
        SET    tg_last_msg_id  = ?,
               last_fetched_at = ?,
               error_count     = 0,
               last_error_at   = NULL,
               next_retry_at   = NULL,
               status          = 'ok'
        WHERE  id = ?
        """,
        (last_msg_id, _utcnow_iso(), source_id),
    )
    db.commit()


# ── rss_sources writes ────────────────────────────────────────────────────────

def update_source_ok(
    db: sqlite3.Connection,
    source_id: int,
    etag: Optional[str],
    last_modified: Optional[str],
) -> None:
    """Called after a successful fetch (200 or 304)."""
    db.execute(
        """
        UPDATE rss_sources
        SET    etag            = ?,
               last_modified   = ?,
               last_fetched_at = ?,
               error_count     = 0,
               last_error_at   = NULL,
               next_retry_at   = NULL,
               status          = 'ok'
        WHERE  id = ?
        """,
        (etag, last_modified, _utcnow_iso(), source_id),
    )
    db.commit()


def update_source_error(db: sqlite3.Connection, source_id: int) -> str:
    """
    Called after a failed fetch.
    Increments error_count, computes exponential next_retry_at, flips status.
    Returns the new status: 'backoff' or 'dead'.
    """
    row = db.execute(
        "SELECT error_count FROM rss_sources WHERE id = ?",
        (source_id,),
    ).fetchone()

    if row is None:
        return "backoff"

    new_count = row["error_count"] + 1
    delay_minutes = min(2 ** new_count, _BACKOFF_MAX_MINUTES)
    next_retry = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
    new_status = "dead" if new_count >= _DEAD_AFTER_ERRORS else "backoff"

    db.execute(
        """
        UPDATE rss_sources
        SET    error_count   = ?,
               last_error_at = ?,
               next_retry_at = ?,
               status        = ?
        WHERE  id = ?
        """,
        (new_count, _utcnow_iso(), _iso(next_retry), new_status, source_id),
    )
    db.commit()

    if new_status == "dead":
        logger.error(
            "Source id=%d marked DEAD after %d consecutive errors",
            source_id,
            new_count,
        )
    else:
        logger.warning(
            "Source id=%d backoff: error_count=%d, retry in %d min",
            source_id,
            new_count,
            delay_minutes,
        )

    return new_status


# ── seen_articles ─────────────────────────────────────────────────────────────

def is_exact_duplicate(db: sqlite3.Connection, raw_hash: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM seen_articles WHERE raw_hash = ?",
        (raw_hash,),
    ).fetchone()
    return row is not None


def get_recent_title_tokens(
    db: sqlite3.Connection,
    within_hours: int = 4,
    limit: int = 1000,
) -> list[str]:
    """
    Returns title_token strings for near-dedup Jaccard check.

    ORDER BY seen_at DESC ensures the most recent articles are checked first,
    which improves early-exit hit rate in _best_jaccard().
    LIMIT caps memory and CPU even under high ingest volume.
    Trade-off: articles older than the limit window may be missed by near-dedup,
    but exact-dedup (raw_hash) still catches them.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    rows = db.execute(
        "SELECT title_tokens FROM seen_articles WHERE seen_at >= ? ORDER BY seen_at DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    return [r["title_tokens"] for r in rows]


def insert_seen_article(
    db: sqlite3.Connection,
    source_id: int,
    raw_hash: str,
    title_tokens: str,
    url: Optional[str],
    published_at: str,
    cluster_id: Optional[int] = None,
    *,
    commit: bool = True,
) -> Optional[int]:
    """INSERT OR IGNORE — idempotent on restart. Returns rowid or None if duplicate."""
    cur = db.execute(
        """
        INSERT OR IGNORE INTO seen_articles
            (source_id, raw_hash, title_tokens, url, published_at, cluster_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_id, raw_hash, title_tokens, url, published_at, cluster_id),
    )
    if commit:
        db.commit()
    return cur.lastrowid if cur.rowcount else None


def assign_cluster(
    db: sqlite3.Connection,
    article_id: int,
    cluster_id: int,
) -> None:
    db.execute(
        "UPDATE seen_articles SET cluster_id = ? WHERE id = ?",
        (cluster_id, article_id),
    )
    db.commit()


# ── event_clusters ────────────────────────────────────────────────────────────

def find_candidate_clusters(
    db: sqlite3.Connection,
    within_hours: int = 4,
) -> list[sqlite3.Row]:
    """
    Load clusters whose first_seen_at is within the window.
    Filtering by first_seen_at (not last_updated_at) ensures we still match
    clusters that received no new articles for a while but are still "open".
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    return db.execute(
        """
        SELECT *
        FROM   event_clusters
        WHERE  first_seen_at >= ?
        ORDER  BY first_seen_at DESC
        LIMIT  500
        """,
        (cutoff,),
    ).fetchall()


def get_cluster_source_ids(
    db: sqlite3.Connection,
    cluster_id: int,
) -> set[int]:
    """Return source_ids that have already contributed to this cluster."""
    rows = db.execute(
        "SELECT DISTINCT source_id FROM seen_articles WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchall()
    return {r["source_id"] for r in rows}


def create_cluster(
    db: sqlite3.Connection,
    canonical_title: str,
    title_tokens: str,
    keywords: str,
    score: int,
    tickers: str = "",
    *,
    commit: bool = True,
) -> int:
    cur = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, tickers)
        VALUES (?, ?, ?, ?, ?)
        """,
        (canonical_title, title_tokens, keywords, score, tickers or None),
    )
    if commit:
        db.commit()
    assert cur.lastrowid is not None  # INSERT without OR IGNORE always sets lastrowid
    return cur.lastrowid


def update_cluster(
    db: sqlite3.Connection,
    cluster_id: int,
    score: int,
    new_source: bool,
    merged_keywords: str,
    merged_tickers: str = "",
    *,
    commit: bool = True,
) -> None:
    """
    new_source=True → increment source_count.
    Always increments article_count, updates best_score, keywords, last_updated_at.
    merged_keywords is the caller-computed union of existing + new article tokens.
    merged_tickers is the caller-computed union of tickers across articles.
    """
    db.execute(
        """
        UPDATE event_clusters
        SET    article_count   = article_count + 1,
               source_count    = source_count + ?,
               best_score      = MAX(best_score, ?),
               keywords        = ?,
               tickers         = ?,
               last_updated_at = ?
        WHERE  id = ?
        """,
        (1 if new_source else 0, score, merged_keywords, merged_tickers or None, _utcnow_iso(), cluster_id),
    )
    if commit:
        db.commit()


def mark_cluster_sent(
    db: sqlite3.Connection,
    cluster_id: int,
    decision: str,
    score: int,
    cooldown_hours: int = 2,
) -> None:
    now = datetime.now(timezone.utc)
    cooldown = _iso(now + timedelta(hours=cooldown_hours))
    status = "published" if decision == "NEW_EVENT" else "updated"
    db.execute(
        """
        UPDATE event_clusters
        SET    status          = ?,
               last_sent_at   = ?,
               cooldown_until  = ?,
               published_score = ?
        WHERE  id = ?
        """,
        (status, _iso(now), cooldown, score, cluster_id),
    )
    db.commit()


def get_cluster(
    db: sqlite3.Connection,
    cluster_id: int,
) -> Optional[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM event_clusters WHERE id = ?",
        (cluster_id,),
    ).fetchone()


def get_top_sent_clusters(
    db: sqlite3.Connection,
    within_hours: int = 24,
    limit: int = 10,
) -> list[sqlite3.Row]:
    """
    Return the top published clusters for the daily digest.

    Selects clusters that were actually sent (last_sent_at is set) within the
    window, ordered by (best_score * source_count) descending — the same signal
    the publisher uses to decide importance.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    return db.execute(
        """
        SELECT id, canonical_title, best_score, source_count,
               tickers, keywords, title_tokens, last_sent_at
        FROM   event_clusters
        WHERE  last_sent_at >= ?
          AND  status IN ('published', 'updated')
        ORDER  BY best_score * source_count DESC
        LIMIT  ?
        """,
        (cutoff, limit),
    ).fetchall()


# ── telegram_sends ────────────────────────────────────────────────────────────

def has_recent_send_attempt(
    db: sqlite3.Connection,
    cluster_id: int,
    within_hours: int = 2,
) -> bool:
    """
    Return True if any telegram_send row exists for this cluster within the window.

    Includes failed sends (ok=0) because a timed-out HTTP request may have been
    delivered by Telegram even though our code received no response. Checking
    failed attempts prevents the retry-on-next-cycle duplicate pattern.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    row = db.execute(
        "SELECT 1 FROM telegram_sends WHERE cluster_id = ? AND sent_at >= ? LIMIT 1",
        (cluster_id, cutoff),
    ).fetchone()
    return row is not None


def get_recently_sent_title_tokens(
    db: sqlite3.Connection,
    within_hours: int = 2,
    exclude_cluster_id: Optional[int] = None,
) -> list[str]:
    """
    Return title_tokens for clusters that had a SUCCESSFUL send within the window.

    Used for cross-cluster duplicate detection: two different clusters may represent
    the same event when sources use wording different enough to escape both near-dedup
    (Jaccard < threshold) and containment clustering (containment < threshold).
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    if exclude_cluster_id is not None:
        rows = db.execute(
            """
            SELECT DISTINCT ec.title_tokens
            FROM   telegram_sends ts
            JOIN   event_clusters ec ON ts.cluster_id = ec.id
            WHERE  ts.sent_at >= ? AND ts.ok = 1 AND ts.cluster_id != ?
            """,
            (cutoff, exclude_cluster_id),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT DISTINCT ec.title_tokens
            FROM   telegram_sends ts
            JOIN   event_clusters ec ON ts.cluster_id = ec.id
            WHERE  ts.sent_at >= ? AND ts.ok = 1
            """,
            (cutoff,),
        ).fetchall()
    return [r["title_tokens"] for r in rows]


def log_send(
    db: sqlite3.Connection,
    cluster_id: int,
    decision: str,
    score: int,
    source_count: int,
    headline: str,
    tg_message_id: Optional[int] = None,
    ok: bool = True,
    error_text: Optional[str] = None,
) -> None:
    db.execute(
        """
        INSERT INTO telegram_sends
            (cluster_id, decision, score, source_count, headline,
             tg_message_id, ok, error_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cluster_id, decision, score, source_count, headline,
            tg_message_id, 1 if ok else 0, error_text,
        ),
    )
    db.commit()


# ── monitoring ────────────────────────────────────────────────────────────────

def get_last_ok_send_at(db: sqlite3.Connection) -> Optional[datetime]:
    """Return the timestamp of the most recent successful Telegram send, or None."""
    row = db.execute(
        "SELECT sent_at FROM telegram_sends WHERE ok=1 ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row["sent_at"].replace("Z", "+00:00"))


def get_source_stats(db: sqlite3.Connection) -> dict[str, int]:
    """Return counts of enabled sources grouped by status: ok / backoff / dead."""
    rows = db.execute(
        "SELECT status, COUNT(*) AS n FROM rss_sources WHERE enabled = 1 GROUP BY status"
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def get_dead_sources(db: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return all enabled sources currently marked dead."""
    return db.execute(
        "SELECT id, name, url, error_count FROM rss_sources WHERE status = 'dead' AND enabled = 1"
    ).fetchall()


# ── admin: rss_sources CRUD ───────────────────────────────────────────────────

def get_all_sources(db: sqlite3.Connection) -> list[sqlite3.Row]:
    return db.execute(
        "SELECT id, name, url, enabled, status, error_count, last_fetched_at, created_at "
        "FROM rss_sources ORDER BY id"
    ).fetchall()


def add_source(db: sqlite3.Connection, name: str, url: str) -> int:
    """
    Insert a new RSS source. Raises sqlite3.IntegrityError on duplicate name or URL.
    Returns the new row id.
    """
    cur = db.execute(
        "INSERT INTO rss_sources (name, url) VALUES (?, ?)",
        (name, url),
    )
    db.commit()
    assert cur.lastrowid is not None
    return cur.lastrowid


def disable_source(db: sqlite3.Connection, source_id: int) -> bool:
    """Soft-delete: set enabled=0. Returns True if a row was updated."""
    cur = db.execute(
        "UPDATE rss_sources SET enabled = 0 WHERE id = ?",
        (source_id,),
    )
    db.commit()
    return cur.rowcount > 0


def update_source(
    db: sqlite3.Connection,
    source_id: int,
    url: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> bool:
    """
    Update url and/or enabled flag. Returns True if a row was updated.
    At least one of url or enabled must be provided.
    """
    parts: list[str] = []
    params: list = []
    if url is not None:
        parts.append("url = ?")
        params.append(url)
    if enabled is not None:
        parts.append("enabled = ?")
        params.append(1 if enabled else 0)
    if not parts:
        return False
    params.append(source_id)
    cur = db.execute(
        f"UPDATE rss_sources SET {', '.join(parts)} WHERE id = ?",
        params,
    )
    db.commit()
    return cur.rowcount > 0


def reset_source_backoff(db: sqlite3.Connection, source_id: int) -> bool:
    """Reset backoff state so the source is polled on the next cycle."""
    cur = db.execute(
        """
        UPDATE rss_sources
        SET    status        = 'ok',
               error_count   = 0,
               last_error_at = NULL,
               next_retry_at = NULL
        WHERE  id = ?
        """,
        (source_id,),
    )
    db.commit()
    return cur.rowcount > 0


# ── portfolio_subscriptions ───────────────────────────────────────────────────

def get_subscribed_users(db: sqlite3.Connection, tickers: list[str]) -> list[int]:
    """Return user_ids subscribed to any of the given tickers."""
    if not tickers:
        return []
    placeholders = ",".join("?" * len(tickers))
    rows = db.execute(
        f"SELECT DISTINCT user_id FROM portfolio_subscriptions WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return [r["user_id"] for r in rows]


def get_user_tickers(db: sqlite3.Connection, user_id: int) -> list[str]:
    """Return tickers subscribed by a user, sorted alphabetically."""
    rows = db.execute(
        "SELECT ticker FROM portfolio_subscriptions WHERE user_id = ? ORDER BY ticker",
        (user_id,),
    ).fetchall()
    return [r["ticker"] for r in rows]


def set_user_tickers(db: sqlite3.Connection, user_id: int, tickers: list[str]) -> None:
    """Replace a user's subscriptions with the given tickers (idempotent)."""
    with db:
        db.execute(
            "DELETE FROM portfolio_subscriptions WHERE user_id = ?",
            (user_id,),
        )
        db.executemany(
            "INSERT OR IGNORE INTO portfolio_subscriptions (user_id, ticker) VALUES (?, ?)",
            [(user_id, t.upper()) for t in tickers],
        )


def clear_user_tickers(db: sqlite3.Connection, user_id: int) -> None:
    """Remove all subscriptions for a user."""
    db.execute(
        "DELETE FROM portfolio_subscriptions WHERE user_id = ?",
        (user_id,),
    )
    db.commit()


# ── retention ─────────────────────────────────────────────────────────────────

def run_retention(db: sqlite3.Connection) -> None:
    with db:
        db.execute(
            "DELETE FROM seen_articles WHERE seen_at < ?",
            (_iso(datetime.now(timezone.utc) - timedelta(hours=48)),),
        )
        db.execute(
            "DELETE FROM event_clusters WHERE first_seen_at < ?",
            (_iso(datetime.now(timezone.utc) - timedelta(days=7)),),
        )
        # reset backoff entries whose retry window has passed
        db.execute(
            """
            UPDATE rss_sources
            SET    status = 'ok', next_retry_at = NULL
            WHERE  status = 'backoff'
              AND  next_retry_at <= ?
            """,
            (_utcnow_iso(),),
        )
    logger.info("Retention complete")


# ── helpers ───────────────────────────────────────────────────────────────────

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

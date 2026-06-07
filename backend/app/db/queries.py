"""
All SQL operations in one place.
Each function receives an open DBConnection and is responsible
for committing only what it touches. The caller owns the connection lifecycle.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import psycopg2

from app.db.database import DBConnection

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

    # ── tier 5: stock market focused ─────────────────────────────────────────
    ("RBC",        "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"),  # РБК финансы

    # ── disabled (unreachable as of 2026-05) ─────────────────────────────────
    # ("Government",  "http://government.ru/news/rss/"),  # blocks VPS IP
    # ("Forbes",      "https://www.forbes.ru/rss"),       # 400
    # ("Finam",       "https://www.finam.ru/..."),        # 403
    # ("CBR",         "https://www.cbr.ru/rss/"),         # 404
    # ("BCS",         "https://bcs-express.ru/rss"),      # 403
    # ("InvestFunds", "https://investfunds.ru/news/rss/"),# 404
]

_TG_SEEDS = [
    ("TG:markettwits",    "tg://markettwits"),    # market commentary, popular
    ("TG:russianmacro",   "tg://russianmacro"),   # macro & CBR
    ("TG:cbrstocks",      "tg://cbrstocks"),       # CBR/stocks news
    ("TG:moexnews",       "tg://moexnews"),        # official MOEX channel
    ("TG:dohod",          "tg://dohod"),            # ДОХОДЪ — дивиденды и рейтинги
]

_BACKOFF_MAX_MINUTES = 120
_DEAD_AFTER_ERRORS   = 10


def seed_sources(db: DBConnection) -> None:
    db.executemany(
        "INSERT INTO rss_sources (name, url) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        _RSS_SEEDS,
    )
    db.executemany(
        "INSERT INTO rss_sources (name, url, source_type) VALUES (%s, %s, 'telegram') ON CONFLICT DO NOTHING",
        _TG_SEEDS,
    )
    db.commit()
    logger.info("Sources seeded: %d RSS, %d Telegram", len(_RSS_SEEDS), len(_TG_SEEDS))


# ── rss_sources reads ─────────────────────────────────────────────────────────

def get_active_sources(db: DBConnection) -> list[Any]:
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
          AND  (next_retry_at IS NULL OR next_retry_at <= %s)
        ORDER  BY id
        """,
        (now,),
    ).fetchall()


def get_active_tg_channels(db: DBConnection) -> list[Any]:
    """Return Telegram channels that are enabled and not dead."""
    now = _utcnow_iso()
    return db.execute(
        """
        SELECT *
        FROM   rss_sources
        WHERE  enabled = 1
          AND  source_type = 'telegram'
          AND  status  != 'dead'
          AND  (next_retry_at IS NULL OR next_retry_at <= %s)
        ORDER  BY id
        """,
        (now,),
    ).fetchall()


def update_tg_channel_ok(
    db: DBConnection,
    source_id: int,
    last_msg_id: int,
) -> None:
    db.execute(
        """
        UPDATE rss_sources
        SET    tg_last_msg_id  = %s,
               last_fetched_at = %s,
               error_count     = 0,
               last_error_at   = NULL,
               next_retry_at   = NULL,
               status          = 'ok'
        WHERE  id = %s
        """,
        (last_msg_id, _utcnow_iso(), source_id),
    )
    db.commit()


# ── rss_sources writes ────────────────────────────────────────────────────────

def update_source_ok(
    db: DBConnection,
    source_id: int,
    etag: Optional[str],
    last_modified: Optional[str],
) -> None:
    """Called after a successful fetch (200 or 304)."""
    db.execute(
        """
        UPDATE rss_sources
        SET    etag            = %s,
               last_modified   = %s,
               last_fetched_at = %s,
               error_count     = 0,
               last_error_at   = NULL,
               next_retry_at   = NULL,
               status          = 'ok'
        WHERE  id = %s
        """,
        (etag, last_modified, _utcnow_iso(), source_id),
    )
    db.commit()


def update_source_error(db: DBConnection, source_id: int) -> str:
    """
    Called after a failed fetch.
    Increments error_count, computes exponential next_retry_at, flips status.
    Returns the new status: 'backoff' or 'dead'.
    """
    row = db.execute(
        "SELECT error_count FROM rss_sources WHERE id = %s",
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
        SET    error_count   = %s,
               last_error_at = %s,
               next_retry_at = %s,
               status        = %s
        WHERE  id = %s
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

def is_exact_duplicate(db: DBConnection, raw_hash: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM seen_articles WHERE raw_hash = %s",
        (raw_hash,),
    ).fetchone()
    return row is not None


def get_recent_title_tokens(
    db: DBConnection,
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
        "SELECT title_tokens FROM seen_articles WHERE seen_at >= %s ORDER BY seen_at DESC LIMIT %s",
        (cutoff, limit),
    ).fetchall()
    return [r["title_tokens"] for r in rows]


def get_near_dup_candidates(
    db: DBConnection,
    candidate_tokens: str,
    within_hours: int = 4,
    limit: int = 50,
) -> list[str]:
    """
    Use the pg_trgm GIN index on title_tokens to find trigram-similar rows
    within the time window. Returns at most `limit` token strings, ordered by
    similarity descending.

    The threshold (0.15) is intentionally lower than our Jaccard threshold (0.35)
    to avoid false negatives: token strings that share 35%+ of their word-set
    may have lower character-trigram overlap when tokens are short or inflected.
    The exact Jaccard/containment check in Python filters the small result set.

    The `%%` operator uses the GIN index; `SET LOCAL` scopes the threshold to
    the current transaction only.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    db.execute("SET LOCAL pg_trgm.similarity_threshold = 0.15")
    rows = db.execute(
        """
        SELECT title_tokens
        FROM   seen_articles
        WHERE  seen_at >= %s
          AND  title_tokens %% %s
        ORDER  BY similarity(title_tokens, %s) DESC
        LIMIT  %s
        """,
        (cutoff, candidate_tokens, candidate_tokens, limit),
    ).fetchall()
    return [r["title_tokens"] for r in rows]


def insert_seen_article(
    db: DBConnection,
    source_id: int,
    raw_hash: str,
    title_tokens: str,
    url: Optional[str],
    published_at: str,
    cluster_id: Optional[int] = None,
    *,
    commit: bool = True,
) -> Optional[int]:
    """INSERT ... ON CONFLICT DO NOTHING — idempotent on restart. Returns row id or None if duplicate."""
    cur = db.execute(
        """
        INSERT INTO seen_articles
            (source_id, raw_hash, title_tokens, url, published_at, cluster_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (raw_hash) DO NOTHING
        RETURNING id
        """,
        (source_id, raw_hash, title_tokens, url, published_at, cluster_id),
    )
    if commit:
        db.commit()
    row = cur.fetchone()
    return row["id"] if row else None


def assign_cluster(
    db: DBConnection,
    article_id: int,
    cluster_id: int,
) -> None:
    db.execute(
        "UPDATE seen_articles SET cluster_id = %s WHERE id = %s",
        (cluster_id, article_id),
    )
    db.commit()


# ── event_clusters ────────────────────────────────────────────────────────────

def find_candidate_clusters(
    db: DBConnection,
    within_hours: int = 4,
) -> list[Any]:
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
        WHERE  first_seen_at >= %s
        ORDER  BY first_seen_at DESC
        LIMIT  500
        """,
        (cutoff,),
    ).fetchall()


def get_cluster_source_ids(
    db: DBConnection,
    cluster_id: int,
) -> set[int]:
    """Return source_ids that have already contributed to this cluster."""
    rows = db.execute(
        "SELECT DISTINCT source_id FROM seen_articles WHERE cluster_id = %s",
        (cluster_id,),
    ).fetchall()
    return {r["source_id"] for r in rows}


def create_cluster(
    db: DBConnection,
    canonical_title: str,
    title_tokens: str,
    keywords: str,
    score: int,
    tickers: str = "",
    embedding: Optional[bytes] = None,
    *,
    commit: bool = True,
) -> int:
    cur = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, tickers, embedding)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (canonical_title, title_tokens, keywords, score, tickers or None, embedding),
    )
    if commit:
        db.commit()
    row = cur.fetchone()
    assert row is not None
    return row["id"]


def update_cluster(
    db: DBConnection,
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
               source_count    = source_count + %s,
               best_score      = GREATEST(best_score, %s),
               keywords        = %s,
               tickers         = %s,
               last_updated_at = %s
        WHERE  id = %s
        """,
        (1 if new_source else 0, score, merged_keywords, merged_tickers or None, _utcnow_iso(), cluster_id),
    )
    if commit:
        db.commit()


def mark_cluster_sent(
    db: DBConnection,
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
        SET    status          = %s,
               last_sent_at   = %s,
               cooldown_until  = %s,
               published_score = %s
        WHERE  id = %s
        """,
        (status, _iso(now), cooldown, score, cluster_id),
    )
    db.commit()


def get_cluster(
    db: DBConnection,
    cluster_id: int,
) -> Optional[Any]:
    return db.execute(
        "SELECT * FROM event_clusters WHERE id = %s",
        (cluster_id,),
    ).fetchone()


def get_top_sent_clusters(
    db: DBConnection,
    since_dt: datetime,
    limit: int = 10,
) -> list[Any]:
    """
    Return the top published clusters for the daily digest.

    Selects clusters first seen on or after since_dt (start of today in MSK)
    that have been published to Telegram, ordered by (best_score * source_count)
    descending.
    """
    cutoff = _iso(since_dt)
    return db.execute(
        """
        SELECT id, canonical_title, best_score, source_count,
               tickers, keywords, title_tokens, last_sent_at
        FROM   event_clusters
        WHERE  first_seen_at >= %s
          AND  last_sent_at IS NOT NULL
          AND  status IN ('published', 'updated')
        ORDER  BY best_score * source_count DESC
        LIMIT  %s
        """,
        (cutoff, limit),
    ).fetchall()


def count_recent_speaker_publishes(
    db: DBConnection,
    speaker_prefix: str,
    within_hours: int,
) -> int:
    """
    Count published clusters whose canonical_title starts with speaker_prefix (case-insensitive).
    Used to detect speaker saturation: too many posts from the same person in a short window.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM   event_clusters
        WHERE  last_sent_at >= %s
          AND  status IN ('published', 'updated')
          AND  LOWER(canonical_title) LIKE %s
        """,
        (cutoff, f"{speaker_prefix.lower()}%"),
    ).fetchone()
    return int(row["cnt"]) if row else 0


# ── telegram_sends ────────────────────────────────────────────────────────────

def has_recent_send_attempt(
    db: DBConnection,
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
        "SELECT 1 FROM telegram_sends WHERE cluster_id = %s AND sent_at >= %s LIMIT 1",
        (cluster_id, cutoff),
    ).fetchone()
    return row is not None


def get_recently_sent_title_tokens(
    db: DBConnection,
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
            WHERE  ts.sent_at >= %s AND ts.ok = 1 AND ts.cluster_id != %s
            """,
            (cutoff, exclude_cluster_id),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT DISTINCT ec.title_tokens
            FROM   telegram_sends ts
            JOIN   event_clusters ec ON ts.cluster_id = ec.id
            WHERE  ts.sent_at >= %s AND ts.ok = 1
            """,
            (cutoff,),
        ).fetchall()
    return [r["title_tokens"] for r in rows]


def get_recently_sent_clusters(
    db: DBConnection,
    within_hours: int = 2,
    exclude_cluster_id: Optional[int] = None,
) -> list[Any]:
    """
    Return (title_tokens, embedding) rows for clusters sent successfully within the window.

    Extends get_recently_sent_title_tokens with embeddings so the cross-cluster dup guard
    can use cosine similarity when embeddings are available, falling back to token overlap.
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    if exclude_cluster_id is not None:
        return db.execute(
            """
            SELECT DISTINCT ec.title_tokens, ec.embedding
            FROM   telegram_sends ts
            JOIN   event_clusters ec ON ts.cluster_id = ec.id
            WHERE  ts.sent_at >= %s AND ts.ok = 1 AND ts.cluster_id != %s
            """,
            (cutoff, exclude_cluster_id),
        ).fetchall()
    return db.execute(
        """
        SELECT DISTINCT ec.title_tokens, ec.embedding
        FROM   telegram_sends ts
        JOIN   event_clusters ec ON ts.cluster_id = ec.id
        WHERE  ts.sent_at >= %s AND ts.ok = 1
        """,
        (cutoff,),
    ).fetchall()


def log_send(
    db: DBConnection,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            cluster_id, decision, score, source_count, headline,
            tg_message_id, 1 if ok else 0, error_text,
        ),
    )
    db.commit()


# ── monitoring ────────────────────────────────────────────────────────────────

def get_last_ok_send_at(db: DBConnection) -> Optional[datetime]:
    """Return the timestamp of the most recent successful Telegram send, or None."""
    row = db.execute(
        "SELECT sent_at FROM telegram_sends WHERE ok=1 ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row["sent_at"].replace("Z", "+00:00"))


def get_source_stats(db: DBConnection) -> dict[str, int]:
    """Return counts of enabled sources grouped by status: ok / backoff / dead."""
    rows = db.execute(
        "SELECT status, COUNT(*) AS n FROM rss_sources WHERE enabled = 1 GROUP BY status"
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def get_dead_sources(db: DBConnection) -> list[Any]:
    """Return all enabled sources currently marked dead."""
    return db.execute(
        "SELECT id, name, url, error_count FROM rss_sources WHERE status = 'dead' AND enabled = 1"
    ).fetchall()


# ── admin: rss_sources CRUD ───────────────────────────────────────────────────

def get_all_sources(db: DBConnection) -> list[Any]:
    return db.execute(
        "SELECT id, name, url, enabled, status, error_count, last_fetched_at, created_at "
        "FROM rss_sources ORDER BY id"
    ).fetchall()


def add_source(db: DBConnection, name: str, url: str) -> int:
    """
    Insert a new RSS source. Raises psycopg2.errors.UniqueViolation on duplicate name or URL.
    Returns the new row id.
    """
    cur = db.execute(
        "INSERT INTO rss_sources (name, url) VALUES (%s, %s) RETURNING id",
        (name, url),
    )
    db.commit()
    row = cur.fetchone()
    assert row is not None
    return row["id"]


def disable_source(db: DBConnection, source_id: int) -> bool:
    """Soft-delete: set enabled=0. Returns True if a row was updated."""
    cur = db.execute(
        "UPDATE rss_sources SET enabled = 0 WHERE id = %s",
        (source_id,),
    )
    db.commit()
    return cur.rowcount > 0


def update_source(
    db: DBConnection,
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
        parts.append("url = %s")
        params.append(url)
    if enabled is not None:
        parts.append("enabled = %s")
        params.append(1 if enabled else 0)
    if not parts:
        return False
    params.append(source_id)
    cur = db.execute(
        f"UPDATE rss_sources SET {', '.join(parts)} WHERE id = %s",
        params,
    )
    db.commit()
    return cur.rowcount > 0


def reset_source_backoff(db: DBConnection, source_id: int) -> bool:
    """Reset backoff state so the source is polled on the next cycle."""
    cur = db.execute(
        """
        UPDATE rss_sources
        SET    status        = 'ok',
               error_count   = 0,
               last_error_at = NULL,
               next_retry_at = NULL
        WHERE  id = %s
        """,
        (source_id,),
    )
    db.commit()
    return cur.rowcount > 0


# ── portfolio_subscriptions ───────────────────────────────────────────────────

def get_subscribed_users(db: DBConnection, tickers: list[str]) -> list[int]:
    """Return user_ids subscribed to any of the given tickers."""
    if not tickers:
        return []
    placeholders = ",".join(["%s"] * len(tickers))
    rows = db.execute(
        f"SELECT DISTINCT user_id FROM portfolio_subscriptions WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return [r["user_id"] for r in rows]


def get_subscribed_users_with_settings(
    db: DBConnection, tickers: list[str]
) -> list[dict]:
    """Return subscribed users with quiet-hours settings.

    Each dict has: user_id (Telegram ID), quiet_from, quiet_to.
    user_id in portfolio_subscriptions stores the Telegram ID directly.
    """
    if not tickers:
        return []
    placeholders = ",".join(["%s"] * len(tickers))
    rows = db.execute(
        f"""
        SELECT DISTINCT ON (ps.user_id)
               ps.user_id,
               us.quiet_from,
               us.quiet_to,
               COALESCE(us.utc_offset, 3)  AS utc_offset,
               COALESCE(us.min_score, 30)  AS min_score
        FROM   portfolio_subscriptions ps
        LEFT JOIN users u ON u.telegram_id = ps.user_id
        LEFT JOIN user_settings us ON us.user_id = u.id
        WHERE  ps.ticker IN ({placeholders})
        """,
        tickers,
    ).fetchall()
    return [
        {
            "user_id":    r["user_id"],
            "quiet_from": r["quiet_from"],
            "quiet_to":   r["quiet_to"],
            "utc_offset": r["utc_offset"],
            "min_score":  r["min_score"],
        }
        for r in rows
    ]


_MONTHS_SHORT = (
    "", "янв", "фев", "мар", "апр", "май", "июн",
    "июл", "авг", "сен", "окт", "ноя", "дек",
)


def _fmt_cluster_entry(title: str, updated_at: str) -> str:
    """Format a historical cluster as '15 мая: title' for RAG context."""
    try:
        dt = datetime.fromisoformat(updated_at.rstrip("Z")).replace(tzinfo=timezone.utc)
        date_str = f"{dt.day} {_MONTHS_SHORT[dt.month]}"
    except Exception:
        date_str = "?"
    return f"{date_str}: {title}"


def get_recent_cluster_titles_for_tickers(
    db: DBConnection, tickers: list[str], limit: int = 5, days: int = 30
) -> list[str]:
    """Return recent published cluster titles matching any of the tickers (RAG context)."""
    if not tickers:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ilike_clauses = " OR ".join("tickers ILIKE %s" for _ in tickers)
    params = tuple(f"%{t}%" for t in tickers) + (cutoff, limit)
    rows = db.execute(
        f"""
        SELECT canonical_title, last_updated_at FROM event_clusters
        WHERE ({ilike_clauses})
          AND last_updated_at > %s
          AND status IN ('published', 'updated')
        ORDER BY last_updated_at DESC
        LIMIT %s
        """,
        params,
    ).fetchall()
    return [_fmt_cluster_entry(row["canonical_title"], row["last_updated_at"]) for row in rows]


def get_similar_clusters_by_embedding(
    db: DBConnection,
    embedding_bytes: bytes,
    limit: int = 3,
    days: int = 30,
    exclude_id: int = 0,
) -> list[str]:
    """Cosine similarity search over stored BYTEA embeddings (computed in Python/numpy).

    Returns up to `limit` formatted strings 'DD мес: title' for clusters
    semantically similar (cosine >= 0.50) to the given embedding.
    """
    if not embedding_bytes:
        return []
    import numpy as np
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = db.execute(
        """
        SELECT id, canonical_title, last_updated_at, embedding
        FROM event_clusters
        WHERE first_seen_at >= %s
          AND status IN ('published', 'updated')
          AND embedding IS NOT NULL
          AND id != %s
        ORDER BY last_updated_at DESC
        LIMIT 500
        """,
        (cutoff, exclude_id),
    ).fetchall()
    if not rows:
        return []
    query_vec = np.frombuffer(embedding_bytes, dtype="float32")
    scored: list[tuple[float, dict]] = []
    for row in rows:
        try:
            cand_vec = np.frombuffer(row["embedding"], dtype="float32")
            sim = float(np.dot(query_vec, cand_vec))
        except Exception:
            continue
        if sim >= 0.50:
            scored.append((sim, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [
        _fmt_cluster_entry(row["canonical_title"], row["last_updated_at"])
        for _, row in scored[:limit]
    ]


def get_user_tickers(db: DBConnection, user_id: int) -> list[str]:
    """Return tickers subscribed by a user, sorted alphabetically."""
    rows = db.execute(
        "SELECT ticker FROM portfolio_subscriptions WHERE user_id = %s ORDER BY ticker",
        (user_id,),
    ).fetchall()
    return [r["ticker"] for r in rows]


def set_user_tickers(db: DBConnection, user_id: int, tickers: list[str]) -> None:
    """Replace a user's subscriptions with the given tickers (idempotent)."""
    import logging as _logging
    _log = _logging.getLogger(__name__)
    old = get_user_tickers(db, user_id)
    _log.info(
        "portfolio_change action=set user_id=%d old=%d new=%d tickers=%s",
        user_id, len(old), len(tickers), ",".join(sorted(t.upper() for t in tickers)) or "(none)",
        extra={"event": "portfolio_change", "action": "set", "user_id": user_id,
               "old_count": len(old), "new_count": len(tickers)},
    )
    with db:
        db.execute(
            "DELETE FROM portfolio_subscriptions WHERE user_id = %s",
            (user_id,),
        )
        db.executemany(
            "INSERT INTO portfolio_subscriptions (user_id, ticker) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [(user_id, t.upper()) for t in tickers],
        )


def clear_user_tickers(db: DBConnection, user_id: int) -> None:
    """Remove all subscriptions for a user."""
    import logging as _logging
    _log = _logging.getLogger(__name__)
    old = get_user_tickers(db, user_id)
    _log.info(
        "portfolio_change action=clear user_id=%d wiped=%d",
        user_id, len(old),
        extra={"event": "portfolio_change", "action": "clear", "user_id": user_id, "wiped": len(old)},
    )
    db.execute(
        "DELETE FROM portfolio_subscriptions WHERE user_id = %s",
        (user_id,),
    )
    db.commit()


# ── users ─────────────────────────────────────────────────────────────────────

def upsert_user(
    db: DBConnection,
    telegram_id: int,
    username: Optional[str],
    first_name: str,
) -> str:
    """Register or update a user. Returns the user's current plan ('free'/'pro')."""
    cur = db.execute(
        """
        INSERT INTO users (telegram_id, username, first_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE
            SET username   = EXCLUDED.username,
                first_name = EXCLUDED.first_name
        RETURNING plan
        """,
        (telegram_id, username, first_name),
    )
    db.commit()
    row = cur.fetchone()
    assert row is not None
    return row["plan"]


def get_user(db: DBConnection, telegram_id: int) -> Optional[Any]:
    return db.execute(
        "SELECT * FROM users WHERE telegram_id = %s",
        (telegram_id,),
    ).fetchone()


def get_user_plan(db: DBConnection, telegram_id: int) -> str:
    """Return the user's plan, defaulting to 'free' if the user is not yet registered."""
    row = db.execute(
        "SELECT plan FROM users WHERE telegram_id = %s",
        (telegram_id,),
    ).fetchone()
    return row["plan"] if row else "free"


# ── price_snapshots ───────────────────────────────────────────────────────────

def insert_price_snapshot(
    db: DBConnection,
    cluster_id: int,
    ticker: str,
    event_type: str,
    published_at: str,
    price_at_publish: Optional[float],
) -> None:
    db.execute(
        """
        INSERT INTO price_snapshots
            (cluster_id, ticker, event_type, published_at, price_at_publish)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (cluster_id, ticker, event_type, published_at, price_at_publish),
    )
    db.commit()


def get_pending_1h_snapshots(db: DBConnection, limit: int = 20) -> list[Any]:
    """Snapshots older than 1h that still need price_1h."""
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=1))
    return db.execute(
        """
        SELECT id, ticker FROM price_snapshots
        WHERE  price_at_publish IS NOT NULL
          AND  price_1h IS NULL
          AND  published_at <= %s
        LIMIT  %s
        """,
        (cutoff, limit),
    ).fetchall()


def get_pending_24h_snapshots(db: DBConnection, limit: int = 20) -> list[Any]:
    """Snapshots older than 24h that still need price_24h."""
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=24))
    return db.execute(
        """
        SELECT id, ticker FROM price_snapshots
        WHERE  price_at_publish IS NOT NULL
          AND  price_24h IS NULL
          AND  published_at <= %s
        LIMIT  %s
        """,
        (cutoff, limit),
    ).fetchall()


def update_price_snapshot_1h(
    db: DBConnection,
    snapshot_id: int,
    price: float,
    timestamp: str,
) -> None:
    db.execute(
        "UPDATE price_snapshots SET price_1h = %s, snapshot_1h_at = %s WHERE id = %s",
        (price, timestamp, snapshot_id),
    )
    db.commit()


def update_price_snapshot_24h(
    db: DBConnection,
    snapshot_id: int,
    price: float,
    timestamp: str,
) -> None:
    db.execute(
        "UPDATE price_snapshots SET price_24h = %s, snapshot_24h_at = %s WHERE id = %s",
        (price, timestamp, snapshot_id),
    )
    db.commit()


def get_price_correlations(
    db: DBConnection,
    event_type: str,
    ticker_list: list[str],
    min_samples: int = 3,
) -> list[Any]:
    """
    Avg 24h price change per ticker for the given event_type.
    Only returns tickers with >= min_samples completed snapshots.
    """
    if not ticker_list:
        return []
    placeholders = ",".join(["%s"] * len(ticker_list))
    return db.execute(
        f"""
        SELECT
            ticker,
            AVG((price_24h - price_at_publish) / NULLIF(price_at_publish, 0) * 100) AS avg_24h_pct,
            COUNT(*) AS sample_count
        FROM   price_snapshots
        WHERE  event_type = %s
          AND  ticker IN ({placeholders})
          AND  price_at_publish > 0
          AND  price_24h IS NOT NULL
        GROUP  BY ticker
        HAVING COUNT(*) >= %s
        ORDER  BY sample_count DESC
        """,
        [event_type, *ticker_list, min_samples],
    ).fetchall()


# ── user_settings ─────────────────────────────────────────────────────────────

DEFAULT_SETTINGS: dict = {"min_score": 30, "quiet_from": None, "quiet_to": None, "utc_offset": 3}


def get_user_settings(db: DBConnection, user_id: int) -> Any:
    """Return the user_settings row by internal users.id, or defaults if none."""
    row = db.execute(
        "SELECT * FROM user_settings WHERE user_id = %s",
        (user_id,),
    ).fetchone()
    return row if row is not None else DEFAULT_SETTINGS


def get_user_settings_by_telegram_id(db: DBConnection, telegram_id: int) -> Any:
    """Return the user_settings row by telegram_id (JOIN through users), or defaults."""
    row = db.execute(
        """
        SELECT us.*
        FROM   user_settings us
        JOIN   users u ON u.id = us.user_id
        WHERE  u.telegram_id = %s
        """,
        (telegram_id,),
    ).fetchone()
    return row if row is not None else DEFAULT_SETTINGS


def save_user_settings(
    db: DBConnection,
    user_id: int,
    *,
    min_score: int = 30,
    quiet_from: Optional[int] = None,
    quiet_to: Optional[int] = None,
    utc_offset: int = 3,
) -> None:
    """Upsert all settings fields for the user."""
    db.execute(
        """
        INSERT INTO user_settings (user_id, min_score, quiet_from, quiet_to, utc_offset, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE
            SET min_score  = EXCLUDED.min_score,
                quiet_from = EXCLUDED.quiet_from,
                quiet_to   = EXCLUDED.quiet_to,
                utc_offset = EXCLUDED.utc_offset,
                updated_at = EXCLUDED.updated_at
        """,
        (user_id, min_score, quiet_from, quiet_to, utc_offset, _utcnow_iso()),
    )
    db.commit()


def is_quiet_hour(
    quiet_from: Optional[int],
    quiet_to: Optional[int],
    utc_hour: int,
    utc_offset: int = 3,
) -> bool:
    """Return True if the current local hour falls within the quiet window.

    quiet_from/quiet_to are stored as local time hours.
    utc_offset converts the server UTC hour to the user's local hour.
    Handles midnight-crossing ranges: quiet_from=23, quiet_to=8 → 23:xx–07:xx local.
    """
    if quiet_from is None or quiet_to is None:
        return False
    local_hour = (utc_hour + utc_offset) % 24
    if quiet_from < quiet_to:
        return quiet_from <= local_hour < quiet_to
    return local_hour >= quiet_from or local_hour < quiet_to


# ── subscriptions ─────────────────────────────────────────────────────────────

_GRACE_DAYS = 3


def create_subscription(
    db: DBConnection,
    telegram_id: int,
    plan: str,
    expires_at: str,
    payment_id: Optional[str] = None,
) -> int:
    """Create a new subscription and set users.plan='pro'. Returns subscription id."""
    user = get_user(db, telegram_id)
    if user is None:
        raise ValueError(f"User telegram_id={telegram_id} not found")
    cur = db.execute(
        """
        INSERT INTO subscriptions (user_id, plan, expires_at, payment_id)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (user["id"], plan, expires_at, payment_id),
    )
    sub_id = cur.fetchone()["id"]
    db.execute("UPDATE users SET plan = 'pro' WHERE id = %s", (user["id"],))
    db.commit()
    return sub_id


def get_active_subscription(db: DBConnection, telegram_id: int) -> Optional[Any]:
    """Return the most recent active or grace subscription, or None."""
    return db.execute(
        """
        SELECT s.*
        FROM   subscriptions s
        JOIN   users u ON s.user_id = u.id
        WHERE  u.telegram_id = %s
          AND  s.status IN ('active', 'grace')
        ORDER  BY s.created_at DESC
        LIMIT  1
        """,
        (telegram_id,),
    ).fetchone()


def get_expiring_subscriptions(db: DBConnection, within_days: int = 3) -> list[Any]:
    """Return active subscriptions expiring within the given window (for reminders)."""
    now = _utcnow_iso()
    cutoff = _iso(datetime.now(timezone.utc) + timedelta(days=within_days))
    return db.execute(
        """
        SELECT s.id, s.plan, s.expires_at, u.telegram_id
        FROM   subscriptions s
        JOIN   users u ON s.user_id = u.id
        WHERE  s.status = 'active'
          AND  s.expires_at > %s
          AND  s.expires_at <= %s
        """,
        (now, cutoff),
    ).fetchall()


def expire_subscriptions(db: DBConnection) -> tuple[int, int]:
    """
    Advance subscription lifecycle:
      active → grace  when expires_at < now
      grace  → expired when expires_at < now - GRACE_DAYS, also sets users.plan = 'free'
    Returns (graced_count, expired_count).
    """
    now = _utcnow_iso()
    grace_cutoff = _iso(datetime.now(timezone.utc) - timedelta(days=_GRACE_DAYS))

    cur = db.execute(
        "UPDATE subscriptions SET status = 'grace' WHERE status = 'active' AND expires_at < %s",
        (now,),
    )
    graced = cur.rowcount

    cur = db.execute(
        """
        UPDATE subscriptions SET status = 'expired'
        WHERE  status = 'grace' AND expires_at < %s
        RETURNING user_id
        """,
        (grace_cutoff,),
    )
    expired_user_ids = [r["user_id"] for r in cur.fetchall()]

    for uid in expired_user_ids:
        still_active = db.execute(
            "SELECT 1 FROM subscriptions WHERE user_id = %s AND status IN ('active','grace') LIMIT 1",
            (uid,),
        ).fetchone()
        if not still_active:
            db.execute("UPDATE users SET plan = 'free' WHERE id = %s", (uid,))

    db.commit()
    return graced, len(expired_user_ids)


# ── retention ─────────────────────────────────────────────────────────────────

def run_retention(db: DBConnection) -> None:
    with db:
        db.execute(
            "DELETE FROM seen_articles WHERE seen_at < %s",
            (_iso(datetime.now(timezone.utc) - timedelta(hours=48)),),
        )
        db.execute(
            "DELETE FROM event_clusters WHERE first_seen_at < %s",
            (_iso(datetime.now(timezone.utc) - timedelta(days=7)),),
        )
        db.execute(
            "DELETE FROM price_snapshots WHERE created_at < %s",
            (_iso(datetime.now(timezone.utc) - timedelta(days=90)),),
        )
        # reset backoff entries whose retry window has passed
        db.execute(
            """
            UPDATE rss_sources
            SET    status = 'ok', next_retry_at = NULL
            WHERE  status = 'backoff'
              AND  next_retry_at <= %s
            """,
            (_utcnow_iso(),),
        )
    logger.info("Retention complete")


# ── helpers ───────────────────────────────────────────────────────────────────

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ══════════════════════════════════════════════════════════════════════════════
# Calendar — corporate events
# ══════════════════════════════════════════════════════════════════════════════

def _to_json(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _from_json(val) -> dict:
    """
    Safely deserialize JSONB value returned by psycopg2.
    psycopg2 may return JSONB columns already parsed as dict, or as a raw string.
    Used by jobs.py to deserialize event details before formatting DMs.
    """
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    return json.loads(val)


def upsert_corporate_event(
    db: DBConnection,
    ticker: str,
    event_type: str,
    event_date: date,
    details: dict,
) -> int:
    """
    Insert or update a corporate event.
    ON CONFLICT (ticker, event_type, event_date) updates details + synced_at.
    Returns the event id.
    """
    cur = db.execute(
        """
        INSERT INTO corporate_events (ticker, event_type, event_date, details)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker, event_type, event_date) DO UPDATE
            SET details   = EXCLUDED.details,
                synced_at = NOW()
        RETURNING id
        """,
        (ticker, event_type, event_date, _to_json(details)),
    )
    event_id = cur.fetchone()["id"]
    db.commit()
    return event_id


def get_all_portfolio_tickers(db: DBConnection) -> list[str]:
    """Return sorted list of unique tickers across all portfolio_subscriptions."""
    rows = db.execute(
        "SELECT DISTINCT ticker FROM portfolio_subscriptions ORDER BY ticker"
    ).fetchall()
    return [r["ticker"] for r in rows]


def get_pending_calendar_notifications(
    db: DBConnection,
    days_ahead: int,
) -> list[Any]:
    """
    Return (event, user) pairs where:
      - event_date == today + days_ahead
      - user has that ticker in their portfolio
      - notification NOT yet sent (not in calendar_notifications_sent)

    Row fields: event_id, ticker, event_type, event_date, details, user_id, telegram_id
    """
    target_date = (datetime.now(timezone.utc).date() + timedelta(days=days_ahead))
    return db.execute(
        """
        SELECT
            ce.id          AS event_id,
            ce.ticker,
            ce.event_type,
            ce.event_date,
            ce.details,
            u.id           AS user_id,
            u.telegram_id
        FROM   corporate_events             ce
        JOIN   portfolio_subscriptions      ps  ON ps.ticker  = ce.ticker
        JOIN   users                        u   ON u.telegram_id = ps.user_id
        LEFT   JOIN calendar_notifications_sent cns
               ON cns.event_id = ce.id AND cns.user_id = u.id
        WHERE  ce.event_date = %s
          AND  cns.id IS NULL
        ORDER  BY ce.ticker, u.telegram_id
        """,
        (target_date,),
    ).fetchall()


def mark_calendar_notification_sent(
    db: DBConnection,
    user_id: int,
    event_id: int,
) -> None:
    """Record that a calendar DM was sent. Idempotent via ON CONFLICT DO NOTHING."""
    db.execute(
        """
        INSERT INTO calendar_notifications_sent (user_id, event_id)
        VALUES (%s, %s)
        ON CONFLICT (user_id, event_id) DO NOTHING
        """,
        (user_id, event_id),
    )
    db.commit()


def get_portfolio_events_for_user(
    db: DBConnection,
    telegram_id: int,
    from_date: date,
    to_date: date,
) -> list[Any]:
    """
    Return corporate events in [from_date, to_date] for tickers
    in the given user's portfolio. Ordered by event_date ASC.
    """
    return db.execute(
        """
        SELECT ce.ticker, ce.event_type, ce.event_date, ce.details
        FROM   corporate_events        ce
        JOIN   portfolio_subscriptions ps ON ps.ticker = ce.ticker
        WHERE  ps.user_id = %s
          AND  ce.event_date BETWEEN %s AND %s
        ORDER  BY ce.event_date, ce.ticker
        """,
        (telegram_id, from_date, to_date),
    ).fetchall()


def toggle_user_ticker(db, user_id: int, ticker: str, adding: bool) -> None:
    """Add or remove a single ticker — single SQL op instead of full DELETE+INSERT."""
    if adding:
        db.execute(
            "INSERT INTO portfolio_subscriptions (user_id, ticker) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user_id, ticker.upper()),
        )
    else:
        db.execute(
            "DELETE FROM portfolio_subscriptions WHERE user_id = %s AND ticker = %s",
            (user_id, ticker.upper()),
        )
    db.commit()


def get_recent_clusters(db: "DBConnection", limit: int = 20) -> list[dict]:
    """
    Return the most recently created event clusters with their last send status.
    Used by admin /pipeline/clusters endpoint and /status bot command.
    """
    cur = db.execute("""
        SELECT
            ec.id,
            ec.canonical_title,
            ec.source_count,
            ec.status,
            ec.tickers,
            ec.first_seen_at,
            ts.decision,
            ts.score,
            ts.ok          AS sent_ok,
            ts.error_text
        FROM event_clusters ec
        LEFT JOIN (
            SELECT DISTINCT ON (cluster_id)
                cluster_id, decision, score, ok, error_text
            FROM telegram_sends
            ORDER BY cluster_id, sent_at DESC
        ) ts ON ts.cluster_id = ec.id
        ORDER BY ec.first_seen_at DESC
        LIMIT %s
    """, (limit,))
    return [dict(row) for row in cur.fetchall()]


def get_cluster_by_id(db: DBConnection, cluster_id: int) -> Optional[dict]:
    """Return cluster row or None if not found."""
    row = db.execute(
        "SELECT id, canonical_title, tickers FROM event_clusters WHERE id = %s",
        (cluster_id,),
    ).fetchone()
    return dict(row) if row else None


def update_cluster_tickers(db: DBConnection, cluster_id: int, tickers: str) -> None:
    """Update tickers for a cluster. Empty string clears the column (sets NULL)."""
    db.execute(
        "UPDATE event_clusters SET tickers = %s WHERE id = %s",
        (tickers or None, cluster_id),
    )
    db.commit()


# ── metrics ───────────────────────────────────────────────────────────────────

def log_bot_command(db: DBConnection, telegram_id: int, command: str) -> None:
    db.execute(
        "INSERT INTO bot_command_log (telegram_id, command) VALUES (%s, %s)",
        (telegram_id, command),
    )
    db.commit()


def get_metrics_delta(db: DBConnection, hours: int) -> dict:
    row = db.execute(
        """
        SELECT
          MAX(articles_fetched)   - MIN(articles_fetched)   AS fetched,
          MAX(articles_exact_dup) - MIN(articles_exact_dup) AS exact_dup,
          MAX(articles_near_dup)  - MIN(articles_near_dup)  AS near_dup,
          MAX(articles_noise)     - MIN(articles_noise)      AS noise,
          MAX(events_published)   - MIN(events_published)    AS published,
          MAX(tg_sent_ok)         - MIN(tg_sent_ok)          AS tg_ok,
          MAX(tg_sent_fail)       - MIN(tg_sent_fail)        AS tg_fail,
          MAX(tg_rate_limited)    - MIN(tg_rate_limited)     AS rate_limited,
          COUNT(*) AS snapshot_count
        FROM metrics_snapshots
        WHERE ts >= NOW() - make_interval(hours => %s)
        """,
        (hours,),
    ).fetchone()
    return dict(row) if row else {}


def get_user_stats(db: DBConnection, hours: int) -> dict:
    total = db.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    new_users = db.execute(
        """
        SELECT COUNT(*) AS n FROM users
        WHERE created_at::timestamptz >= NOW() - make_interval(hours => %s)
        """,
        (hours,),
    ).fetchone()["n"]
    active = db.execute(
        """
        SELECT COUNT(DISTINCT telegram_id) AS n FROM bot_command_log
        WHERE ts >= NOW() - make_interval(hours => %s)
        """,
        (hours,),
    ).fetchone()["n"]
    commands_n = db.execute(
        """
        SELECT COUNT(*) AS n FROM bot_command_log
        WHERE ts >= NOW() - make_interval(hours => %s)
        """,
        (hours,),
    ).fetchone()["n"]
    return {"total": total, "new": new_users, "active": active, "commands": commands_n}

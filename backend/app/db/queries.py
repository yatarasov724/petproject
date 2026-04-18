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
    # RBC finance feed is unreachable (DNS failure) — disabled
    # ("RBC", "https://rss.rbc.ru/finances/news.rss"),
    ("TASS",       "https://tass.ru/rss/v2.xml"),
    ("Interfax",   "https://www.interfax.ru/rss"),       # was /rss.asp → 301
    ("Vedomosti",  "https://www.vedomosti.ru/rss/news"),
    ("Kommersant", "https://www.kommersant.ru/rss/news.xml"),  # was /RSS/ → 301
    ("Prime",      "https://1prime.ru/export/rss2/index.xml"),  # финансовое агентство
]

_BACKOFF_MAX_MINUTES = 120
_DEAD_AFTER_ERRORS   = 10


def seed_sources(db: sqlite3.Connection) -> None:
    db.executemany(
        "INSERT OR IGNORE INTO rss_sources (name, url) VALUES (?, ?)",
        _RSS_SEEDS,
    )
    db.commit()
    logger.info("RSS sources seeded (%d entries)", len(_RSS_SEEDS))


# ── rss_sources reads ─────────────────────────────────────────────────────────

def get_active_sources(db: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Return sources that are enabled, not dead, and either:
    - have no retry delay (status='ok'), or
    - are in backoff but next_retry_at has passed.
    """
    now = _utcnow_iso()
    return db.execute(
        """
        SELECT *
        FROM   rss_sources
        WHERE  enabled = 1
          AND  status  != 'dead'
          AND  (next_retry_at IS NULL OR next_retry_at <= ?)
        ORDER  BY id
        """,
        (now,),
    ).fetchall()


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
) -> list[str]:
    """Returns list of title_token strings for near-dedup Jaccard check."""
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=within_hours))
    rows = db.execute(
        "SELECT title_tokens FROM seen_articles WHERE seen_at >= ?",
        (cutoff,),
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
) -> int:
    cur = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score)
        VALUES (?, ?, ?, ?)
        """,
        (canonical_title, title_tokens, keywords, score),
    )
    db.commit()
    return cur.lastrowid


def update_cluster(
    db: sqlite3.Connection,
    cluster_id: int,
    score: int,
    new_source: bool,
    merged_keywords: str,
) -> None:
    """
    new_source=True → increment source_count.
    Always increments article_count, updates best_score, keywords, last_updated_at.
    merged_keywords is the caller-computed union of existing + new article tokens.
    """
    db.execute(
        """
        UPDATE event_clusters
        SET    article_count   = article_count + 1,
               source_count    = source_count + ?,
               best_score      = MAX(best_score, ?),
               keywords        = ?,
               last_updated_at = ?
        WHERE  id = ?
        """,
        (1 if new_source else 0, score, merged_keywords, _utcnow_iso(), cluster_id),
    )
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


# ── telegram_sends ────────────────────────────────────────────────────────────

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

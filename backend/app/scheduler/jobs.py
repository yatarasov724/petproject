"""
Scheduler job definitions.

poll_job    — runs every 60 s, drives the full pipeline
cleanup_job — runs every 24 h, purges stale DB rows
backup_job  — runs every 6 h, hot-backup of the SQLite DB (keeps last 10)

Each job:
  - opens its own DB connection (one per job run, not per article)
  - delegates all article-level logic to orchestrator.process()
  - emits metrics via app.core.metrics
  - closes the connection in finally
"""

import logging
from collections import Counter

from app.core import metrics
from app.core.alerting import send_ops as _send_ops
from app.db.database import get_db
from app.db import queries
from app.pipeline.fetcher import fetch_all
from app.pipeline.orchestrator import process
from app.telegram import client as tg
from app.telegram.formatter import format_digest

logger = logging.getLogger(__name__)

# Dead-source IDs we've already alerted about — reset on service restart.
_alerted_dead: set[int] = set()

# ── digest relevance filter ───────────────────────────────────────────────────
# Lemmatized tokens that signal a cluster is Russia/MOEX-relevant.
# Checked against both `keywords` (top-12 by length) and `title_tokens`
# (full anchor token set) so short tokens like "рф" are never missed.

_RUSSIA_MARKET_TOKENS: frozenset[str] = frozenset({
    # Country / government
    "рф", "россия", "российский",
    "правительство", "минфин", "цб",
    # Key officials
    "путин", "мишустин", "набиуллина", "новак", "силуанов",
    "греф", "миллер", "сечин", "белоусов",
    # Top MOEX companies
    "газпром", "сбербанк", "лукойл", "роснефть", "новатэк",
    "норникель", "яндекс", "татнефть", "транснефть", "алроса",
    "северсталь", "нлмк", "ммк", "сургутнефтегаз", "аэрофлот",
    "магнит", "мтс", "ростелеком", "фосагро", "озон",
    "тинькофф", "мосбиржа", "втб", "полюс", "мечел", "селигдар",
    # Russian financial instruments
    "рубль", "офз",
    # Energy — critical for Russian budget and MOEX heavyweights
    "нефть", "опек",
})

_DIGEST_LIMIT    = 10   # max clusters in the digest
_DIGEST_PRE_FETCH = 30  # fetch 3× before RF filter to have enough candidates


def _is_russia_relevant(cluster) -> bool:
    """
    Return True if the cluster is relevant to the Russian market / MOEX.
    Checks keyword tokens from both the accumulated `keywords` field
    and the anchor article's `title_tokens`.
    """
    # A cluster with MOEX tickers is definitively Russia-relevant.
    if cluster["tickers"]:
        return True
    tokens = (
        set((cluster["keywords"]     or "").split())
        | set((cluster["title_tokens"] or "").split())
    )
    return bool(tokens & _RUSSIA_MARKET_TOKENS)


async def poll_job() -> None:
    """
    Fetch → per-article pipeline → log aggregate stats.
    Errors in individual articles are absorbed by orchestrator.process().
    """
    db = get_db()
    try:
        articles = await fetch_all(db)
        metrics.inc(metrics.ARTICLES_FETCHED, len(articles))

        counts: Counter[str] = Counter()
        for article in articles:
            result = await process(db, article)
            counts[result.outcome.value] += 1

        # Dump per-poll stats alongside process-lifetime totals
        poll_stats = dict(counts)
        metrics.log_snapshot(poll_stats)

        logger.info(
            "poll complete",
            extra={
                "event":    "poll_complete",
                **{f"poll_{k}": v for k, v in poll_stats.items()},
            },
        )

    except Exception:
        metrics.inc(metrics.PIPELINE_ERRORS)
        logger.exception("poll_job crashed — will retry on next tick")
    finally:
        db.close()


def cleanup_job() -> None:
    """
    Retention: delete seen_articles > 48h, clusters > 7d, reset expired backoffs.
    """
    db = get_db()
    try:
        queries.run_retention(db)
        metrics.inc(metrics.CLEANUP_RUNS)
        logger.info(
            "cleanup complete",
            extra={"event": "cleanup_complete"},
        )
    except Exception:
        logger.exception("cleanup_job crashed")
    finally:
        db.close()


async def heartbeat_job() -> None:
    """
    Send a compact status line to the ops chat every 5 minutes.
    Also fires a one-time alert for each source that has become dead.
    No-ops if TELEGRAM_OPS_CHAT_ID is not configured.
    """
    global _alerted_dead

    db = get_db()
    try:
        stats     = queries.get_source_stats(db)
        ok_n      = stats.get("ok", 0)
        backoff_n = stats.get("backoff", 0)
        dead_n    = stats.get("dead", 0)
        total_n   = ok_n + backoff_n + dead_n

        uptime_s       = metrics.uptime_seconds()
        hours, rem     = divmod(uptime_s, 3600)
        minutes        = rem // 60

        header = "🚨" if dead_n else "✅"

        source_parts = [f"{ok_n}/{total_n} ok"]
        if backoff_n:
            source_parts.append(f"{backoff_n} backoff")
        if dead_n:
            source_parts.append(f"{dead_n} DEAD ⚠")

        tg_ok   = metrics.get(metrics.TG_SENT_OK)
        tg_fail = metrics.get(metrics.TG_SENT_FAIL)

        text = (
            f"{header} MOEX parser — alive\n"
            f"⏱ uptime: {hours}h {minutes}m\n"
            f"📡 sources: {' · '.join(source_parts)}\n"
            f"📊 published: {tg_ok} total · {tg_fail} failed"
        )
        await _send_ops(text)

        # One-time alert per dead source (resets on service restart).
        dead_sources = queries.get_dead_sources(db)
        for src in dead_sources:
            if src["id"] not in _alerted_dead:
                await _send_ops(
                    f"🚨 Source DEAD: {src['name']} ({src['error_count']} errors)\n"
                    f"URL: {src['url']}"
                )
                _alerted_dead.add(src["id"])

    except Exception:
        logger.exception("heartbeat_job crashed")
    finally:
        db.close()


async def digest_job(within_hours: int, label: str) -> None:
    """
    Send a daily digest of the top published events to the main channel.

    within_hours — how far back to look for sent clusters.
    label        — display time shown in the header, e.g. "22:00" (MSK).

    Selection:
      1. Fetch up to _DIGEST_PRE_FETCH candidates ordered by score*source_count.
      2. Keep only Russia/MOEX-relevant ones via _is_russia_relevant().
      3. Take the top _DIGEST_LIMIT of what remains.
    """
    db = get_db()
    try:
        candidates = queries.get_top_sent_clusters(
            db, within_hours=within_hours, limit=_DIGEST_PRE_FETCH
        )
        clusters = [c for c in candidates if _is_russia_relevant(c)][:_DIGEST_LIMIT]

        if not clusters:
            logger.info(
                "digest_job: no Russia-relevant clusters in the last %dh — skipping",
                within_hours,
                extra={"event": "digest_skipped", "within_hours": within_hours},
            )
            return

        from app.ai.digest import generate_digest
        headlines = [c["canonical_title"] for c in clusters]
        ai_digest = await generate_digest(headlines)

        text = format_digest(list(clusters), ai_digest, label=label)
        msg_id = await tg.send_text(text)

        if msg_id is not None:
            logger.info(
                "digest sent",
                extra={
                    "event":       "digest_sent",
                    "label":       label,
                    "cluster_n":   len(clusters),
                    "ai":          ai_digest is not None,
                    "tg_msg_id":   msg_id,
                },
            )
        else:
            logger.error(
                "digest send failed",
                extra={"event": "digest_send_failed", "label": label},
            )

    except Exception:
        logger.exception("digest_job crashed")
    finally:
        db.close()


def backup_job() -> None:
    """
    Hot-backup the SQLite DB every 6 h. Keeps the last 10 backups.
    Uses sqlite3.Connection.backup() — safe while the service is live.
    """
    try:
        from scripts.backup_db import backup, _DEFAULT_DB, _BACKUP_DIR
        dest = backup(db_path=_DEFAULT_DB, backup_dir=_BACKUP_DIR, max_keep=10)
        logger.info(
            "db backup complete",
            extra={"event": "db_backup_complete", "file": dest.name},
        )
    except Exception:
        logger.exception("backup_job crashed")

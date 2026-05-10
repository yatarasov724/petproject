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
from datetime import datetime, timezone

from app.core import metrics
import app.core.tg_client as _tg_client
from app.core.alerting import send_ops as _send_ops
from app.db.database import get_db
from app.db import queries
from app.pipeline.fetcher import fetch_all
from app.pipeline.tg_fetcher import fetch_all_tg
from app.pipeline.orchestrator import process
from app.pipeline.relevance import is_russia_relevant
from app.telegram import client as tg
from app.telegram.formatter import format_digest

logger = logging.getLogger(__name__)

# Dead-source IDs we've already alerted about — reset on service restart.
_alerted_dead: set[int] = set()

# Silence alerting state — reset on service restart.
_SILENCE_WARN_HOURS  = 2   # show ⚠️ in heartbeat text
_SILENCE_ALERT_HOURS = 3   # send dedicated ops alert
_SILENCE_ALERT_COOLDOWN_H = 2  # don't re-fire the dedicated alert within this window
_last_silence_alert: datetime | None = None

_DIGEST_LIMIT     = 10  # max clusters in the digest
_DIGEST_PRE_FETCH = 30  # fetch 3× before RF filter to have enough candidates


async def poll_job() -> None:
    """
    Fetch → per-article pipeline → log aggregate stats.
    Errors in individual articles are absorbed by orchestrator.process().
    """
    db = get_db()
    try:
        articles = await fetch_all(db)
        metrics.inc(metrics.ARTICLES_FETCHED, len(articles))

        tg_client = _tg_client.get()
        if tg_client is not None:
            tg_articles = await fetch_all_tg(db, tg_client)
            metrics.inc(metrics.ARTICLES_FETCHED, len(tg_articles))
            articles.extend(tg_articles)

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
    Also fires alerts for dead sources and publish silence.
    No-ops if TELEGRAM_OPS_CHAT_ID is not configured.
    """
    global _alerted_dead, _last_silence_alert

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

        # ── silence tracking ─────────────────────────────────────────────────
        last_ok = queries.get_last_ok_send_at(db)
        now = datetime.now(timezone.utc)
        if last_ok:
            silence_s = (now - last_ok).total_seconds()
            sil_h, sil_rem = divmod(int(silence_s), 3600)
            sil_m = sil_rem // 60
            last_sent_str = f"{sil_h}h {sil_m}m ago" if sil_h else f"{sil_m}m ago"
        else:
            silence_s = float("inf")
            last_sent_str = "never"

        header = "🚨" if dead_n else ("⚠️" if silence_s >= _SILENCE_WARN_HOURS * 3600 else "✅")

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
            f"📊 published: {tg_ok} total · {tg_fail} failed\n"
            f"🕐 last msg: {last_sent_str}"
        )
        await _send_ops(text)

        # ── silence alert (deduplicated by cooldown) ──────────────────────────
        if silence_s >= _SILENCE_ALERT_HOURS * 3600:
            cooldown_passed = (
                _last_silence_alert is None
                or (now - _last_silence_alert).total_seconds() >= _SILENCE_ALERT_COOLDOWN_H * 3600
            )
            if cooldown_passed:
                sil_h, sil_rem = divmod(int(silence_s), 3600)
                sil_m = sil_rem // 60
                await _send_ops(
                    f"⚠️ No messages published for {sil_h}h {sil_m}m\n"
                    f"Pipeline is running but nothing passed relevance/AI gate.\n"
                    f"Check: /health or logs for 'relevance_gate_silence' / 'ai_gate_silence'."
                )
                _last_silence_alert = now
                logger.warning(
                    "silence alert fired: no publish for %.0f h", silence_s / 3600,
                    extra={"event": "silence_alert", "silence_hours": round(silence_s / 3600, 1)},
                )

        # ── one-time alert per dead source (resets on service restart) ────────
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
        clusters = [c for c in candidates if is_russia_relevant(c)][:_DIGEST_LIMIT]

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

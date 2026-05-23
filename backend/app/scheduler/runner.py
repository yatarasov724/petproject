"""
APScheduler setup and lifecycle.

Keeps scheduler state in module scope (singleton).
start() / stop() are called from main.py startup/shutdown hooks.
"""

import logging
from functools import partial

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.scheduler.jobs import (
    poll_job, cleanup_job, backup_job, heartbeat_job, digest_job,
    bot_commands_job, price_snapshot_job,
    calendar_sync_job, calendar_notify_job, calendar_digest_job,
)

logger = logging.getLogger(__name__)

_scheduler = AsyncIOScheduler()


def start() -> None:
    _scheduler.add_job(
        poll_job,
        trigger="interval",
        seconds=60,
        id="poll_rss",
        max_instances=1,        # never run two polls concurrently
        coalesce=True,          # if a run was missed, run once not multiple
    )
    _scheduler.add_job(
        cleanup_job,
        trigger="interval",
        hours=24,
        id="cleanup",
        max_instances=1,
    )
    _scheduler.add_job(
        backup_job,
        trigger="interval",
        hours=6,
        id="backup",
        max_instances=1,
    )
    _scheduler.add_job(
        heartbeat_job,
        trigger="interval",
        minutes=5,
        id="heartbeat",
        max_instances=1,
        misfire_grace_time=None,  # always run even if delayed by a slow poll cycle
    )
    # Daily digest at 22:00 MSK (19:00 UTC) — top-10 events of the full day
    _scheduler.add_job(
        partial(digest_job, within_hours=24, label="22:00"),
        trigger="cron",
        hour=19,
        minute=0,
        timezone="UTC",
        id="digest_2200",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        bot_commands_job,
        trigger="interval",
        seconds=10,
        id="bot_commands",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        price_snapshot_job,
        trigger="interval",
        hours=1,
        id="price_snapshots",
        max_instances=1,
        coalesce=True,
    )
    # ── Corporate events calendar ────────────────────────────────────────────
    # Nightly sync: 02:00 UTC (05:00 MSK)
    _scheduler.add_job(
        calendar_sync_job,
        trigger="cron",
        hour=2, minute=0,
        timezone="UTC",
        id="calendar_sync",
        max_instances=1,
        coalesce=True,
    )

    # Daily notify: 06:00 UTC (09:00 MSK)
    _scheduler.add_job(
        calendar_notify_job,
        trigger="cron",
        hour=6, minute=0,
        timezone="UTC",
        id="calendar_notify",
        max_instances=1,
        coalesce=True,
    )

    # Weekly digest: Sunday 16:00 UTC (19:00 MSK)
    _scheduler.add_job(
        calendar_digest_job,
        trigger="cron",
        day_of_week="sun",
        hour=16, minute=0,
        timezone="UTC",
        id="calendar_digest",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started (poll=60s, cleanup=24h, backup=6h, heartbeat=5m, "
        "digest=22:00 MSK, bot=10s, price_snapshots=1h, "
        "calendar_sync=02:00 UTC, calendar_notify=06:00 UTC, calendar_digest=Sun 16:00 UTC)"
    )


def stop() -> None:
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")

"""
APScheduler setup and lifecycle.

Keeps scheduler state in module scope (singleton).
start() / stop() are called from main.py startup/shutdown hooks.
"""

import logging
from functools import partial

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.scheduler.jobs import poll_job, cleanup_job, backup_job, heartbeat_job, digest_job

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
    )
    # Digest at 18:30 MSK (15:30 UTC) — covers ~last 9h of the trading session
    _scheduler.add_job(
        partial(digest_job, within_hours=9, label="18:30"),
        trigger="cron",
        hour=15,
        minute=30,
        timezone="UTC",
        id="digest_1830",
        max_instances=1,
        coalesce=True,
    )
    # Digest at 22:00 MSK (19:00 UTC) — covers the full trading day (12h)
    _scheduler.add_job(
        partial(digest_job, within_hours=12, label="22:00"),
        trigger="cron",
        hour=19,
        minute=0,
        timezone="UTC",
        id="digest_2200",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started (poll=60s, cleanup=24h, backup=6h, heartbeat=5m, digest=18:30+22:00 MSK)"
    )


def stop() -> None:
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")

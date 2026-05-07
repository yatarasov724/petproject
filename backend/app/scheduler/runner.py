"""
APScheduler setup and lifecycle.

Keeps scheduler state in module scope (singleton).
start() / stop() are called from main.py startup/shutdown hooks.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.scheduler.jobs import poll_job, cleanup_job, backup_job, heartbeat_job

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
    _scheduler.start()
    logger.info("Scheduler started (poll=60s, cleanup=24h, backup=6h, heartbeat=5m)")


def stop() -> None:
    _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")

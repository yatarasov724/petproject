"""
Application entrypoint.

Startup sequence
────────────────
  1. logging_setup.configure()  — JSON or text, level from env
  2. init_db()                  — CREATE TABLE IF NOT EXISTS (idempotent)
  3. seed_sources()             — INSERT OR IGNORE rss_sources (idempotent)
  4. runner.start()             — register APScheduler jobs

Shutdown
────────
  1. runner.stop()              — graceful scheduler shutdown
"""

import sqlite3

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core import logging_setup, metrics
from app.db.database import init_db, get_db
from app.db.queries import seed_sources
from app.scheduler import runner

# ── logging — must be first ───────────────────────────────────────────────────
logging_setup.configure(
    json_logs=(settings.log_format.lower() == "json"),
    level=settings.log_level,
)

import logging
logger = logging.getLogger(__name__)

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="MOEX News Assistant")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── lifecycle ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    init_db()

    db = get_db()
    try:
        seed_sources(db)
    finally:
        db.close()

    runner.start()
    logger.info("app started", extra={"event": "app_started"})


@app.on_event("shutdown")
async def shutdown() -> None:
    runner.stop()
    logger.info("app stopped", extra={"event": "app_stopped"})


# ── routes ─────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "MOEX News Assistant"}


@app.get("/health")
def health():
    """Operational health check: DB stats + process-lifetime counters."""
    db = get_db()
    try:
        source_rows = db.execute(
            "SELECT status, COUNT(*) AS n FROM rss_sources GROUP BY status"
        ).fetchall()
        clusters_24h = db.execute(
            "SELECT COUNT(*) FROM event_clusters "
            "WHERE first_seen_at >= datetime('now', '-24 hours')"
        ).fetchone()[0]
        sends_24h = db.execute(
            "SELECT COUNT(*) FROM telegram_sends "
            "WHERE sent_at >= datetime('now', '-24 hours')"
        ).fetchone()[0]
        sends_ok = db.execute(
            "SELECT COUNT(*) FROM telegram_sends "
            "WHERE sent_at >= datetime('now', '-24 hours') AND ok = 1"
        ).fetchone()[0]

        return {
            "status": "ok",
            "rss_sources": {row["status"]: row["n"] for row in source_rows},
            "clusters_24h": clusters_24h,
            "sends_24h":    sends_24h,
            "sends_ok_24h": sends_ok,
            "counters":     metrics.snapshot(),
        }
    except sqlite3.Error as exc:
        logger.error("health check db error: %s", exc)
        return {"status": "error", "detail": str(exc)}
    finally:
        db.close()

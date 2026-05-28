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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core import logging_setup, metrics
import app.core.tg_client as _tg_client
from app.db.database import init_db, get_db
from app.db.queries import seed_sources
from app.scheduler import runner
from app.api.routes.admin import router as admin_router
import aiohttp

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


async def _set_my_commands() -> None:
    """Register bot commands in Telegram so they appear in the / menu."""
    if settings.dry_run:
        return
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/setMyCommands"
    commands = [
        {"command": "portfolio", "description": "Мой портфель"},
        {"command": "calendar",  "description": "Ближайшие события"},
        {"command": "settings",  "description": "Настройки"},
        {"command": "help",      "description": "Помощь"},
        {"command": "feedback",  "description": "Обратная связь"},
    ]
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            await session.post(url, json={"commands": commands})
        logger.info("bot commands registered", extra={"event": "set_my_commands_ok"})
    except Exception as exc:
        logger.warning("setMyCommands failed: %s", exc)


@app.on_event("startup")
async def startup() -> None:
    init_db()

    db = get_db()
    try:
        seed_sources(db)
    finally:
        db.close()

    await _tg_client.connect()  # no-op if TG credentials are not configured
    await _set_my_commands()
    runner.start()
    logger.info("app started", extra={"event": "app_started"})


@app.on_event("shutdown")
async def shutdown() -> None:
    runner.stop()
    await _tg_client.disconnect()
    logger.info("app stopped", extra={"event": "app_stopped"})


# ── routers ────────────────────────────────────────────────────────────────────

app.include_router(admin_router)

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
        from datetime import datetime, timedelta, timezone
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

        clusters_24h = db.execute(
            "SELECT COUNT(*) AS n FROM event_clusters WHERE first_seen_at >= %s",
            (cutoff,),
        ).fetchone()["n"]
        sends_24h = db.execute(
            "SELECT COUNT(*) AS n FROM telegram_sends WHERE sent_at >= %s",
            (cutoff,),
        ).fetchone()["n"]
        sends_ok = db.execute(
            "SELECT COUNT(*) AS n FROM telegram_sends WHERE sent_at >= %s AND ok = 1",
            (cutoff,),
        ).fetchone()["n"]

        return {
            "status": "ok",
            "rss_sources": {row["status"]: row["n"] for row in source_rows},
            "clusters_24h": clusters_24h,
            "sends_24h":    sends_24h,
            "sends_ok_24h": sends_ok,
            "counters":     metrics.snapshot(),
        }
    except Exception as exc:
        logger.error("health check db error: %s", exc)
        return {"status": "error", "detail": str(exc)}
    finally:
        db.close()

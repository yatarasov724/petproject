"""
Admin API for managing RSS sources at runtime without restarting the service.

All endpoints require the X-Admin-Key header matching settings.secret_key.
Default key is "changeme" — set SECRET_KEY in .env before exposing this API.

Endpoints
─────────
  GET    /admin/sources          — list all sources
  POST   /admin/sources          — add a new source
  PUT    /admin/sources/{id}     — update url / enabled flag
  DELETE /admin/sources/{id}     — soft-disable (enabled=0)
  POST   /admin/sources/{id}/reset — clear backoff state
"""

import psycopg2
import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, HttpUrl

from app.core.config import settings
from app.db.database import get_db, DBConnection
from app.db import queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ── auth ──────────────────────────────────────────────────────────────────────

def _require_admin(x_admin_key: str = Header(alias="X-Admin-Key")) -> None:
    if x_admin_key != settings.secret_key:
        raise HTTPException(status_code=403, detail="Invalid admin key")


# ── request / response models ─────────────────────────────────────────────────

class SourceCreate(BaseModel):
    name: str
    url: HttpUrl


class SourceUpdate(BaseModel):
    url:     HttpUrl | None = None
    enabled: bool    | None = None


class SourceResponse(BaseModel):
    id:              int
    name:            str
    url:             str
    enabled:         bool
    status:          str
    error_count:     int
    last_fetched_at: str | None


def _row_to_response(row: Any) -> SourceResponse:
    return SourceResponse(
        id=row["id"],
        name=row["name"],
        url=row["url"],
        enabled=bool(row["enabled"]),
        status=row["status"],
        error_count=row["error_count"],
        last_fetched_at=row["last_fetched_at"],
    )


# ── DB dependency ─────────────────────────────────────────────────────────────

def _get_db():
    db = get_db()
    try:
        yield db
    finally:
        db.close()


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/sources", response_model=list[SourceResponse])
def list_sources(
    _: None = Depends(_require_admin),
    db: DBConnection = Depends(_get_db),
):
    return [_row_to_response(row) for row in queries.get_all_sources(db)]


@router.post("/sources", response_model=SourceResponse, status_code=201)
def create_source(
    body: SourceCreate,
    _: None = Depends(_require_admin),
    db: DBConnection = Depends(_get_db),
):
    try:
        new_id = queries.add_source(db, name=body.name, url=str(body.url))
    except psycopg2.IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Source name or URL already exists")

    row = queries.get_all_sources(db)
    source = next((r for r in row if r["id"] == new_id), None)
    if source is None:
        raise HTTPException(status_code=500, detail="Source created but not found")

    logger.info("admin: source created", extra={"source_id": new_id, "source_name": body.name})
    return _row_to_response(source)


@router.put("/sources/{source_id}", response_model=SourceResponse)
def update_source(
    source_id: int,
    body: SourceUpdate,
    _: None = Depends(_require_admin),
    db: DBConnection = Depends(_get_db),
):
    if body.url is None and body.enabled is None:
        raise HTTPException(status_code=422, detail="Provide at least one of: url, enabled")

    try:
        updated = queries.update_source(
            db, source_id,
            url=str(body.url) if body.url is not None else None,
            enabled=body.enabled,
        )
    except psycopg2.IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="URL already used by another source")

    if not updated:
        raise HTTPException(status_code=404, detail="Source not found")

    rows = queries.get_all_sources(db)
    source = next((r for r in rows if r["id"] == source_id), None)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")

    logger.info("admin: source updated", extra={"source_id": source_id})
    return _row_to_response(source)


@router.delete("/sources/{source_id}", status_code=204)
def disable_source(
    source_id: int,
    _: None = Depends(_require_admin),
    db: DBConnection = Depends(_get_db),
):
    if not queries.disable_source(db, source_id):
        raise HTTPException(status_code=404, detail="Source not found")
    logger.info("admin: source disabled", extra={"source_id": source_id})


@router.post("/sources/{source_id}/reset", response_model=SourceResponse)
def reset_source(
    source_id: int,
    _: None = Depends(_require_admin),
    db: DBConnection = Depends(_get_db),
):
    if not queries.reset_source_backoff(db, source_id):
        raise HTTPException(status_code=404, detail="Source not found")

    rows = queries.get_all_sources(db)
    source = next((r for r in rows if r["id"] == source_id), None)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")

    logger.info("admin: source backoff reset", extra={"source_id": source_id})
    return _row_to_response(source)


# ── test channel send ─────────────────────────────────────────────────────────

from app.telegram.client import send_text


@router.post("/test_channel", dependencies=[Depends(_require_admin)])
async def test_channel():
    """
    Send a test message to the configured Telegram channel.
    Use this to verify TELEGRAM_CHANNEL_ID and bot permissions are correct.
    """
    msg_id = await send_text("🔧 *Тест канала*\n\nКанал настроен корректно\\.")
    if msg_id is None:
        raise HTTPException(
            status_code=502,
            detail="Failed to send to Telegram channel — check bot token and channel ID",
        )
    return {
        "status": "ok",
        "message_id": msg_id,
        "channel_id": settings.telegram_channel_id,
    }


# ── pipeline diagnostics ──────────────────────────────────────────────────────

@router.get("/pipeline/clusters", dependencies=[Depends(_require_admin)])
def pipeline_clusters(
    limit: int = 20,
    db: DBConnection = Depends(_get_db),
):
    """
    Return the most recently created event clusters with pipeline outcomes.
    Fields: status (new/published/updated), sent_ok (True/None), decision, score.
    """
    rows = queries.get_recent_clusters(db, limit=min(limit, 100))
    # Serialise datetime/date objects to ISO strings
    serialised = []
    for row in rows:
        serialised.append({
            k: v.isoformat() if hasattr(v, "isoformat") else v
            for k, v in row.items()
        })
    return {"count": len(serialised), "clusters": serialised}

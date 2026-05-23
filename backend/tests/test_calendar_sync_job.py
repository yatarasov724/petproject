"""Tests for calendar_sync_job."""
import pytest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from app.calendar.moex_client import CorporateEvent
from app.db import queries


def _make_user_with_ticker(db, telegram_id: int, ticker: str) -> int:
    cur = db.execute(
        "INSERT INTO users (telegram_id, username) VALUES (%s, 'u') RETURNING id",
        (telegram_id,),
    )
    uid = cur.fetchone()["id"]
    db.execute(
        "INSERT INTO portfolio_subscriptions (user_id, ticker) VALUES (%s, %s)",
        (uid, ticker),
    )
    db.commit()
    return uid


@pytest.mark.asyncio
async def test_sync_job_stores_events(db):
    _make_user_with_ticker(db, 100, "SBER")
    mock_events = [
        CorporateEvent("SBER", "dividend_cutoff",
                       date.today() + timedelta(days=10), {"amount": 33.58, "currency": "RUB"}),
    ]
    with patch("app.calendar.moex_client.MoexIssClient.fetch_all",
               new_callable=AsyncMock, return_value=mock_events):
        from app.scheduler.jobs import calendar_sync_job
        await calendar_sync_job()

    rows = db.execute(
        "SELECT * FROM corporate_events WHERE ticker = 'SBER'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "dividend_cutoff"


@pytest.mark.asyncio
async def test_sync_job_upserts_on_repeat_run(db):
    _make_user_with_ticker(db, 101, "GAZP")
    ev = CorporateEvent("GAZP", "earnings",
                        date.today() + timedelta(days=5), {"report_type": "МСФО"})

    with patch("app.calendar.moex_client.MoexIssClient.fetch_all",
               new_callable=AsyncMock, return_value=[ev]):
        from app.scheduler.jobs import calendar_sync_job
        await calendar_sync_job()
        await calendar_sync_job()  # second run — should upsert, not duplicate

    count = db.execute(
        "SELECT COUNT(*) FROM corporate_events WHERE ticker='GAZP'"
    ).fetchone()["count"]
    assert count == 1


@pytest.mark.asyncio
async def test_sync_job_noop_when_no_portfolio(db):
    # No portfolio_subscriptions → nothing to sync
    from app.scheduler.jobs import calendar_sync_job
    await calendar_sync_job()  # must not raise
    count = db.execute(
        "SELECT COUNT(*) FROM corporate_events"
    ).fetchone()["count"]
    assert count == 0

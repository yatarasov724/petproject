"""Tests for weekly calendar digest job."""
import pytest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

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
async def test_digest_job_sends_when_events_exist(db):
    _make_user_with_ticker(db, 300, "SBER")
    today = date.today()
    queries.upsert_corporate_event(  # past event
        db, "SBER", "dividend_cutoff", today - timedelta(days=3),
        {"amount": 33.58, "currency": "RUB"},
    )
    queries.upsert_corporate_event(  # upcoming event
        db, "SBER", "earnings", today + timedelta(days=4),
        {"report_type": "МСФО"},
    )

    with patch("app.scheduler.jobs.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.scheduler.jobs import calendar_digest_job
        await calendar_digest_job()

    assert mock_dm.called
    text = mock_dm.call_args[0][1]
    assert "SBER" in text


@pytest.mark.asyncio
async def test_digest_job_skips_user_with_no_events(db):
    _make_user_with_ticker(db, 301, "GAZP")
    # No events for GAZP within ±7 days

    with patch("app.scheduler.jobs.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.scheduler.jobs import calendar_digest_job
        await calendar_digest_job()

    assert not mock_dm.called


@pytest.mark.asyncio
async def test_digest_job_skips_user_with_no_portfolio(db):
    # No portfolio subscriptions at all
    with patch("app.scheduler.jobs.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.scheduler.jobs import calendar_digest_job
        await calendar_digest_job()
    assert not mock_dm.called

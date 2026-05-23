"""Tests for calendar DM formatting and notify job."""
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


# ── format_calendar_dm ────────────────────────────────────────────────────────

def test_format_dm_dividend_cutoff_contains_key_fields():
    from app.calendar.notify import format_calendar_dm
    text = format_calendar_dm(
        ticker="SBER", event_type="dividend_cutoff",
        event_date=date(2099, 6, 20),
        details={"amount": 33.58, "currency": "RUB"},
        days_ahead=3,
    )
    assert "SBER" in text
    assert "33" in text   # amount
    assert "3" in text    # days_ahead


def test_format_dm_earnings_shows_report_type():
    from app.calendar.notify import format_calendar_dm
    text = format_calendar_dm(
        ticker="GAZP", event_type="earnings",
        event_date=date(2099, 5, 28),
        details={"report_type": "МСФО"},
        days_ahead=3,
    )
    assert "GAZP" in text
    assert "МСФО" in text


def test_format_dm_buyback_contains_label():
    from app.calendar.notify import format_calendar_dm
    text = format_calendar_dm(
        ticker="LKOH", event_type="buyback",
        event_date=date(2099, 7, 1),
        details={},
        days_ahead=3,
    )
    assert "LKOH" in text


# ── calendar_notify_job ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_notify_job_sends_dm_for_event_in_3_days(db):
    _make_user_with_ticker(db, 200, "SBER")
    queries.upsert_corporate_event(
        db, "SBER", "dividend_cutoff",
        date.today() + timedelta(days=3),
        {"amount": 33.58, "currency": "RUB"},
    )

    with patch("app.calendar.notify.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.scheduler.jobs import calendar_notify_job
        await calendar_notify_job()

    assert mock_dm.called
    assert mock_dm.call_args[0][0] == 200   # telegram_id
    assert "SBER" in mock_dm.call_args[0][1]


@pytest.mark.asyncio
async def test_notify_job_no_duplicate_on_second_run(db):
    uid = _make_user_with_ticker(db, 201, "GAZP")
    eid = queries.upsert_corporate_event(
        db, "GAZP", "earnings",
        date.today() + timedelta(days=3),
        {"report_type": "МСФО"},
    )

    with patch("app.calendar.notify.send_dm",
               new_callable=AsyncMock, return_value=1):
        from app.scheduler.jobs import calendar_notify_job
        await calendar_notify_job()
        await calendar_notify_job()  # second run

    count = db.execute(
        "SELECT COUNT(*) FROM calendar_notifications_sent WHERE user_id=%s AND event_id=%s",
        (uid, eid),
    ).fetchone()["count"]
    assert count == 1


@pytest.mark.asyncio
async def test_notify_job_skips_event_not_in_3_days(db):
    _make_user_with_ticker(db, 202, "LKOH")
    queries.upsert_corporate_event(
        db, "LKOH", "dividend_cutoff",
        date.today() + timedelta(days=7),  # 7 days, not 3
        {"amount": 10.0},
    )

    with patch("app.calendar.notify.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.scheduler.jobs import calendar_notify_job
        await calendar_notify_job()

    assert not mock_dm.called

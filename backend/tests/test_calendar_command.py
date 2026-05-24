"""Tests for /calendar bot command."""
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
        (telegram_id, ticker),
    )
    db.commit()
    return uid


@pytest.mark.asyncio
async def test_calendar_empty_portfolio_sends_prompt(db):
    """User with no tickers receives a prompt to use /portfolio."""
    db.execute("INSERT INTO users (telegram_id, username) VALUES (300, 'u')")
    db.commit()

    with patch("app.bot.commands.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.bot.commands import _handle_calendar
        await _handle_calendar(db, 300)

    mock_dm.assert_called_once()
    text = mock_dm.call_args[0][1]
    assert "портфель пуст" in text.lower()
    assert "/portfolio" in text


@pytest.mark.asyncio
async def test_calendar_no_upcoming_events_sends_empty_message(db):
    """User has tickers but no events in corporate_events → friendly empty message."""
    _make_user_with_ticker(db, 301, "SBER")

    with patch("app.bot.commands.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.bot.commands import _handle_calendar
        await _handle_calendar(db, 301)

    mock_dm.assert_called_once()
    text = mock_dm.call_args[0][1]
    assert "нет событий" in text.lower()


@pytest.mark.asyncio
async def test_calendar_shows_events_with_ticker_and_amount(db):
    """User with a dividend event sees the ticker and dividend amount in the reply."""
    _make_user_with_ticker(db, 302, "LKOH")
    queries.upsert_corporate_event(
        db, "LKOH", "dividend_cutoff",
        date.today() + timedelta(days=10),
        {"amount": 945, "currency": "RUB"},
    )

    with patch("app.bot.commands.send_dm",
               new_callable=AsyncMock, return_value=1) as mock_dm:
        from app.bot.commands import _handle_calendar
        await _handle_calendar(db, 302)

    mock_dm.assert_called_once()
    text = mock_dm.call_args[0][1]
    assert "LKOH" in text
    assert "945" in text

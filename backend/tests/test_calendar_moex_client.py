"""Tests for MOEX ISS API client (aiohttp mocked)."""
import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from app.calendar.moex_client import CorporateEvent, MoexIssClient


# ── mock helpers ──────────────────────────────────────────────────────────────

def _mock_response(payload: dict, status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=payload)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _mock_session(resp):
    session = MagicMock()
    session.get = MagicMock(return_value=resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


# ── fetch_dividends ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_dividends_returns_cutoff_and_payment():
    payload = {
        "dividends": {
            "columns": ["secid", "isin", "registryclosedate", "value", "currencyid",
                        "decisiondate", "fixdate"],
            "data": [["SBER", "RU0009029540", "2099-06-20", 33.58, "RUB",
                      "2099-04-01", "2099-07-15"]],
        }
    }
    with patch("aiohttp.ClientSession", return_value=_mock_session(_mock_response(payload))):
        events = await MoexIssClient().fetch_dividends("SBER")

    cutoffs  = [e for e in events if e.event_type == "dividend_cutoff"]
    payments = [e for e in events if e.event_type == "dividend_payment"]
    assert len(cutoffs) == 1
    assert cutoffs[0].ticker == "SBER"
    assert cutoffs[0].event_date == date(2099, 6, 20)
    assert cutoffs[0].details["amount"] == 33.58
    assert cutoffs[0].details["currency"] == "RUB"
    assert len(payments) == 1
    assert payments[0].event_date == date(2099, 7, 15)


@pytest.mark.asyncio
async def test_fetch_dividends_skips_past_events():
    payload = {
        "dividends": {
            "columns": ["secid", "isin", "registryclosedate", "value", "currencyid",
                        "decisiondate", "fixdate"],
            "data": [["SBER", "RU...", "2020-01-10", 10.0, "RUB", "2019-12-01", "2020-02-01"]],
        }
    }
    with patch("aiohttp.ClientSession", return_value=_mock_session(_mock_response(payload))):
        events = await MoexIssClient().fetch_dividends("SBER")
    assert events == []


@pytest.mark.asyncio
async def test_fetch_dividends_empty_data():
    payload = {"dividends": {"columns": [], "data": []}}
    with patch("aiohttp.ClientSession", return_value=_mock_session(_mock_response(payload))):
        events = await MoexIssClient().fetch_dividends("SBER")
    assert events == []


@pytest.mark.asyncio
async def test_fetch_dividends_http_error_returns_empty():
    with patch("aiohttp.ClientSession", return_value=_mock_session(_mock_response({}, status=500))):
        events = await MoexIssClient().fetch_dividends("SBER")
    assert events == []


# ── fetch_events ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_events_returns_earnings():
    payload = {
        "events": {
            "columns": ["id", "event_type", "title", "date_start", "secid"],
            "data": [[1, "earning", "Отчёт МСФО за Q1 2099", "2099-05-28", "GAZP"]],
        }
    }
    with patch("aiohttp.ClientSession", return_value=_mock_session(_mock_response(payload))):
        events = await MoexIssClient().fetch_events("GAZP", days_ahead=90)

    assert len(events) == 1
    assert events[0].event_type == "earnings"
    assert events[0].ticker == "GAZP"
    assert events[0].event_date == date(2099, 5, 28)
    assert events[0].details.get("report_type") == "МСФО"


@pytest.mark.asyncio
async def test_fetch_events_network_error_returns_empty(caplog):
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.get.side_effect = Exception("connection refused")
    with patch("aiohttp.ClientSession", return_value=session):
        events = await MoexIssClient().fetch_events("SBER", days_ahead=90)
    assert events == []

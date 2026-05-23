"""Tests for calendar DB queries."""
import json
import pytest
from datetime import date, timedelta
from app.db import queries
from app.db.database import DBConnection


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_user(db: DBConnection, telegram_id: int = 111) -> int:
    cur = db.execute(
        "INSERT INTO users (telegram_id, username) VALUES (%s, %s) RETURNING id",
        (telegram_id, "testuser"),
    )
    uid = cur.fetchone()["id"]
    db.commit()
    return uid


def _subscribe(db: DBConnection, user_id: int, ticker: str) -> None:
    db.execute(
        "INSERT INTO portfolio_subscriptions (user_id, ticker) VALUES (%s, %s)",
        (user_id, ticker),
    )
    db.commit()


def _make_event(
    db: DBConnection,
    ticker: str = "SBER",
    event_type: str = "dividend_cutoff",
    days: int = 3,
    details: dict | None = None,
) -> int:
    return queries.upsert_corporate_event(
        db,
        ticker=ticker,
        event_type=event_type,
        event_date=date.today() + timedelta(days=days),
        details=details or {"amount": 33.58, "currency": "RUB"},
    )


# ── upsert_corporate_event ────────────────────────────────────────────────────

def test_upsert_inserts_new_event(db):
    eid = queries.upsert_corporate_event(
        db,
        ticker="GAZP",
        event_type="dividend_cutoff",
        event_date=date(2026, 6, 15),
        details={"amount": 15.33, "currency": "RUB"},
    )
    assert isinstance(eid, int)
    row = db.execute("SELECT * FROM corporate_events WHERE id = %s", (eid,)).fetchone()
    assert row["ticker"] == "GAZP"
    assert row["event_type"] == "dividend_cutoff"


def test_upsert_updates_existing_event(db):
    eid1 = queries.upsert_corporate_event(
        db, "GAZP", "dividend_cutoff", date(2026, 6, 15), {"amount": 15.0}
    )
    eid2 = queries.upsert_corporate_event(
        db, "GAZP", "dividend_cutoff", date(2026, 6, 15), {"amount": 16.0}
    )
    assert eid1 == eid2  # same row updated
    row = db.execute(
        "SELECT details FROM corporate_events WHERE id = %s", (eid1,)
    ).fetchone()
    details = row["details"] if isinstance(row["details"], dict) else json.loads(row["details"])
    assert details["amount"] == 16.0


# ── get_all_portfolio_tickers ─────────────────────────────────────────────────

def test_get_all_portfolio_tickers_returns_unique(db):
    uid = _make_user(db, 222)
    _subscribe(db, uid, "SBER")
    _subscribe(db, uid, "GAZP")
    tickers = queries.get_all_portfolio_tickers(db)
    assert set(tickers) >= {"SBER", "GAZP"}


def test_get_all_portfolio_tickers_empty(db):
    assert queries.get_all_portfolio_tickers(db) == []


# ── get_pending_calendar_notifications ───────────────────────────────────────

def test_pending_notifications_returns_matching(db):
    uid = _make_user(db, 333)
    _subscribe(db, uid, "SBER")
    eid = _make_event(db, ticker="SBER", days=3)
    rows = queries.get_pending_calendar_notifications(db, days_ahead=3)
    assert any(r["event_id"] == eid and r["telegram_id"] == 333 for r in rows)


def test_pending_notifications_skips_already_sent(db):
    uid = _make_user(db, 444)
    _subscribe(db, uid, "SBER")
    eid = _make_event(db, ticker="SBER", days=3)
    queries.mark_calendar_notification_sent(db, uid, eid)
    rows = queries.get_pending_calendar_notifications(db, days_ahead=3)
    assert not any(r["event_id"] == eid and r["telegram_id"] == 444 for r in rows)


def test_pending_notifications_ignores_wrong_date(db):
    uid = _make_user(db, 555)
    _subscribe(db, uid, "SBER")
    _make_event(db, ticker="SBER", days=5)  # 5 days out, not 3
    rows = queries.get_pending_calendar_notifications(db, days_ahead=3)
    assert rows == []


# ── mark_calendar_notification_sent ──────────────────────────────────────────

def test_mark_sent_is_idempotent(db):
    uid = _make_user(db, 666)
    eid = _make_event(db)
    queries.mark_calendar_notification_sent(db, uid, eid)
    queries.mark_calendar_notification_sent(db, uid, eid)  # must not raise
    count = db.execute(
        "SELECT COUNT(*) FROM calendar_notifications_sent WHERE user_id=%s AND event_id=%s",
        (uid, eid),
    ).fetchone()["count"]
    assert count == 1


# ── get_portfolio_events_for_user ─────────────────────────────────────────────

def test_get_portfolio_events_only_returns_users_tickers(db):
    uid = _make_user(db, 777)
    _subscribe(db, uid, "LKOH")
    today = date.today()
    queries.upsert_corporate_event(db, "LKOH", "earnings", today + timedelta(days=2),
                                   {"report_type": "МСФО"})
    queries.upsert_corporate_event(db, "SBER", "dividend_cutoff", today + timedelta(days=2),
                                   {"amount": 33.0})  # SBER — not in user's portfolio
    events = queries.get_portfolio_events_for_user(
        db, 777, from_date=today, to_date=today + timedelta(days=7)
    )
    assert len(events) == 1
    assert events[0]["ticker"] == "LKOH"

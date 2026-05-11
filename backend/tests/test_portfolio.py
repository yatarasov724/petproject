"""
Tests for portfolio subscription CRUD and notification dispatch.
"""

import pytest
from unittest.mock import AsyncMock, patch

from app.db import queries


# ── subscription CRUD ─────────────────────────────────────────────────────────

def test_set_and_get_tickers(db):
    queries.set_user_tickers(db, 111, ["GAZP", "SBER"])
    result = queries.get_user_tickers(db, 111)
    assert result == ["GAZP", "SBER"]


def test_set_tickers_replaces_previous(db):
    queries.set_user_tickers(db, 111, ["GAZP", "SBER"])
    queries.set_user_tickers(db, 111, ["LKOH"])
    result = queries.get_user_tickers(db, 111)
    assert result == ["LKOH"]


def test_set_tickers_uppercases(db):
    queries.set_user_tickers(db, 111, ["gazp", "sber"])
    result = queries.get_user_tickers(db, 111)
    assert "GAZP" in result
    assert "SBER" in result


def test_clear_user_tickers(db):
    queries.set_user_tickers(db, 111, ["GAZP"])
    queries.clear_user_tickers(db, 111)
    assert queries.get_user_tickers(db, 111) == []


def test_clear_nonexistent_user(db):
    queries.clear_user_tickers(db, 999)  # must not raise


def test_get_subscribed_users_match(db):
    queries.set_user_tickers(db, 111, ["GAZP", "LKOH"])
    queries.set_user_tickers(db, 222, ["SBER"])
    users = queries.get_subscribed_users(db, ["GAZP"])
    assert 111 in users
    assert 222 not in users


def test_get_subscribed_users_multi_ticker(db):
    queries.set_user_tickers(db, 111, ["GAZP"])
    queries.set_user_tickers(db, 222, ["SBER"])
    users = queries.get_subscribed_users(db, ["GAZP", "SBER"])
    assert 111 in users
    assert 222 in users


def test_get_subscribed_users_no_match(db):
    queries.set_user_tickers(db, 111, ["GAZP"])
    users = queries.get_subscribed_users(db, ["NVTK"])
    assert users == []


def test_get_subscribed_users_empty_list(db):
    users = queries.get_subscribed_users(db, [])
    assert users == []


def test_get_subscribed_users_deduplicates(db):
    # User subscribed to two tickers both in the cluster → returned once
    queries.set_user_tickers(db, 111, ["GAZP", "SBER"])
    users = queries.get_subscribed_users(db, ["GAZP", "SBER"])
    assert users.count(111) == 1


def test_subscriptions_isolated_per_user(db):
    queries.set_user_tickers(db, 111, ["GAZP"])
    queries.set_user_tickers(db, 222, ["LKOH"])
    assert queries.get_user_tickers(db, 111) == ["GAZP"]
    assert queries.get_user_tickers(db, 222) == ["LKOH"]


# ── notification dispatch ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_notify_sends_dm_to_subscribers(db):
    queries.set_user_tickers(db, 111, ["SBER"])
    queries.set_user_tickers(db, 222, ["GAZP"])

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", new_callable=AsyncMock) as mock_dm,
    ):
        from app.bot.portfolio import notify
        await notify("SBER,GAZP", "Банки под давлением", cluster_id=1)

    assert mock_dm.call_count == 2
    called_user_ids = {call.args[0] for call in mock_dm.call_args_list}
    assert called_user_ids == {111, 222}


@pytest.mark.asyncio
async def test_notify_skips_no_subscribers(db):
    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", new_callable=AsyncMock) as mock_dm,
    ):
        from app.bot.portfolio import notify
        await notify("GAZP", "Газпром снижает дивиденды", cluster_id=2)

    mock_dm.assert_not_called()


@pytest.mark.asyncio
async def test_notify_skips_empty_tickers():
    with patch("app.bot.portfolio.send_dm", new_callable=AsyncMock) as mock_dm:
        from app.bot.portfolio import notify
        await notify("", "Заголовок без тикеров", cluster_id=3)

    mock_dm.assert_not_called()


@pytest.mark.asyncio
async def test_notify_message_contains_ticker_and_title(db):
    queries.set_user_tickers(db, 111, ["SBER"])

    sent_texts: list[str] = []

    async def capture_dm(user_id, text):
        sent_texts.append(text)
        return True

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
    ):
        from app.bot.portfolio import notify
        await notify("SBER", "Сбер снижает прибыль", cluster_id=4)

    assert len(sent_texts) == 1
    assert "SBER" in sent_texts[0]
    assert "Сбер" in sent_texts[0]


# ── command handler ───────────────────────────────────────────────────────────

def _make_update(user_id: int, text: str, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "from": {"id": user_id},
            "text": text,
        },
    }


@pytest.mark.asyncio
async def test_command_start_sends_welcome(db):
    with patch("app.bot.commands.send_dm", new_callable=AsyncMock) as mock_dm:
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/start"))

    mock_dm.assert_called_once()
    assert 111 == mock_dm.call_args.args[0]


@pytest.mark.asyncio
async def test_command_portfolio_subscribe(db):
    with patch("app.bot.commands.send_dm", new_callable=AsyncMock):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/portfolio GAZP SBER"))

    assert "GAZP" in queries.get_user_tickers(db, 111)
    assert "SBER" in queries.get_user_tickers(db, 111)


@pytest.mark.asyncio
async def test_command_portfolio_show_empty(db):
    sent: list[str] = []

    async def capture(user_id, text):
        sent.append(text)
        return True

    with patch("app.bot.commands.send_dm", side_effect=capture):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/portfolio"))

    assert len(sent) == 1
    assert "нет активных" in sent[0].lower() or "подписок" in sent[0]


@pytest.mark.asyncio
async def test_command_portfolio_show_existing(db):
    queries.set_user_tickers(db, 111, ["LKOH"])

    sent: list[str] = []

    async def capture(user_id, text):
        sent.append(text)
        return True

    with patch("app.bot.commands.send_dm", side_effect=capture):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/portfolio"))

    assert "LKOH" in sent[0]


@pytest.mark.asyncio
async def test_command_unsubscribe(db):
    queries.set_user_tickers(db, 111, ["GAZP"])

    with patch("app.bot.commands.send_dm", new_callable=AsyncMock):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/unsubscribe"))

    assert queries.get_user_tickers(db, 111) == []


@pytest.mark.asyncio
async def test_command_portfolio_strips_dollar_prefix(db):
    with patch("app.bot.commands.send_dm", new_callable=AsyncMock):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/portfolio $GAZP $SBER"))

    tickers = queries.get_user_tickers(db, 111)
    assert "GAZP" in tickers
    assert "SBER" in tickers


@pytest.mark.asyncio
async def test_command_portfolio_unknown_ticker(db):
    sent: list[str] = []

    async def capture(user_id, text):
        sent.append(text)
        return True

    with patch("app.bot.commands.send_dm", side_effect=capture):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/portfolio XYZABC"))

    assert "не найден" in sent[0].lower() or "не распознан" in sent[0].lower()
    assert queries.get_user_tickers(db, 111) == []

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import aiohttp


@pytest.mark.asyncio
async def test_set_my_commands_sends_correct_commands():
    """_set_my_commands() POST-ит правильный список команд в Telegram API."""
    posted_payloads: list[dict] = []

    async def mock_post(url, json=None, **kwargs):
        posted_payloads.append(json or {})
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        return mock_resp

    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_session.post = mock_post

    with patch("app.main.aiohttp.ClientSession", return_value=mock_session):
        from app.main import _set_my_commands
        await _set_my_commands()

    assert len(posted_payloads) == 1
    commands = {c["command"] for c in posted_payloads[0]["commands"]}
    assert commands == {"portfolio", "calendar", "settings", "help"}


def test_build_main_menu_keyboard_regular_user():
    from app.bot.commands import _build_main_menu_keyboard
    kb = _build_main_menu_keyboard(is_admin=False)
    all_data = {btn["callback_data"] for row in kb["inline_keyboard"] for btn in row}
    assert "menu:portfolio" in all_data
    assert "menu:calendar"  in all_data
    assert "menu:settings"  in all_data
    assert "menu:help"      in all_data
    assert "menu:status" not in all_data


def test_build_main_menu_keyboard_admin():
    from app.bot.commands import _build_main_menu_keyboard
    kb = _build_main_menu_keyboard(is_admin=True)
    all_data = {btn["callback_data"] for row in kb["inline_keyboard"] for btn in row}
    assert "menu:status" in all_data
    # Standard buttons must still be present for admin
    assert "menu:portfolio" in all_data
    assert "menu:calendar" in all_data
    assert "menu:settings" in all_data
    assert "menu:help" in all_data


def test_help_text_contains_sections():
    from app.bot.commands import _help_text
    text = _help_text()
    assert "portfolio" in text.lower() or "портфель" in text.lower()
    assert "calendar"  in text.lower() or "календарь" in text.lower()
    assert "settings"  in text.lower() or "настройки" in text.lower()


def test_reply_keyboard_constant_structure():
    from app.bot.commands import _REPLY_KEYBOARD
    assert "keyboard" in _REPLY_KEYBOARD
    assert _REPLY_KEYBOARD["keyboard"] == [[{"text": "☰ Меню"}]]
    assert _REPLY_KEYBOARD.get("resize_keyboard") is True
    assert _REPLY_KEYBOARD.get("is_persistent") is True


@pytest.mark.asyncio
async def test_send_menu_calls_send_dm_with_inline_keyboard(db):
    """_send_menu() calls send_dm with the inline menu keyboard."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"user_id": user_id, "text": text, "reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import _send_menu
        await _send_menu(555, is_admin=False)

    assert len(sent) == 1
    assert sent[0]["user_id"] == 555
    kb = sent[0]["reply_markup"]
    assert kb is not None
    all_data = {btn["callback_data"] for row in kb["inline_keyboard"] for btn in row}
    assert "menu:portfolio" in all_data
    assert "menu:status" not in all_data  # not admin


def _make_update(user_id: int, text: str, first_name: str = "Test", update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "from": {"id": user_id, "first_name": first_name},
            "text": text,
        },
    }


@pytest.mark.asyncio
async def test_start_new_user_shows_wizard_step1(db):
    """New user (no tickers) gets wizard step 1 with 'Начать настройку' button."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(111, "/start"))

    assert len(sent) == 1
    kb = sent[0]["reply_markup"]
    assert kb is not None
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "onb:start" in all_data


@pytest.mark.asyncio
async def test_start_returning_user_shows_menu(db):
    """Returning user (has tickers) gets welcome + reply keyboard + inline menu."""
    from app.db import queries
    queries.set_user_tickers(db, 222, ["SBER"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(222, "/start"))

    # First message should have the reply keyboard
    assert any(
        msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
        for msg in sent
    )
    # Second message should have inline menu
    all_callbacks = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "menu:portfolio" in all_callbacks


@pytest.mark.asyncio
async def test_menu_button_sends_inline_menu(db):
    """'☰ Меню' text triggers inline menu."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(333, "☰ Меню"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "menu:portfolio" in all_data
    assert "menu:calendar" in all_data


@pytest.mark.asyncio
async def test_help_command_sends_help_text(db):
    """/help sends the help text."""
    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 1

    with patch("app.bot.commands.send_dm", side_effect=capture_dm):
        from app.bot.commands import handle_update
        await handle_update(db, _make_update(444, "/help"))

    assert len(sent_texts) == 1
    text = sent_texts[0]
    # Must contain all command references
    assert "/portfolio" in text
    assert "/calendar" in text
    assert "/settings" in text


def _make_callback(user_id: int, data: str, message_id: int = 1, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": "cbq_test",
            "from": {"id": user_id, "first_name": "Test"},
            "data": data,
            "message": {
                "message_id": message_id,
                "chat": {"id": user_id},
            },
        },
    }


def test_build_keyboard_custom_done_callback():
    """_build_keyboard() with done_callback='onb:2' uses that as the done button callback."""
    from app.bot.commands import _build_keyboard
    kb = _build_keyboard(set(), done_callback="onb:2")
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "onb:2" in all_data
    assert "done" not in all_data


def test_build_keyboard_default_done_callback():
    """_build_keyboard() default done_callback is 'done'."""
    from app.bot.commands import _build_keyboard
    kb = _build_keyboard(set())
    all_data = [btn["callback_data"] for row in kb["inline_keyboard"] for btn in row]
    assert "done" in all_data


@pytest.mark.asyncio
async def test_onb_start_sends_portfolio_keyboard(db):
    """`onb:start` sends a new message with portfolio keyboard using onb:2 done button."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:start"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:2" in all_data


@pytest.mark.asyncio
async def test_onb_2_sends_score_keyboard(db):
    """`onb:2` sends step 3 — score selection keyboard."""
    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"text": text, "reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:2"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:score:30" in all_data
    assert "onb:skip:score" in all_data


@pytest.mark.asyncio
async def test_onb_score_saves_and_sends_quiet_keyboard(db):
    """`onb:score:50` saves the score and shows step 4 (quiet hours)."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:score:50"))

    all_data = [
        btn["callback_data"]
        for msg in sent
        if msg.get("reply_markup") and "inline_keyboard" in msg["reply_markup"]
        for row in msg["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert "onb:quiet:off" in all_data
    assert "onb:skip:quiet" in all_data

    # Score saved in DB
    user_row = queries.get_user(db, 111)
    s = queries.get_user_settings(db, user_row["id"])
    assert s["min_score"] == 50


@pytest.mark.asyncio
async def test_onb_quiet_saves_and_shows_done(db):
    """`onb:quiet:22:8` saves quiet hours and shows step 5 with reply keyboard."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")
    queries.set_user_tickers(db, 111, ["SBER"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:quiet:22:8"))

    # Reply keyboard should appear
    reply_keyboards = [
        msg["reply_markup"]
        for msg in sent
        if msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
    ]
    assert len(reply_keyboards) == 1

    # Quiet hours saved
    user_row = queries.get_user(db, 111)
    s = queries.get_user_settings(db, user_row["id"])
    assert s["quiet_from"] == 22
    assert s["quiet_to"] == 8


@pytest.mark.asyncio
async def test_onb_skip_quiet_shows_done_without_saving(db):
    """`onb:skip:quiet` shows step 5 without changing quiet hours."""
    from app.db import queries
    queries.upsert_user(db, 111, None, "Test")
    queries.set_user_tickers(db, 111, ["GAZP"])

    sent: list[dict] = []

    async def capture_dm(user_id, text, reply_markup=None, **kwargs):
        sent.append({"reply_markup": reply_markup})
        return 1

    with (
        patch("app.bot.commands.answer_callback_query", AsyncMock()),
        patch("app.bot.commands.edit_dm", AsyncMock(return_value=True)),
        patch("app.bot.commands.send_dm", side_effect=capture_dm),
    ):
        from app.bot.commands import handle_update
        await handle_update(db, _make_callback(111, "onb:skip:quiet"))

    reply_keyboards = [
        msg for msg in sent
        if msg.get("reply_markup", {}).get("keyboard") == [[{"text": "☰ Меню"}]]
    ]
    assert len(reply_keyboards) == 1

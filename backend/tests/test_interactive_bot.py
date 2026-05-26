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

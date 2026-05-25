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

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from app.main import _set_my_commands
        await _set_my_commands()

    assert len(posted_payloads) == 1
    commands = {c["command"] for c in posted_payloads[0]["commands"]}
    assert commands == {"portfolio", "calendar", "settings", "help"}

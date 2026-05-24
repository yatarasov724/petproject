"""Tests for the /status bot command."""
import pytest
from unittest.mock import AsyncMock, patch

from app.bot.commands import handle_update, ADMIN_USER_IDS
from app.db import queries


ADMIN_USER_ID = 402652773


def _make_update(user_id: int, text: str) -> dict:
    return {
        "message": {
            "from": {"id": user_id, "first_name": "Test", "username": "testuser"},
            "text": text,
        }
    }


@pytest.mark.asyncio
async def test_status_command_sends_metrics(db):
    """Admin user receives a status message containing pipeline metrics."""
    with patch("app.bot.commands.send_dm", new_callable=AsyncMock) as mock_send:
        await handle_update(db, _make_update(ADMIN_USER_ID, "/status"))

    mock_send.assert_called_once()
    args = mock_send.call_args[0]
    assert args[0] == ADMIN_USER_ID
    sent_text = args[1]
    assert "Status" in sent_text or "status" in sent_text.lower()


@pytest.mark.asyncio
async def test_status_command_blocked_for_non_admin(db):
    """Non-admin user gets access denied."""
    non_admin = 999999
    with patch("app.bot.commands.send_dm", new_callable=AsyncMock) as mock_send:
        await handle_update(db, _make_update(non_admin, "/status"))

    mock_send.assert_called_once()
    args = mock_send.call_args[0]
    sent_text = args[1]
    assert "⛔" in sent_text or "доступ" in sent_text.lower()


def test_admin_user_ids_contains_known_admin():
    assert 402652773 in ADMIN_USER_IDS

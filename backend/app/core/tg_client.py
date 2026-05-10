"""
Telethon client singleton — shared across poll cycles.

connect() is called once at app startup; get() is used by tg_fetcher
during each poll cycle. Returns None everywhere if credentials are missing.
"""

import logging
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession

from app.core.config import settings

logger = logging.getLogger(__name__)

_client: Optional[TelegramClient] = None


def is_configured() -> bool:
    return bool(
        settings.telegram_api_id
        and settings.telegram_api_hash
        and settings.telegram_session_string
    )


async def connect() -> Optional[TelegramClient]:
    global _client
    if not is_configured():
        logger.info("Telethon: credentials not set — Telegram channel polling disabled")
        return None

    client = TelegramClient(
        StringSession(settings.telegram_session_string),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.connect()  # type: ignore[misc]

    if not await client.is_user_authorized():  # type: ignore[misc]
        logger.error(
            "Telethon: session string is invalid — "
            "regenerate with: python generate_session.py"
        )
        await client.disconnect()  # type: ignore[misc]
        return None

    _client = client
    logger.info("Telethon: connected and authorized")
    return _client


async def disconnect() -> None:
    global _client
    if _client is not None:
        await _client.disconnect()  # type: ignore[misc]
        _client = None
        logger.info("Telethon: disconnected")


def get() -> Optional[TelegramClient]:
    return _client

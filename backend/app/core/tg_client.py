"""
Telethon client singleton — shared across poll cycles.

Session strategy:
  1. Primary: SQLite file session at SESSION_FILE (.session extension added by Telethon).
     The file persists across restarts — Telegram keeps it valid as long as the
     service connects regularly.
  2. Bootstrap: if the file doesn't exist but TELEGRAM_SESSION_STRING is set in .env,
     the string session is used once to seed the file session, then discarded.

To regenerate a dead session:
  1. Delete telegram_session.session
  2. Run: python generate_session.py
  The script now writes the file directly and also prints the string as backup.

connect() returns None everywhere credentials are missing or the session is invalid.
All callers must handle None — polling is simply skipped that cycle.
"""

import logging
from pathlib import Path
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession

from app.core.config import settings

logger = logging.getLogger(__name__)

_client: Optional[TelegramClient] = None

# Session file lives next to the DB, in the working directory.
SESSION_FILE = Path("telegram_session")


def is_configured() -> bool:
    file_exists = SESSION_FILE.with_suffix(".session").exists()
    has_string  = bool(settings.telegram_session_string)
    has_creds   = bool(settings.telegram_api_id and settings.telegram_api_hash)
    return has_creds and (file_exists or has_string)


async def connect() -> Optional[TelegramClient]:
    global _client

    if not (settings.telegram_api_id and settings.telegram_api_hash):
        logger.info("Telethon: API_ID/HASH not set — channel polling disabled")
        return None

    api_id   = settings.telegram_api_id
    api_hash = settings.telegram_api_hash

    session_path = SESSION_FILE.with_suffix(".session")

    # ── bootstrap from string session if file doesn't exist ──────────────────
    if not session_path.exists():
        if not settings.telegram_session_string:
            logger.info("Telethon: no session file and no TELEGRAM_SESSION_STRING — polling disabled")
            return None

        logger.info("Telethon: bootstrapping file session from TELEGRAM_SESSION_STRING")
        try:
            src = TelegramClient(
                StringSession(settings.telegram_session_string),
                api_id, api_hash,
            )
            await src.connect()  # type: ignore[misc]

            dst = TelegramClient(str(SESSION_FILE), api_id, api_hash)
            dst.session.set_dc(  # type: ignore[union-attr]
                src.session.dc_id,           # type: ignore[union-attr]
                src.session.server_address,  # type: ignore[union-attr]
                src.session.port,            # type: ignore[union-attr]
            )
            dst.session.auth_key = src.session.auth_key  # type: ignore[union-attr]
            dst.session.save()  # type: ignore[union-attr]

            await src.disconnect()  # type: ignore[misc]
            logger.info("Telethon: session file created at %s", session_path)
        except Exception as exc:
            logger.error("Telethon: failed to bootstrap session file: %s", exc)
            return None

    # ── connect using the file session ────────────────────────────────────────
    client = TelegramClient(str(SESSION_FILE), api_id, api_hash)
    await client.connect()  # type: ignore[misc]

    if not await client.is_user_authorized():  # type: ignore[misc]
        logger.error(
            "Telethon: session is invalid — delete %s and run: python generate_session.py",
            session_path,
        )
        await client.disconnect()  # type: ignore[misc]
        return None

    _client = client
    logger.info("Telethon: connected and authorized (file session: %s)", session_path)
    return _client


async def disconnect() -> None:
    global _client
    if _client is not None:
        await _client.disconnect()  # type: ignore[misc]
        _client = None
        logger.info("Telethon: disconnected")


def get() -> Optional[TelegramClient]:
    return _client

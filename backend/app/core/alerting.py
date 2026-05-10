"""
Ops alerting via Telegram.
Sends to TELEGRAM_OPS_CHAT_ID when configured; silently no-ops otherwise.
All errors are swallowed — alerting must never crash the main pipeline.
"""

import logging

import aiohttp

from app.core.config import settings

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT  = aiohttp.ClientTimeout(total=5)


async def send_ops(text: str) -> None:
    """Send plain text to the ops chat. Never raises."""
    if not settings.telegram_ops_chat_id:
        return
    if settings.dry_run:
        logger.info("[DRY RUN] ops: %s", text)
        return
    url     = _BASE_URL.format(token=settings.telegram_bot_token)
    payload = {
        "chat_id":                  settings.telegram_ops_chat_id,
        "text":                     text,
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        "ops alert failed: HTTP %d — %s", resp.status, body[:120]
                    )
    except Exception as exc:
        logger.warning("ops alert dropped: %s", exc, exc_info=True)

"""
Ops alerting via Telegram.
Sends to TELEGRAM_OPS_CHAT_ID when configured; silently no-ops otherwise.
All errors are swallowed — alerting must never crash the main pipeline.
"""

import logging

import aiohttp

from app.core.config import settings

logger = logging.getLogger(__name__)

_BASE_URL  = "https://api.telegram.org/bot{token}/sendMessage"
_PIN_URL   = "https://api.telegram.org/bot{token}/pinChatMessage"
_TIMEOUT   = aiohttp.ClientTimeout(total=15)   # was 5s — intermittent CancelledError under load
_MAX_RETRY = 2


async def send_ops(text: str) -> None:
    """Send plain text to the ops chat. Never raises. Retries once on transient failure."""
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
    for attempt in range(1, _MAX_RETRY + 1):
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return
                    body = await resp.text()
                    logger.warning(
                        "ops alert failed (attempt %d): HTTP %d — %s",
                        attempt, resp.status, body[:120],
                    )
        except BaseException as exc:
            # CancelledError is BaseException in Python 3.11+, not Exception.
            # Alerting must never raise — swallow everything including cancellations.
            logger.warning("ops alert dropped (attempt %d): %s", attempt, exc)


async def send_ops_pinned(text: str) -> None:
    """Send plain text to the ops chat and pin the message. Never raises."""
    if not settings.telegram_ops_chat_id:
        return
    if settings.dry_run:
        logger.info("[DRY RUN] ops (pinned): %s", text)
        return
    send_url = _BASE_URL.format(token=settings.telegram_bot_token)
    payload  = {
        "chat_id":                  settings.telegram_ops_chat_id,
        "text":                     text,
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(send_url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("ops pinned alert send failed: HTTP %d — %s", resp.status, body[:120])
                    return
                data = await resp.json(content_type=None)

        message_id = data.get("result", {}).get("message_id")
        if not message_id:
            logger.warning("ops pinned alert: no message_id in response")
            return

        pin_url = _PIN_URL.format(token=settings.telegram_bot_token)
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(pin_url, json={
                "chat_id":              settings.telegram_ops_chat_id,
                "message_id":           message_id,
                "disable_notification": True,
            }) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("ops pin failed: HTTP %d — %s", resp.status, body[:120])
    except BaseException as exc:
        logger.warning("ops pinned alert dropped: %s", exc)

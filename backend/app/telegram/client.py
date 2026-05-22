"""
Telegram Bot API client.

Responsibilities:
  - Format and send one message to the configured channel
  - Handle 429 Too Many Requests (respect Retry-After header)
  - Retry on transient errors (5xx, network timeout) with exponential backoff
  - Never retry on permanent errors (400 Bad Request, 403 Forbidden)
  - Support DRY_RUN mode: full pipeline, no HTTP call, cluster state updated

Retry matrix:
  success (200+ok)  → return message_id, done
  429               → sleep Retry-After (capped at 30s), retry
  5xx / timeout     → exponential backoff (2s, 4s, 8s), retry
  400 / 401 / 403   → permanent, stop immediately
"""

import asyncio
import logging
from typing import Any, Optional

from app.db.database import DBConnection

import aiohttp

from app.ai.analyzer import AIAnalysis
from app.core import metrics
from app.core.config import settings
from app.db import queries
from app.pipeline.scorer import ScoreResult
from app.pipeline.publish_decision import Decision, PublishDecision
from app.telegram.formatter import format_message

logger = logging.getLogger(__name__)

_BASE_URL          = "https://api.telegram.org/bot{token}/sendMessage"
_EDIT_URL          = "https://api.telegram.org/bot{token}/editMessageText"
_UPDATES_URL       = "https://api.telegram.org/bot{token}/getUpdates"
_ANSWER_CBQ_URL    = "https://api.telegram.org/bot{token}/answerCallbackQuery"
_TIMEOUT           = aiohttp.ClientTimeout(total=10)
_MAX_RETRIES       = 3
_RETRY_BASE_S      = 2    # backoff: 2s → 4s → 8s
_MAX_RETRY_AFTER_S = 30   # cap Retry-After sleep

# Persists across calls within a process lifetime; resets on restart.
# On restart, Telegram re-delivers unconfirmed updates — /portfolio is idempotent.
_update_offset: int = 0


# ── public API ────────────────────────────────────────────────────────────────

async def get_updates() -> list[dict]:
    """
    Fetch pending bot updates via getUpdates (timeout=0, non-blocking).
    Advances the offset so each update is delivered exactly once per process.
    Returns an empty list on any error or in DRY_RUN mode.
    """
    global _update_offset
    if settings.dry_run:
        return []

    url = _UPDATES_URL.format(token=settings.telegram_bot_token)
    params = {
        "offset":          _update_offset,
        "timeout":         0,
        "allowed_updates": '["message","callback_query"]',
    }
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                body = await resp.json(content_type=None)
                if not body.get("ok"):
                    return []
                updates = body.get("result", [])
                if updates:
                    _update_offset = updates[-1]["update_id"] + 1
                return updates
    except Exception as exc:
        logger.warning("get_updates error: %s", exc)
        return []


async def send_dm(
    user_id: int,
    text: str,
    reply_markup: Optional[dict] = None,
) -> Optional[int]:
    """
    Send a MarkdownV2 message to a user (private DM), optionally with an inline keyboard.
    Only works if the user has previously started a conversation with the bot.
    Returns message_id on success, None on failure.
    """
    if settings.dry_run:
        logger.info("[DRY RUN] send_dm user_id=%d:\n%s", user_id, text)
        return 0

    url = _BASE_URL.format(token=settings.telegram_bot_token)
    payload: dict = {
        "chat_id":                  user_id,
        "text":                     text,
        "parse_mode":               "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.json(content_type=None)
                if resp.status == 200 and body.get("ok"):
                    return body["result"]["message_id"]
                logger.warning(
                    "send_dm failed user_id=%d: %s",
                    user_id,
                    body.get("description", ""),
                )
                return None
    except Exception as exc:
        logger.warning("send_dm error user_id=%d: %s", user_id, exc)
        return None


async def edit_dm(
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Optional[dict] = None,
) -> bool:
    """Edit an existing DM message text and/or inline keyboard."""
    if settings.dry_run:
        return True

    url = _EDIT_URL.format(token=settings.telegram_bot_token)
    payload: dict = {
        "chat_id":                  chat_id,
        "message_id":               message_id,
        "text":                     text,
        "parse_mode":               "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.json(content_type=None)
                ok = resp.status == 200 and body.get("ok")
                if not ok and "message is not modified" not in body.get("description", ""):
                    logger.warning("edit_dm failed chat_id=%d: %s", chat_id, body.get("description", ""))
                return bool(ok)
    except Exception as exc:
        logger.warning("edit_dm error chat_id=%d: %s", chat_id, exc)
        return False


async def answer_callback_query(
    callback_query_id: str,
    text: str = "",
    show_alert: bool = False,
) -> None:
    """Acknowledge a button press. Set show_alert=True to display a popup instead of a toast."""
    if settings.dry_run:
        return
    url = _ANSWER_CBQ_URL.format(token=settings.telegram_bot_token)
    payload: dict = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    if show_alert:
        payload["show_alert"] = True
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            await session.post(url, json=payload)
    except Exception as exc:
        logger.warning("answer_callback_query error: %s", exc)


async def edit_message(message_id: int, text: str) -> bool:
    """
    Edit an already-sent Telegram message in place.
    Used for AI enrichment: send plain message first, then edit when AI responds.
    Returns True on success or if already up-to-date.
    """
    if settings.dry_run or message_id <= 0:
        return True

    url = _EDIT_URL.format(token=settings.telegram_bot_token)
    payload = {
        "chat_id":                  settings.telegram_channel_id,
        "message_id":               message_id,
        "text":                     _apply_env_prefix(text),
        "parse_mode":               "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.json(content_type=None)
                ok = resp.status == 200 and body.get("ok")
                if not ok:
                    desc = body.get("description", "")
                    if "message is not modified" in desc:
                        return True
                    logger.warning(
                        "editMessageText failed message_id=%d: %s", message_id, desc,
                    )
                return bool(ok)
    except Exception as exc:
        logger.warning("editMessageText error message_id=%d: %s", message_id, exc)
        return False


async def send_text(text: str) -> Optional[int]:
    """
    Send a plain MarkdownV2 message to the main channel without cluster context.
    Used for digest posts and other standalone messages.

    Returns:
      positive int — Telegram message_id on success
      0            — DRY_RUN
      None         — send failed
    """
    if settings.dry_run:
        logger.info("[DRY RUN] send_text:\n%s", text)
        return 0

    msg_id, error_text = await _send_with_retry(text)
    if msg_id is None:
        metrics.inc(metrics.TG_SENT_FAIL)
        logger.error("send_text failed: %s", error_text)
    else:
        metrics.inc(metrics.TG_SENT_OK)
    return msg_id


async def send(
    db: DBConnection,
    cluster: Any,
    score_result: ScoreResult,
    pub_decision: PublishDecision,
    ai_analysis: Optional[AIAnalysis] = None,
    correlations: Optional[list] = None,
) -> Optional[int]:
    """
    Format and send a message to the Telegram channel.
    Always writes to telegram_sends (including in DRY_RUN).

    Returns:
      positive int  — Telegram message_id on success
      0             — DRY_RUN (logical success, no real message_id)
      None          — send failed
    """
    text = format_message(
        cluster=cluster,
        score_result=score_result,
        decision=pub_decision.decision,
        ai_analysis=ai_analysis,
        correlations=correlations or [],
    )

    if settings.dry_run:
        logger.info(
            "[DRY RUN] would send %s cluster=#%d score=%d\n%s",
            pub_decision.decision.value,
            pub_decision.cluster_id,
            pub_decision.score,
            text,
            extra={
                "event":      "tg_dry_run",
                "decision":   pub_decision.decision.value,
                "cluster_id": pub_decision.cluster_id,
                "score":      pub_decision.score,
            },
        )
        # Mark cluster sent so pipeline state stays consistent across cycles.
        # Without this, every poll would re-trigger the same decision.
        queries.mark_cluster_sent(
            db,
            cluster_id=pub_decision.cluster_id,
            decision=pub_decision.decision.value,
            score=pub_decision.score,
        )
        queries.log_send(
            db,
            cluster_id=pub_decision.cluster_id,
            decision=pub_decision.decision.value,
            score=pub_decision.score,
            source_count=cluster["source_count"],
            headline=cluster["canonical_title"],
            tg_message_id=None,
            ok=True,
            error_text="dry_run",
        )
        metrics.inc(metrics.TG_SENT_OK)
        return 0  # DRY_RUN: no real message_id

    logger.info(
        "sending %s cluster=#%d score=%d sources=%d type=%s",
        pub_decision.decision.value,
        pub_decision.cluster_id,
        pub_decision.score,
        cluster["source_count"],
        score_result.event_type.value,
        extra={
            "event":      "tg_send_attempt",
            "decision":   pub_decision.decision.value,
            "cluster_id": pub_decision.cluster_id,
            "score":      pub_decision.score,
        },
    )

    tg_message_id, error_text = await _send_with_retry(text)
    ok = tg_message_id is not None

    if ok:
        metrics.inc(metrics.TG_SENT_OK)
        queries.mark_cluster_sent(
            db,
            cluster_id=pub_decision.cluster_id,
            decision=pub_decision.decision.value,
            score=pub_decision.score,
        )
    else:
        metrics.inc(metrics.TG_SENT_FAIL)
        logger.error(
            "telegram send failed: cluster=#%d error=%s",
            pub_decision.cluster_id,
            error_text,
            extra={"event": "tg_send_failed", "cluster_id": pub_decision.cluster_id},
        )

    queries.log_send(
        db,
        cluster_id=pub_decision.cluster_id,
        decision=pub_decision.decision.value,
        score=pub_decision.score,
        source_count=cluster["source_count"],
        headline=cluster["canonical_title"],
        tg_message_id=tg_message_id,
        ok=ok,
        error_text=error_text,
    )

    return tg_message_id  # int on success, None on failure


# ── internals ─────────────────────────────────────────────────────────────────

def _apply_env_prefix(text: str) -> str:
    if settings.environment == "development":
        return "\\[DEV\\]\n" + text
    return text


async def _send_with_retry(text: str) -> tuple[Optional[int], Optional[str]]:
    """
    Attempt to send up to _MAX_RETRIES times.
    Returns (tg_message_id, None) on success or (None, error_text) on failure.
    """
    url     = _BASE_URL.format(token=settings.telegram_bot_token)
    payload = {
        "chat_id":                  settings.telegram_channel_id,
        "text":                     _apply_env_prefix(text),
        "parse_mode":               "MarkdownV2",
        "disable_web_page_preview": True,
    }
    last_error = "unknown"

    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        for attempt in range(1, _MAX_RETRIES + 1):
            msg_id, retry_after, permanent = await _try_send(session, url, payload)

            if msg_id is not None:
                return msg_id, None

            if permanent:
                last_error = f"permanent error (attempt {attempt})"
                break

            if retry_after is not None:
                wait = min(retry_after, _MAX_RETRY_AFTER_S)
                logger.warning(
                    "Telegram 429 — sleeping %ds (Retry-After=%ds) attempt %d/%d",
                    wait, retry_after, attempt, _MAX_RETRIES,
                )
                metrics.inc(metrics.TG_RATE_LIMITED)
                last_error = f"429 rate limited (retry_after={retry_after}s)"
                await asyncio.sleep(wait)
                continue

            # Transient error (5xx, timeout, network): exponential backoff
            last_error = f"transient error (attempt {attempt})"
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BASE_S ** attempt
                logger.warning(
                    "Telegram transient error — sleeping %ds attempt %d/%d",
                    wait, attempt, _MAX_RETRIES,
                )
                await asyncio.sleep(wait)

    return None, last_error


async def _try_send(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
) -> tuple[Optional[int], Optional[int], bool]:
    """
    Single HTTP attempt.
    Returns: (message_id, retry_after, permanent)
      (N,    None, False) — success
      (None, N,    False) — 429, sleep N seconds and retry
      (None, None, True)  — permanent error, do not retry
      (None, None, False) — transient error, may retry
    """
    try:
        async with session.post(url, json=payload) as resp:
            status = resp.status
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = {}

            if status == 200 and body.get("ok"):
                msg_id = body.get("result", {}).get("message_id")
                return msg_id, None, False

            if status == 429:
                retry_after = int(body.get("parameters", {}).get("retry_after", 5))
                logger.warning(
                    "Telegram 429: retry_after=%ds description=%s",
                    retry_after,
                    body.get("description", ""),
                )
                return None, retry_after, False

            if status in (400, 401, 403):
                logger.error(
                    "Telegram permanent error %d: %s",
                    status,
                    body.get("description", ""),
                )
                return None, None, True

            # 5xx or unexpected
            logger.warning("Telegram %d: %s", status, body.get("description", ""))
            return None, None, False

    except asyncio.TimeoutError:
        logger.warning("Telegram request timed out")
        return None, None, False

    except aiohttp.ClientError as exc:
        logger.warning("Telegram network error: %s", exc)
        return None, None, False

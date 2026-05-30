"""
Telegram channel polling via Telethon.

Each poll cycle fetches the last _MAX_MESSAGES messages from every active
Telegram channel (source_type='telegram'), starting from the last seen
message ID. Messages are converted to RawArticle and returned for the
main pipeline (dedup → cluster → score → publish).

Connection is managed externally (app/core/tg_client.py) so the session
stays alive across poll cycles.
"""

import asyncio
import logging
import re
from app.db.database import DBConnection
from datetime import timezone
from typing import Any

from telethon import TelegramClient

from app.db import queries
from app.pipeline.normalizer import RawArticle, tokenize, make_hash

logger = logging.getLogger(__name__)

_MAX_MESSAGES = 20  # per channel per cycle

# Channel entity cache — avoids redundant API resolution each 60 s.
# Keyed by username; reset on service restart (acceptable stale window: never).
_entity_cache: dict[str, Any] = {}


async def fetch_all_tg(
    db: DBConnection,
    client: TelegramClient,
) -> list[RawArticle]:
    channels = queries.get_active_tg_channels(db)
    if not channels:
        return []

    tasks = [_fetch_channel(client, db, ch) for ch in channels]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    articles: list[RawArticle] = []
    for channel, result in zip(channels, results):
        if isinstance(result, Exception):
            # Transient errors (connection reset, timeout) are logged but do NOT
            # increment error_count — channels stay ok and are retried next cycle.
            logger.warning(
                "TG fetch skipped for %s: %s",
                channel["name"],
                result,
                extra={"event": "tg_fetch_error", "source": channel["name"]},
            )
        else:
            articles.extend(result)  # type: ignore[arg-type]

    logger.info(
        "TG poll: %d channels, %d articles",
        len(channels),
        len(articles),
        extra={"event": "tg_poll_complete", "channels": len(channels), "articles": len(articles)},
    )
    return articles


async def _fetch_channel(
    client: TelegramClient,
    db: DBConnection,
    source: Any,
) -> list[RawArticle]:
    username   = source["url"].removeprefix("tg://")
    source_id  = source["id"]
    source_name = source["name"]
    last_msg_id = source["tg_last_msg_id"] or 0

    try:
        if username not in _entity_cache:
            _entity_cache[username] = await client.get_entity(username)
        entity = _entity_cache[username]

        messages = await client.get_messages(
            entity,
            limit=_MAX_MESSAGES,
            min_id=last_msg_id,
        )
    except Exception as exc:
        logger.warning(
            "[%s] Telethon fetch error: %s",
            source_name,
            exc,
            extra={"event": "tg_channel_error", "source": source_name},
        )
        return []  # retry next cycle; don't touch backoff state

    if not messages:
        queries.update_tg_channel_ok(db, source_id, last_msg_id)
        return []

    articles: list[RawArticle] = []
    new_last_id = last_msg_id

    for msg in messages:  # type: ignore[union-attr]
        if not msg.text or len(msg.text) < 20:
            continue

        text = msg.text.strip()
        first_line = _extract_headline(text)

        tokens = tokenize(first_line)
        if not tokens:
            continue

        published_at = msg.date
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        articles.append(RawArticle(
            source_id=source_id,
            source_name=source_name,
            title=first_line,
            url=f"https://t.me/{username}/{msg.id}",
            published_at=published_at,
            raw_hash=make_hash(tokens, published_at),
            title_tokens=" ".join(tokens),
            content=text[:500],
        ))

        if msg.id > new_last_id:
            new_last_id = msg.id

    queries.update_tg_channel_ok(db, source_id, new_last_id)
    logger.info(
        "[%s] fetched %d messages → %d articles",
        source_name,
        len(messages),  # type: ignore[arg-type]
        len(articles),
        extra={
            "event":   "tg_channel_fetched",
            "source":  source_name,
            "msgs":    len(messages),  # type: ignore[arg-type]
            "articles": len(articles),
        },
    )
    return articles


_HASHTAG_RE = re.compile(r'#\S+')
_EMOJI_RE   = re.compile(
    "["
    "\U0001F000-\U0001FFFF"
    "\U00002600-\U000027BF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0000FE00-\U0000FE0F"
    "\U0001F900-\U0001F9FF"
    "\u2757\u26A0\u203C]+",
    flags=re.UNICODE,
)


def _extract_headline(text: str) -> str:
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if not lines:
        return text[:150]
    fallback = lines[0]
    for line in lines:
        cleaned = _EMOJI_RE.sub("", _HASHTAG_RE.sub("", line)).strip()
        if len(cleaned) >= 5:
            return line[:150]
    return fallback[:150]

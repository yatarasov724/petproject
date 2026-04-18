"""
RSS ingestion: fetches all active sources, returns RawArticle list.

Design decisions:
- One aiohttp.ClientSession per poll cycle (connection pooling across sources).
- ETag / If-Modified-Since sent on every request → 304 short-circuits parsing.
- Errors are isolated per source: one dead source never blocks others.
- Backoff state is written to DB immediately after each error.
- feedparser.parse() is synchronous but fast (<100ms per feed); acceptable for MVP.
"""

import asyncio
import logging
import sqlite3
from typing import Optional

import aiohttp
import feedparser

from app.core import metrics
from app.db import queries
from app.pipeline.normalizer import RawArticle, normalize

logger = logging.getLogger(__name__)

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)
_USER_AGENT    = "MOEXNewsBot/1.0 (+https://github.com/yatarasov724/petproject)"


async def fetch_all(db: sqlite3.Connection) -> list[RawArticle]:
    """
    Entry point for the poll job.
    Fetches all active sources concurrently, returns de-raw-duplicated articles.
    """
    sources = queries.get_active_sources(db)
    if not sources:
        logger.warning("No active RSS sources found")
        return []

    async with aiohttp.ClientSession(
        headers={"User-Agent": _USER_AGENT},
        timeout=_FETCH_TIMEOUT,
    ) as session:
        tasks = [_fetch_source(session, db, src) for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    articles: list[RawArticle] = []
    for source, result in zip(sources, results):
        if isinstance(result, Exception):
            # gather with return_exceptions=True — should not happen since
            # _fetch_source catches internally, but guard anyway.
            logger.error("Unhandled error for source %s: %s", source["name"], result)
            queries.update_source_error(db, source["id"])
        else:
            articles.extend(result)

    logger.info(
        "Poll complete: %d sources, %d raw articles",
        len(sources),
        len(articles),
    )
    return articles


async def _fetch_source(
    session: aiohttp.ClientSession,
    db: sqlite3.Connection,
    source: sqlite3.Row,
) -> list[RawArticle]:
    """
    Fetch one source. Returns list of RawArticle (may be empty on 304 or error).
    Writes fetch result back to rss_sources.
    """
    source_id   = source["id"]
    source_name = source["name"]
    url         = source["url"]

    headers = _conditional_headers(source)

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 304:
                logger.debug("[%s] 304 Not Modified — skipping", source_name)
                # Still update last_fetched_at and reset error_count
                queries.update_source_ok(db, source_id, source["etag"], source["last_modified"])
                return []

            if resp.status != 200:
                logger.warning(
                    "[%s] HTTP %d — recording error", source_name, resp.status
                )
                _record_error(db, source_id)
                return []

            body = await resp.text(errors="replace")
            new_etag          = resp.headers.get("ETag")
            new_last_modified = resp.headers.get("Last-Modified")

    except asyncio.TimeoutError:
        logger.warning("[%s] Timeout — recording error", source_name)
        _record_error(db, source_id)
        return []

    except aiohttp.ClientError as exc:
        logger.warning("[%s] Network error: %s — recording error", source_name, exc)
        _record_error(db, source_id)
        return []

    # Update source state after a successful HTTP response
    queries.update_source_ok(db, source_id, new_etag, new_last_modified)

    articles = _parse_feed(body, source_id, source_name)
    logger.info("[%s] fetched %d articles", source_name, len(articles))
    return articles


def _parse_feed(
    body: str,
    source_id: int,
    source_name: str,
) -> list[RawArticle]:
    """
    Parse RSS/Atom body with feedparser, normalize each entry.
    Skips malformed entries silently (normalize returns None).
    """
    try:
        feed = feedparser.parse(body)
    except Exception as exc:
        logger.error("[%s] feedparser crashed: %s", source_name, exc)
        return []

    if feed.bozo:
        exc = getattr(feed, "bozo_exception", None)
        logger.warning("[%s] Malformed feed (bozo): %s", source_name, exc)
        if not feed.entries:
            return []

    articles: list[RawArticle] = []
    for entry in feed.entries:
        article = normalize(entry, source_id, source_name)
        if article is not None:
            articles.append(article)

    return articles


def _conditional_headers(source: sqlite3.Row) -> dict[str, str]:
    headers: dict[str, str] = {}
    if source["etag"]:
        headers["If-None-Match"] = source["etag"]
    if source["last_modified"]:
        headers["If-Modified-Since"] = source["last_modified"]
    return headers


def _record_error(db: sqlite3.Connection, source_id: int) -> None:
    """Record a fetch error and update the corresponding backoff/dead metric."""
    new_status = queries.update_source_error(db, source_id)
    if new_status == "dead":
        metrics.inc(metrics.SOURCES_DEAD)
    else:
        metrics.inc(metrics.SOURCES_BACKOFF)

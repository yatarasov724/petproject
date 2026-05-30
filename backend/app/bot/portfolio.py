"""
Portfolio notification dispatch.

Called from orchestrator (fire-and-forget asyncio.Task) after a successful publish.
Opens its own DB connection so it can safely run after the poll cycle closes the main one.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.ai.analyzer import AIAnalysis

import logging

from app.core import metrics
from app.db import queries
from app.db.database import get_db
from app.telegram.client import send_dm
from app.telegram.formatter import _esc

logger = logging.getLogger(__name__)


async def notify(tickers_raw: str, canonical_title: str, cluster_id: int, score: int | None = None) -> None:
    """
    Send DMs to all users subscribed to any ticker in this cluster.

    tickers_raw — comma-separated MOEX ticker string, e.g. "GAZP,SBER".
    """
    tickers = [t.strip() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    db = get_db()
    try:
        user_ids = queries.get_subscribed_users(db, tickers)
    finally:
        db.close()

    if not user_ids:
        # Track events that had tickers but reached nobody — useful for monitoring.
        metrics.inc(metrics.PORTFOLIO_NO_SUBS)
        logger.debug(
            "portfolio notify: 0 subscribers for tickers=%s cluster_id=%d",
            ",".join(tickers), cluster_id,
            extra={"event": "portfolio_no_subs", "tickers": ",".join(tickers), "cluster_id": cluster_id},
        )
        return

    tickers_line = " · ".join(f"\\${t}" for t in tickers)
    title = _esc(canonical_title)
    text = f"*{title}*\n\n{tickers_line}"

    for user_id in user_ids:
        msg_id = await send_dm(user_id, text)
        ok = msg_id is not None
        if ok:
            metrics.inc(metrics.PORTFOLIO_DM_SENT)
        else:
            metrics.inc(metrics.PORTFOLIO_DM_FAILED)
        logger.info(
            "portfolio notify %s: user_id=%d cluster_id=%d tickers=%s",
            "ok" if ok else "failed",
            user_id,
            cluster_id,
            ",".join(tickers),
            extra={
                "event":      "portfolio_notify_ok" if ok else "portfolio_notify_failed",
                "user_id":    user_id,
                "cluster_id": cluster_id,
            },
        )


async def notify_with_ai(
    tickers_raw: str,
    ai_analysis: "AIAnalysis",
    cluster_id: int,
    canonical_title: str = "",
) -> None:
    """Send AI-enriched DM to all users subscribed to any ticker in this cluster."""
    tickers = [t.strip() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    db = get_db()
    try:
        user_ids = queries.get_subscribed_users(db, tickers)
    finally:
        db.close()

    if not user_ids:
        metrics.inc(metrics.PORTFOLIO_NO_SUBS)
        return

    if ai_analysis.tickers and canonical_title:
        from app.pipeline.ticker_validator import validate_tickers
        safe = validate_tickers(",".join(ai_analysis.tickers), canonical_title)
        dm_tickers = [t for t in safe.split(",") if t] if safe else tickers
    else:
        dm_tickers = ai_analysis.tickers if ai_analysis.tickers else tickers
    tickers_line = " · ".join(f"\\${t}" for t in dm_tickers)

    parts = [
        f"{ai_analysis.emoji} *{_esc(ai_analysis.title)}*",
        "",
        _esc(ai_analysis.summary),
    ]
    if ai_analysis.context:
        parts += ["", f"📌 {_esc(ai_analysis.context)}"]
    parts += [
        "",
        f"⚡ {_esc(ai_analysis.market_effect)}",
    ]
    if tickers_line:
        parts += ["", tickers_line]

    text = "\n".join(parts)

    for user_id in user_ids:
        msg_id = await send_dm(user_id, text)
        ok = msg_id is not None
        metrics.inc(metrics.PORTFOLIO_DM_SENT if ok else metrics.PORTFOLIO_DM_FAILED)
        logger.info(
            "portfolio notify_with_ai %s: user_id=%d cluster_id=%d",
            "ok" if ok else "failed",
            user_id,
            cluster_id,
            extra={
                "event":      "portfolio_notify_ai_ok" if ok else "portfolio_notify_ai_failed",
                "user_id":    user_id,
                "cluster_id": cluster_id,
            },
        )

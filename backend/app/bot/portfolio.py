"""
Portfolio notification dispatch.

Called from orchestrator (fire-and-forget asyncio.Task) after a successful publish.
Opens its own DB connection so it can safely run after the poll cycle closes the main one.
"""

import logging

from app.db import queries
from app.db.database import get_db
from app.telegram.client import send_dm
from app.telegram.formatter import _esc

logger = logging.getLogger(__name__)


async def notify(tickers_raw: str, canonical_title: str, cluster_id: int) -> None:
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
        return

    tickers_line = " · ".join(f"\\${t}" for t in tickers)
    title = _esc(canonical_title)
    text = f"🔔 *Событие по вашему портфелю*\n\n{tickers_line}\n\n{title}"

    for user_id in user_ids:
        ok = await send_dm(user_id, text)
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

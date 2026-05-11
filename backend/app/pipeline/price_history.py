"""
Historical price correlation tracker.

When an event is published with tickers:
  1. capture_price_snapshot() fires as a background task — records current MOEX price.
  2. Hourly job calls update_price_snapshots() — fills in price_1h / price_24h.
  3. Before publishing, get_correlations() returns avg % change for this event type.

Data source: MOEX ISS API (free, no auth required).
Minimum samples before showing correlations: _MIN_SAMPLES (default 3).
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from app.db import queries
from app.db.database import DBConnection, get_db

logger = logging.getLogger(__name__)

_ISS_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR"
    "/securities/{ticker}.json"
    "?iss.meta=off&iss.only=marketdata&marketdata.columns=LAST"
)
_TIMEOUT = aiohttp.ClientTimeout(total=5)
_MIN_SAMPLES = 3


@dataclass(frozen=True)
class TickerCorrelation:
    ticker: str
    avg_24h_pct: float
    sample_count: int


async def fetch_price(ticker: str) -> Optional[float]:
    """Fetch last trade price from MOEX ISS. Returns None if unavailable."""
    url = _ISS_URL.format(ticker=ticker)
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                md = data.get("marketdata", {})
                columns = md.get("columns", [])
                rows = md.get("data", [])
                if not rows or "LAST" not in columns:
                    return None
                val = rows[0][columns.index("LAST")]
                return float(val) if val else None
    except Exception as exc:
        logger.debug("fetch_price %s: %s", ticker, exc)
        return None


async def capture_price_snapshot(
    cluster_id: int,
    tickers: str,
    event_type: str,
) -> None:
    """
    Background task: fetch current MOEX prices and record snapshots.
    Opens its own DB connection — safe to run after poll cycle closes its connection.
    """
    published_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]

    db = get_db()
    try:
        for ticker in ticker_list:
            price = await fetch_price(ticker)
            queries.insert_price_snapshot(
                db,
                cluster_id=cluster_id,
                ticker=ticker,
                event_type=event_type,
                published_at=published_at,
                price_at_publish=price,
            )
        logger.debug(
            "price snapshots captured cluster_id=%d tickers=%s",
            cluster_id, tickers,
        )
    except Exception:
        logger.warning("capture_price_snapshot failed cluster_id=%d", cluster_id, exc_info=True)
    finally:
        db.close()


async def update_price_snapshots() -> None:
    """
    Fill in price_1h and price_24h for pending snapshots.
    Called by the hourly background job.
    """
    db = get_db()
    try:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        pending_1h = queries.get_pending_1h_snapshots(db)
        for row in pending_1h:
            price = await fetch_price(row["ticker"])
            if price is not None:
                queries.update_price_snapshot_1h(db, row["id"], price, now_iso)

        pending_24h = queries.get_pending_24h_snapshots(db)
        for row in pending_24h:
            price = await fetch_price(row["ticker"])
            if price is not None:
                queries.update_price_snapshot_24h(db, row["id"], price, now_iso)

        if pending_1h or pending_24h:
            logger.info(
                "price snapshots updated: 1h=%d 24h=%d",
                len(pending_1h), len(pending_24h),
            )
    except Exception:
        logger.warning("update_price_snapshots failed", exc_info=True)
    finally:
        db.close()


def get_correlations(
    db: DBConnection,
    event_type: str,
    tickers: str,
) -> list[TickerCorrelation]:
    """
    Return historical avg 24h price changes for these tickers on this event_type.
    Only returns tickers with >= _MIN_SAMPLES completed snapshots.
    """
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        return []
    rows = queries.get_price_correlations(db, event_type, ticker_list, min_samples=_MIN_SAMPLES)
    return [
        TickerCorrelation(
            ticker=row["ticker"],
            avg_24h_pct=float(row["avg_24h_pct"]),
            sample_count=int(row["sample_count"]),
        )
        for row in rows
    ]

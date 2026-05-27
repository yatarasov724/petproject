# app/pipeline/ticker_validator.py
"""
Pre-publication ticker validation.

Before sending a cluster to Telegram, verify that each assigned ticker's
company keyword appears in the cluster's canonical title.

Commodity tickers (LKOH, ROSN, GAZP, NVTK, PLZL) are exempt — they are
assigned based on market-level keywords (oil price, OPEC, gold) even when
no specific company is mentioned.

Returns a comma-joined string of valid tickers (same format as DB column).
"""

import logging
from typing import Optional

from app.ai.filter import TICKER_KEYWORDS

logger = logging.getLogger(__name__)

# These tickers are valid even if their company keyword isn't in the title.
# They're assigned by commodity/macro rules, not company-name matching.
COMMODITY_TICKERS: frozenset[str] = frozenset({"LKOH", "ROSN", "GAZP", "NVTK", "PLZL"})


def validate_tickers(tickers_str: Optional[str], cluster_title: str) -> str:
    """
    Return comma-joined tickers whose company keywords appear in cluster_title.
    Commodity tickers pass without keyword check.
    Invalid/mismatched tickers are stripped and logged as WARNING.

    Args:
        tickers_str: comma-joined ticker string from event_clusters.tickers, or None.
        cluster_title: the cluster's canonical_title used for keyword lookup.

    Returns:
        Comma-joined string of validated tickers, possibly empty.
    """
    if not tickers_str:
        return ""

    tickers = [t.strip() for t in tickers_str.split(",") if t.strip()]
    title_lower = cluster_title.lower()
    valid: list[str] = []

    for ticker in tickers:
        if ticker in COMMODITY_TICKERS:
            valid.append(ticker)
            continue

        keywords = TICKER_KEYWORDS.get(ticker, [])
        if any(kw in title_lower for kw in keywords):
            valid.append(ticker)
        else:
            logger.warning(
                "ticker_mismatch ticker=%s not in title «%.80s» — stripped from publication",
                ticker,
                cluster_title,
                extra={
                    "event":  "ticker_mismatch",
                    "ticker": ticker,
                    "title":  cluster_title[:80],
                },
            )

    return ",".join(valid)

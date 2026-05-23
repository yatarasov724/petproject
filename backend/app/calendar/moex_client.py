"""
MOEX ISS API client for corporate events.

Endpoints:
  GET https://iss.moex.com/iss/securities/{secid}/dividends.json
      → dividend_cutoff and dividend_payment events

  GET https://iss.moex.com/iss/events.json?secid=…&date_from=…&date_to=…
      → earnings, buybacks, offers

Design:
  - Stateless: no DB dependency. Returns list[CorporateEvent].
  - Errors per call are absorbed — returns [] on any failure.
  - Only returns events with event_date >= today.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import aiohttp

logger = logging.getLogger(__name__)

_BASE       = "https://iss.moex.com/iss"
_TIMEOUT    = aiohttp.ClientTimeout(total=15, connect=5)
_USER_AGENT = "MOEXNewsBot/1.0"

# ISS event_type string → internal event_type
_EVENT_TYPE_MAP: dict[str, str] = {
    "earning":      "earnings",
    "buyback":      "buyback",
    "tender_offer": "offer",
    "placement":    "offer",
}

# Keywords in report title → report_type label
_REPORT_KEYWORDS: list[tuple[str, str]] = [
    ("МСФО", "МСФО"),
    ("РСБУ", "РСБУ"),
    ("GAAP", "GAAP"),
]


@dataclass(frozen=True)
class CorporateEvent:
    ticker:     str
    event_type: str   # 'dividend_cutoff'|'dividend_payment'|'earnings'|'buyback'|'offer'
    event_date: date
    details:    dict = field(default_factory=dict)


class MoexIssClient:
    """Fetches corporate events from MOEX ISS. Each method is independent."""

    async def fetch_dividends(self, ticker: str) -> list[CorporateEvent]:
        """Return dividend_cutoff and dividend_payment events for ticker."""
        url   = f"{_BASE}/securities/{ticker}/dividends.json"
        today = datetime.now(timezone.utc).date()
        events: list[CorporateEvent] = []

        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": _USER_AGENT}, timeout=_TIMEOUT
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.warning("[MOEX] dividends %s HTTP %d", ticker, resp.status)
                        return []
                    data = await resp.json(content_type=None)

            block   = data.get("dividends", {})
            columns = block.get("columns", [])
            rows    = block.get("data", [])
            col     = {name: i for i, name in enumerate(columns)}

            for row in rows:
                cutoff_str  = row[col.get("registryclosedate", 0)]
                payment_str = row[col["fixdate"]] if "fixdate" in col else None
                amount      = row[col.get("value", 0)]
                currency    = row[col["currencyid"]] if "currencyid" in col else "RUB"

                if not cutoff_str:
                    continue
                try:
                    cutoff_date = date.fromisoformat(str(cutoff_str))
                except ValueError:
                    continue
                if cutoff_date < today:
                    continue

                details = {"amount": amount, "currency": currency}
                events.append(CorporateEvent(ticker, "dividend_cutoff", cutoff_date, details))

                if payment_str:
                    try:
                        payment_date = date.fromisoformat(str(payment_str))
                        if payment_date >= today:
                            events.append(
                                CorporateEvent(ticker, "dividend_payment", payment_date, details)
                            )
                    except ValueError:
                        pass

        except Exception as exc:
            logger.warning("[MOEX] fetch_dividends %s: %s", ticker, exc)

        return events

    async def fetch_events(self, ticker: str, days_ahead: int = 90) -> list[CorporateEvent]:
        """Return earnings, buyback and offer events for ticker."""
        today     = datetime.now(timezone.utc).date()
        url       = f"{_BASE}/events.json"
        params    = {
            "secid":     ticker,
            "date_from": today.isoformat(),
            "date_to":   (today + timedelta(days=days_ahead)).isoformat(),
        }
        events: list[CorporateEvent] = []

        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": _USER_AGENT}, timeout=_TIMEOUT
            ) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning("[MOEX] events %s HTTP %d", ticker, resp.status)
                        return []
                    data = await resp.json(content_type=None)

            block   = data.get("events", {})
            columns = block.get("columns", [])
            rows    = block.get("data", [])
            col     = {name: i for i, name in enumerate(columns)}

            for row in rows:
                iss_type   = row[col.get("event_type", 0)] if col else None
                event_type = _EVENT_TYPE_MAP.get(str(iss_type or ""))
                if not event_type:
                    continue

                date_raw = row[col.get("date_start", 0)] if col else None
                if not date_raw:
                    continue
                try:
                    event_date = date.fromisoformat(str(date_raw)[:10])
                except ValueError:
                    continue
                if event_date < today:
                    continue

                details: dict = {}
                if event_type == "earnings" and "title" in col:
                    title = str(row[col["title"]] or "")
                    for keyword, label in _REPORT_KEYWORDS:
                        if keyword in title:
                            details["report_type"] = label
                            break

                events.append(CorporateEvent(ticker, event_type, event_date, details))

        except Exception as exc:
            logger.warning("[MOEX] fetch_events %s: %s", ticker, exc)

        return events

    async def fetch_all(self, ticker: str, days_ahead: int = 90) -> list[CorporateEvent]:
        """Fetch dividends + other events for a single ticker. Errors absorbed."""
        dividends = await self.fetch_dividends(ticker)
        others    = await self.fetch_events(ticker, days_ahead=days_ahead)
        return dividends + others

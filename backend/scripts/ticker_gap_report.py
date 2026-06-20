#!/usr/bin/env python3
"""
Compare TICKER_KEYWORDS in filter.py against live MOEX TQBR instruments registry.

Outputs three lists:
  - New on MOEX, not in bot  → candidates to add to TICKER_KEYWORDS
  - In bot, not on MOEX      → likely delisted, check if should remove
  - Coverage summary

Run: docker exec backend-backend-1 python scripts/ticker_gap_report.py
"""
import sys
sys.path.insert(0, "/app")

from app.db.database import get_db
from app.db import queries
from app.ai.filter import TICKER_KEYWORDS


def main() -> None:
    db = get_db()

    moex_tickers = queries.get_moex_tickers(db)
    if not moex_tickers:
        print("ERROR: moex_instruments table is empty.")
        print("Run the sync job first:")
        print("  docker exec backend-backend-1 python -c \"")
        print("  import asyncio; from app.scheduler.jobs import moex_instruments_sync_job")
        print("  asyncio.run(moex_instruments_sync_job())\"")
        sys.exit(1)

    # Fetch short names for display
    rows = db.execute("SELECT ticker, short_name FROM moex_instruments").fetchall()
    short_name: dict[str, str] = {r["ticker"]: r["short_name"] for r in rows}

    bot_tickers = set(TICKER_KEYWORDS.keys())

    new_on_moex   = sorted(moex_tickers - bot_tickers)
    not_on_moex   = sorted(bot_tickers  - moex_tickers)
    in_both       = bot_tickers & moex_tickers

    # ── новые на бирже, нет в боте ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  NEW ON MOEX, NOT IN BOT  ({len(new_on_moex)} tickers)")
    print(f"  Candidates to add to TICKER_KEYWORDS in filter.py")
    print(f"{'='*60}")
    if new_on_moex:
        for t in new_on_moex:
            name = short_name.get(t, "")
            print(f"  {t:<8}  {name}")
    else:
        print("  (none)")

    # ── есть в боте, нет на бирже ───────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  IN BOT, NOT ON MOEX  ({len(not_on_moex)} tickers)")
    print(f"  Possibly delisted — check if should remove from filter.py")
    print(f"{'='*60}")
    if not_on_moex:
        for t in not_on_moex:
            keywords = ", ".join(TICKER_KEYWORDS[t][:3])
            print(f"  {t:<8}  keywords: {keywords}")
    else:
        print("  (none)")

    # ── итог ─────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    print(f"  MOEX TQBR instruments:  {len(moex_tickers)}")
    print(f"  Bot TICKER_KEYWORDS:    {len(bot_tickers)}")
    print(f"  Covered by bot:         {len(in_both)} ({100*len(in_both)//len(moex_tickers)}% of MOEX)")
    print(f"  Missing from bot:       {len(new_on_moex)}")
    print(f"  Possibly delisted:      {len(not_on_moex)}")
    print()


if __name__ == "__main__":
    main()

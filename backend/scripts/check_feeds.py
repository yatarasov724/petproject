"""
Verify RSS feed availability and basic parsability.

Usage
─────
  python scripts/check_feeds.py               # check all seeds from queries.py
  python scripts/check_feeds.py --url <url>   # check a single URL

Output columns:
  STATUS  — OK / HTTP_<code> / TIMEOUT / ERROR / BOZO
  ENTRIES — number of entries in the feed (0 means empty)
  NAME    — source name
  URL     — feed URL
"""

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass

import aiohttp
import feedparser

_TIMEOUT  = aiohttp.ClientTimeout(total=15, connect=5)
_UA       = "MOEXNewsBot/check_feeds (+check)"

_CANDIDATES = [
    # ── already in seeds ─────────────────────────────────────────────────────
    ("TASS",          "https://tass.ru/rss/v2.xml"),
    ("Interfax",      "https://www.interfax.ru/rss"),
    ("Vedomosti",     "https://www.vedomosti.ru/rss/news"),
    ("Kommersant",    "https://www.kommersant.ru/rss/news.xml"),
    ("1prime",        "https://1prime.ru/export/rss2/index.xml"),

    # ── candidates to add ────────────────────────────────────────────────────
    ("RBK",           "https://rbc.ru/rss/news"),
    ("RBK-finance",   "https://rbc.ru/rss/finances"),
    ("RIA-economy",   "https://ria.ru/export/rss2/economy/index.xml"),
    ("Forbes-RU",     "https://www.forbes.ru/rss"),
    ("Banki-ru",      "https://www.banki.ru/xml/news.rss"),
    ("CBR",           "https://www.cbr.ru/rss/"),
    ("Finam",         "https://www.finam.ru/analysis/newsitem/rsspoint/"),
    ("Investing-RU",  "https://ru.investing.com/rss/news.rss"),
    ("Expert",        "https://expert.ru/rss/"),
    ("Fontanka-eco",  "https://www.fontanka.ru/export/rss/economics.xml"),
    ("RG-economy",    "https://rg.ru/xml/index.xml"),
    ("BFM",           "https://www.bfm.ru/news.rss"),
    ("Moex",          "https://www.moex.com/export/news.aspx?cat=1"),
]


@dataclass
class FeedStatus:
    name:    str
    url:     str
    status:  str
    entries: int
    elapsed: float


async def check_one(
    session: aiohttp.ClientSession,
    name: str,
    url: str,
) -> FeedStatus:
    t0 = time.monotonic()
    try:
        async with session.get(url) as resp:
            elapsed = time.monotonic() - t0
            if resp.status != 200:
                return FeedStatus(name, url, f"HTTP_{resp.status}", 0, elapsed)
            body = await resp.text(errors="replace")
    except asyncio.TimeoutError:
        return FeedStatus(name, url, "TIMEOUT", 0, time.monotonic() - t0)
    except Exception as exc:
        return FeedStatus(name, url, f"ERROR({exc.__class__.__name__})", 0, time.monotonic() - t0)

    feed = feedparser.parse(body)
    status = "BOZO" if (feed.bozo and not feed.entries) else "OK"
    return FeedStatus(name, url, status, len(feed.entries), elapsed)


async def _run(candidates: list[tuple[str, str]]) -> list[FeedStatus]:
    async with aiohttp.ClientSession(
        headers={"User-Agent": _UA},
        timeout=_TIMEOUT,
    ) as session:
        tasks = [check_one(session, name, url) for name, url in candidates]
        return await asyncio.gather(*tasks)


def _main() -> None:
    parser = argparse.ArgumentParser(description="Check RSS feed availability")
    parser.add_argument("--url", help="Check a single URL instead of all candidates")
    parser.add_argument("--name", default="custom", help="Name for --url check")
    args = parser.parse_args()

    candidates = [(args.name, args.url)] if args.url else _CANDIDATES
    results = asyncio.run(_run(candidates))

    results.sort(key=lambda r: (r.status != "OK", r.name))

    print(f"\n{'STATUS':<14} {'ENTRIES':>7} {'TIME':>6}  {'NAME':<18} URL")
    print("-" * 90)
    for r in results:
        marker = "✓" if r.status == "OK" else "✗"
        print(f"{marker} {r.status:<12} {r.entries:>7} {r.elapsed*1000:>5.0f}ms  {r.name:<18} {r.url}")

    ok = sum(1 for r in results if r.status == "OK")
    print(f"\n{ok}/{len(results)} feeds reachable\n")

    if args.url and results[0].status != "OK":
        sys.exit(1)


if __name__ == "__main__":
    _main()

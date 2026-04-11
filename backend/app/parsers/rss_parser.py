import hashlib
import feedparser
import aiohttp
from datetime import datetime
from typing import List
from email.utils import parsedate_to_datetime

from app.parsers.base import BaseParser, RawNews

RSS_SOURCES = [
    {"name": "RBC", "url": "https://rss.rbc.ru/finances/news.rss", "priority": 1},
    {"name": "TASS", "url": "https://tass.ru/rss/v2.xml", "priority": 1},
    {"name": "Interfax", "url": "https://www.interfax.ru/rss.asp", "priority": 1},
    {"name": "Vedomosti", "url": "https://www.vedomosti.ru/rss/news", "priority": 2},
    {"name": "Kommersant", "url": "https://www.kommersant.ru/RSS/news.xml", "priority": 2},
]


class RSSParser(BaseParser):
    def __init__(self):
        self._seen_hashes: set = set()

    def _make_hash(self, title: str, published_at: datetime) -> str:
        raw = f"{title}{published_at.isoformat()}"
        return hashlib.md5(raw.encode()).hexdigest()

    async def _fetch_source(self, session: aiohttp.ClientSession, source: dict) -> List[RawNews]:
        try:
            async with session.get(source["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
        except Exception:
            return []

        feed = feedparser.parse(text)
        results = []

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            content = entry.get("summary", entry.get("description", "")).strip()
            url = entry.get("link", "")

            try:
                published_at = parsedate_to_datetime(entry.get("published", ""))
            except Exception:
                published_at = datetime.utcnow()

            news_hash = self._make_hash(title, published_at)
            if news_hash in self._seen_hashes:
                continue

            self._seen_hashes.add(news_hash)
            results.append(RawNews(
                source=source["name"],
                title=title,
                content=content,
                url=url,
                published_at=published_at,
            ))

        return results

    async def fetch(self) -> List[RawNews]:
        async with aiohttp.ClientSession() as session:
            all_news = []
            for source in RSS_SOURCES:
                news = await self._fetch_source(session, source)
                all_news.extend(news)
            return all_news

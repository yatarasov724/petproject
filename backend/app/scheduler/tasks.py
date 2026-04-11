import asyncio
import pytz
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.parsers.rss_parser import RSSParser
from app.parsers.base import RawNews
from app.ai.filter import is_relevant, get_priority
from app.ai.groq_client import analyze_news

scheduler = AsyncIOScheduler()
rss_parser = RSSParser()
MOSCOW_TZ = pytz.timezone("Europe/Moscow")


def is_market_hours() -> bool:
    now = datetime.now(MOSCOW_TZ)
    if now.weekday() >= 5:
        return False
    open_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
    close_time = now.replace(hour=18, minute=50, second=0, microsecond=0)
    return open_time <= now <= close_time


async def process_news(news: RawNews):
    from app.core.database import SessionLocal
    from app.models.news import NewsItem
    from app.models.signal import Signal
    from app.api.routes.ws import manager
    import json
    import hashlib

    raw_hash = hashlib.md5(f"{news.title}{news.published_at.isoformat()}".encode()).hexdigest()
    db = SessionLocal()

    try:
        # Дедупликация
        if db.query(NewsItem).filter(NewsItem.raw_hash == raw_hash).first():
            return

        # Фильтр до AI
        if not is_relevant(news.title, news.content):
            return

        # Сохраняем новость
        news_item = NewsItem(
            source=news.source,
            title=news.title,
            content=news.content,
            url=news.url,
            published_at=news.published_at,
            raw_hash=raw_hash,
        )
        db.add(news_item)
        db.commit()
        db.refresh(news_item)

        # AI анализ
        priority = get_priority(news.source)
        result = await analyze_news(news.title, news.content, news.source, priority)

        if not result or result.get("action") == "IRRELEVANT":
            return

        # Сохраняем сигналы (может быть несколько тикеров)
        for ticker in result.get("tickers", []):
            signal = Signal(
                news_id=news_item.id,
                ticker=ticker,
                action=result["action"],
                confidence=result.get("confidence", 0),
                timeframe=result.get("timeframe"),
                explanation=result.get("explanation"),
                risk_factors=json.dumps(result.get("risk_factors", []), ensure_ascii=False),
                is_market_hours=is_market_hours(),
            )
            db.add(signal)
            db.commit()
            db.refresh(signal)

            # Broadcast через WebSocket
            await manager.broadcast({
                "type": "new_signal",
                "data": {
                    "id": signal.id,
                    "ticker": ticker,
                    "action": result["action"],
                    "confidence": result.get("confidence"),
                    "timeframe": result.get("timeframe"),
                    "explanation": result.get("explanation"),
                    "credibility": result.get("credibility"),
                    "risk_factors": result.get("risk_factors", []),
                    "news_title": news.title,
                    "source": news.source,
                    "is_market_hours": is_market_hours(),
                },
            })
    finally:
        db.close()


async def poll_rss():
    news_list = await rss_parser.fetch()
    for news in news_list:
        asyncio.create_task(process_news(news))


def start_scheduler():
    scheduler.add_job(poll_rss, "interval", seconds=30, id="rss_poll")
    scheduler.start()

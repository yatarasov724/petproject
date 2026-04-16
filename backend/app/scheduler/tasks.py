import hashlib
import logging
import pytz
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.parsers.rss_parser import RSSParser
from app.parsers.base import RawNews
from app.ai.filter import is_relevant
from app.bot.sender import send_news

logger = logging.getLogger(__name__)

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

    raw_hash = hashlib.md5(f"{news.title}{news.published_at.isoformat()}".encode()).hexdigest()
    db = SessionLocal()

    try:
        # Дедупликация
        if db.query(NewsItem).filter(NewsItem.raw_hash == raw_hash).first():
            return

        # Фильтр релевантности
        if not is_relevant(news.title, news.content):
            return

        # Сохраняем в БД
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

        # Отправляем в Telegram
        logger.info(f"[{news.source}] {news.title[:70]}")
        await send_news(news.title, news.source, news.url)

    except Exception as e:
        logger.error(f"Ошибка обработки новости '{news.title}': {e}")
    finally:
        db.close()


async def poll_rss():
    try:
        news_list = await rss_parser.fetch()
        for news in news_list:
            await process_news(news)
    except Exception as e:
        logger.error(f"Ошибка RSS парсера: {e}")


def start_scheduler():
    scheduler.add_job(poll_rss, "interval", seconds=30, id="rss_poll")
    scheduler.start()
    logger.info("Планировщик запущен")

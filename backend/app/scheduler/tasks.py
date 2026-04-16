import asyncio
import hashlib
import json
import logging
import pytz
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.parsers.rss_parser import RSSParser
from app.parsers.base import RawNews
from app.ai.filter import is_relevant, get_priority, VALID_TICKERS
from app.ai.groq_client import analyze_news
from app.bot.aggregator import SignalAggregator
from app.bot.sender import send_signal

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
rss_parser = RSSParser()
aggregator = SignalAggregator()

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

    raw_hash = hashlib.md5(f"{news.title}{news.published_at.isoformat()}".encode()).hexdigest()
    db = SessionLocal()

    try:
        # Дедупликация по БД
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

        # Валидация тикеров
        result["tickers"] = [t for t in result.get("tickers", []) if t in VALID_TICKERS]
        if not result["tickers"]:
            return

        # Сохраняем сигналы в БД
        for ticker in result["tickers"]:
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

        # Добавляем в агрегатор
        aggregator.add(result, news.title, news.source)

        # WebSocket broadcast
        await manager.broadcast({
            "type": "new_signal",
            "data": {
                "ticker": result["tickers"],
                "action": result["action"],
                "confidence": result.get("confidence"),
                "explanation": result.get("explanation"),
                "credibility": result.get("credibility"),
                "news_title": news.title,
                "source": news.source,
                "is_market_hours": is_market_hours(),
            },
        })

    except Exception as e:
        logger.error(f"Ошибка обработки новости '{news.title}': {e}")
    finally:
        db.close()


async def poll_rss():
    """Опрашивает RSS каждые 30 сек и обрабатывает новые новости."""
    try:
        news_list = await rss_parser.fetch()
        for news in news_list:
            await process_news(news)  # последовательно, не перегружаем БД
    except Exception as e:
        logger.error(f"Ошибка RSS парсера: {e}")


async def flush_aggregator():
    """Каждые 5 минут отправляет агрегированные сигналы в Telegram."""
    try:
        signals = aggregator.flush()
        for sig in signals:
            logger.info(f"Отправляю сигнал: {sig['ticker']} | {sig['action']} | {sig['confidence']}%")
            await send_signal(sig)
    except Exception as e:
        logger.error(f"Ошибка отправки сигналов: {e}")


async def start_telegram_listener():
    """Запускает Telegram парсер как фоновую задачу."""
    try:
        from app.parsers.telegram_parser import TelegramParser

        def on_news(news: RawNews):
            asyncio.create_task(process_news(news))

        tg = TelegramParser(on_news=on_news)
        asyncio.create_task(tg.start())
        logger.info("Telegram парсер запущен")
    except Exception as e:
        logger.error(f"Ошибка запуска Telegram парсера: {e}")


def start_scheduler():
    scheduler.add_job(poll_rss, "interval", seconds=30, id="rss_poll")
    scheduler.add_job(flush_aggregator, "interval", minutes=5, id="flush_aggregator")
    scheduler.start()
    logger.info("Планировщик запущен")

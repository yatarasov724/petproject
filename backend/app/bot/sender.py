import aiohttp
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

SOURCE_NAMES = {
    "RBC": "РБК",
    "TASS": "ТАСС",
    "Interfax": "Интерфакс",
    "Vedomosti": "Ведомости",
    "Kommersant": "Коммерсант",
}


async def send_news(title: str, source: str, url: str):
    source_ru = SOURCE_NAMES.get(source, source)
    text = f"*{source_ru}*\n{title}"
    if url:
        text += f"\n[Читать далее]({url})"

    url_api = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": settings.telegram_channel_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url_api, json=payload) as resp:
            result = await resp.json()
            if result.get("ok"):
                logger.info(f"Telegram OK: {title[:50]}")
            else:
                logger.error(f"Telegram ошибка: {result}")
            return result


async def send_signal(signal: dict):
    """Оставляем для будущего использования с AI."""
    pass

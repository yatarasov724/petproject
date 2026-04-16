import aiohttp
import os

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")

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

    url_api = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url_api, json=payload) as resp:
            return await resp.json()


async def send_signal(signal: dict):
    """Оставляем для будущего использования с AI."""
    pass

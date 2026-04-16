import logging
from typing import Callable
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from app.core.config import settings
from app.parsers.base import RawNews

logger = logging.getLogger(__name__)

TELEGRAM_CHANNELS = [
    "@markettwits",
    "@russianmacro",
    "@cbrstocks",
    "@moexnews",
]


class TelegramParser:
    def __init__(self, on_news: Callable[[RawNews], None]):
        self.on_news = on_news
        self.client = TelegramClient(
            StringSession(settings.telegram_session_string),
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )

    async def start(self):
        # Подключаемся без интерактивного ввода — сессия уже есть
        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.error("Telegram сессия недействительна. Перегенерируйте StringSession.")
            return

        logger.info("Telegram авторизован, слушаем каналы...")

        @self.client.on(events.NewMessage(chats=TELEGRAM_CHANNELS))
        async def handler(event):
            message = event.message
            if not message.text or len(message.text) < 20:
                return

            chat = await event.get_chat()
            source = f"TG:@{getattr(chat, 'username', 'unknown')}"

            news = RawNews(
                source=source,
                title=message.text[:150],
                content=message.text,
                url=f"https://t.me/{getattr(chat, 'username', 'unknown')}/{message.id}",
                published_at=message.date,
            )
            self.on_news(news)

        await self.client.run_until_disconnected()

    async def stop(self):
        await self.client.disconnect()

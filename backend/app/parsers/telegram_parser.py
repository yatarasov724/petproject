from typing import List, Callable
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from app.core.config import settings
from app.parsers.base import RawNews

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
        await self.client.start()

        @self.client.on(events.NewMessage(chats=TELEGRAM_CHANNELS))
        async def handler(event):
            message = event.message
            chat = await event.get_chat()
            source = getattr(chat, "username", "telegram") or "telegram"

            news = RawNews(
                source=f"TG:{source}",
                title=message.text[:100] if message.text else "",
                content=message.text or "",
                url=f"https://t.me/{source}/{message.id}",
                published_at=message.date,
            )
            self.on_news(news)

        await self.client.run_until_disconnected()

    async def stop(self):
        await self.client.disconnect()

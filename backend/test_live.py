"""
Живой тест: Telegram → фильтр → Groq AI → сигнал в консоль
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, ".")
os.environ.setdefault("DATABASE_URL", "sqlite:///./moex_assistant.db")
os.environ.setdefault("FRONTEND_URL", "http://localhost:3000")
os.environ.setdefault("SECRET_KEY", "test")

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from groq import AsyncGroq
from app.ai.filter import is_relevant, VALID_TICKERS
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION_STRING"]
GROQ_KEY = os.environ["GROQ_API_KEY"]

CHANNELS = ["@markettwits", "@russianmacro", "@cbrstocks"]

SYSTEM_PROMPT = """Ты — аналитик российского фондового рынка MOEX.
Анализируй новости и давай торговые сигналы ТОЛЬКО если новость ПРЯМО касается конкретной компании.

ПРАВИЛА:
1. Тикер должен быть ЯВНО упомянут в новости
2. Если связь косвенная — верни IRRELEVANT
3. Тикеры ТОЛЬКО из MOEX: SBER, GAZP, LKOH, YNDX, ROSN, GMKN, NVTK, TATN, VTBR, AFLT, MGNT, PHOR, ALRS, PLZL, OZON, CHMF, NLMK, MAGN, SNGS, MTSS, RTKM, MTLR
4. Confidence ниже 50% → верни IRRELEVANT

Формат (строго JSON):
{"tickers": ["GAZP"], "action": "BUY|SELL|HOLD|IRRELEVANT", "confidence": 0-100, "timeframe": "immediate|short|medium", "explanation": "...", "credibility": 0-100, "risk_factors": []}"""


async def analyze(text: str, source: str) -> dict | None:
    client = AsyncGroq(api_key=GROQ_KEY)
    resp = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Новость: {text[:500]}\nИсточник: {source}"},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    result = json.loads(resp.choices[0].message.content)
    result["tickers"] = [t for t in result.get("tickers", []) if t in VALID_TICKERS]
    if not result["tickers"]:
        result["action"] = "IRRELEVANT"
    return result


async def main():
    tg = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    await tg.connect()
    print("✅ Telegram подключён. Слушаем каналы...\n")

    @tg.on(events.NewMessage(chats=CHANNELS))
    async def handler(event):
        text = event.message.text or ""
        if not text or len(text) < 20:
            return

        chat = await event.get_chat()
        source = f"TG:@{getattr(chat, 'username', 'unknown')}"

        if not is_relevant(text, ""):
            print(f"[SKIP] {source}: {text[:60]}")
            return

        print(f"\n📨 Новость из {source}:")
        print(f"   {text[:100]}")
        print("   Анализирую...")

        try:
            result = await analyze(text, source)
            if result["action"] == "IRRELEVANT":
                print("   → Нерелевантно для рынка")
            else:
                action_icon = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(result["action"], "⚪")
                print(f"   {action_icon} {result['tickers']} | {result['action']} | {result['confidence']}%")
                print(f"   💬 {result.get('explanation', '')}")
        except Exception as e:
            print(f"   ❌ Ошибка AI: {e}")

    await tg.run_until_disconnected()


asyncio.run(main())

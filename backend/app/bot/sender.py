import aiohttp
import os

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")

ACTION_RU = {"BUY": "🟢 ПОКУПАТЬ", "SELL": "🔴 ПРОДАВАТЬ", "HOLD": "🟡 ДЕРЖАТЬ"}
TIMEFRAME_RU = {"immediate": "немедленно", "short": "краткосрочно", "medium": "среднесрочно"}


async def send_signal(signal: dict):
    ticker = signal.get("ticker", "")
    action = signal.get("action", "")
    confidence = signal.get("confidence", 0)
    credibility = signal.get("credibility", 0)
    explanation = signal.get("explanation", "")
    risk_factors = signal.get("risk_factors", [])
    timeframe = TIMEFRAME_RU.get(signal.get("timeframe", ""), "—")
    news_title = signal.get("news_title", "")
    source = signal.get("source", "")
    news_count = signal.get("news_count", 1)
    conflict_note = signal.get("conflict_note", "")

    action_ru = ACTION_RU.get(action, action)

    news_count_text = f" ({news_count} новости)" if news_count > 1 else ""
    conflict_text = f"\n_{conflict_note}_" if conflict_note else ""

    risks_text = ""
    if risk_factors:
        risks_text = "\nРиски: " + "; ".join(risk_factors)

    text = (
        f"*{ticker}* — {action_ru}{news_count_text}\n"
        f"{news_title}\n"
        f"_{source}, {timeframe}_\n"
        f"\n"
        f"{explanation}"
        f"{conflict_text}\n"
        f"\n"
        f"Уверенность: *{confidence}%* | Достоверность: *{credibility}%*"
        f"{risks_text}"
    )

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL_ID,
        "text": text,
        "parse_mode": "Markdown",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            return await resp.json()

"""
Тест полного pipeline: RSS → фильтр → Groq AI → агрегатор → Telegram канал
"""
import asyncio, sys, os, json
sys.path.insert(0, ".")

from dotenv import load_dotenv
load_dotenv()

from groq import AsyncGroq
from app.parsers.rss_parser import RSSParser
from app.ai.filter import is_relevant, VALID_TICKERS
from app.bot.aggregator import SignalAggregator
from app.bot.sender import send_signal

GROQ_KEY = os.environ["GROQ_API_KEY"]

SYSTEM_PROMPT = """Ты — аналитик российского фондового рынка MOEX.
Анализируй новости и давай торговые сигналы ТОЛЬКО если новость ПРЯМО касается конкретной компании.

ПРАВИЛА:
1. Тикер должен быть ЯВНО упомянут в новости
2. Если связь косвенная — верни IRRELEVANT
3. Тикеры ТОЛЬКО из MOEX: SBER, GAZP, LKOH, YNDX, ROSN, GMKN, NVTK, TATN, VTBR, AFLT, MGNT, PHOR, ALRS, PLZL, OZON, CHMF, NLMK, MAGN, SNGS, MTSS, RTKM, MTLR
4. Confidence ниже 50% → верни IRRELEVANT

Формат (строго JSON):
{"tickers": ["GAZP"], "action": "BUY|SELL|HOLD|IRRELEVANT", "confidence": 0-100, "timeframe": "immediate|short|medium", "explanation": "...", "credibility": 0-100, "risk_factors": []}"""


async def analyze(title: str, content: str, source: str) -> dict:
    client = AsyncGroq(api_key=GROQ_KEY)
    resp = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Новость: {title}\n{content[:300]}\nИсточник: {source}"},
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
    print("Получаем новости из RSS...")
    parser = RSSParser()
    news = await parser.fetch()
    relevant = [n for n in news if is_relevant(n.title, n.content)]
    print(f"Релевантных: {len(relevant)} из {len(news)}\n")

    aggregator = SignalAggregator()

    for n in relevant:
        result = await analyze(n.title, n.content, n.source)
        if result.get("action") == "IRRELEVANT":
            print(f"[SKIP] {n.title[:60]}")
            continue

        print(f"[+] {result['tickers']} | {result['action']} | {result['confidence']}% — {n.title[:50]}")
        aggregator.add(result, n.title, n.source)
        await asyncio.sleep(2)

    # Финальные сигналы после агрегации
    final_signals = aggregator.flush()
    print(f"\nПосле агрегации: {len(final_signals)} сигналов к отправке\n")

    for sig in final_signals:
        print(f"Отправляю: {sig['ticker']} | {sig['action']} | {sig['confidence']}%")
        if sig.get("conflict_note"):
            print(f"  Конфликт: {sig['conflict_note']}")
        await send_signal(sig)

    print("\nГотово!")


asyncio.run(main())

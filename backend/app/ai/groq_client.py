import asyncio
from groq import AsyncGroq
from app.core.config import settings

client = AsyncGroq(api_key=settings.groq_api_key)

# Очередь запросов с приоритетами (меньше = выше приоритет)
_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()


async def analyze_news(title: str, content: str, source: str, priority: int = 2) -> dict | None:
    future: asyncio.Future = asyncio.get_event_loop().create_future()
    await _queue.put((priority, title, content, source, future))
    return await future


async def _worker():
    while True:
        priority, title, content, source, future = await _queue.get()
        try:
            result = await _call_groq(title, content, source)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        finally:
            _queue.task_done()
            await asyncio.sleep(2)  # ~30 req/min rate limit


async def _call_groq(title: str, content: str, source: str) -> dict | None:
    prompt = f"Новость: {title}\n\nПодробности: {content}\n\nИсточник: {source}"

    response = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    import json
    return json.loads(response.choices[0].message.content)


SYSTEM_PROMPT = """Ты — аналитик российского фондового рынка MOEX.
Анализируй политические и экономические новости и давай торговые сигналы ТОЛЬКО для российских акций.
Отвечай СТРОГО в JSON формате. Тикеры должны быть из списка MOEX (SBER, GAZP, LKOH, YNDX, и т.д.).

Если новость не релевантна рынку — верни action: "IRRELEVANT".

Формат ответа:
{
  "tickers": ["SBER"],
  "action": "BUY" | "SELL" | "HOLD" | "IRRELEVANT",
  "confidence": 0-100,
  "timeframe": "immediate" | "short" | "medium",
  "explanation": "Краткое объяснение на русском",
  "credibility": 0-100,
  "risk_factors": ["риск 1", "риск 2"]
}"""

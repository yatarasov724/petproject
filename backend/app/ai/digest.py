"""
AI-powered daily digest summary via OpenRouter.

Takes a list of top cluster headlines and returns:
  - Per-headline: emoji (🟢/🔴) + 1-phrase market effect
  - Overall:      1-2 sentence session summary

One API call for the entire digest — no per-cluster overhead.
Returns None on any failure so the digest falls back to static formatting.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

from app.core.config import settings

logger = logging.getLogger(__name__)

_API_URL = "https://openrouter.ai/api/v1/chat/completions"
_MODEL   = "meta-llama/llama-3.3-70b-instruct"
_TIMEOUT = aiohttp.ClientTimeout(total=20)

_SYSTEM_PROMPT = """\
Ты аналитик финансовых новостей. Получаешь пронумерованный список заголовков событий за торговую сессию.

ЗАДАЧА
Для каждого заголовка определи тональность (🟢 позитив / 🔴 негатив) и напиши рыночный эффект (≤ 8 слов).
Затем напиши 1–2 предложения с кратким итогом всей сессии.

ПРАВИЛА
- Только русский язык
- Стиль: сухой, аналитический
- emoji: строго 🟢 (позитив) или 🔴 (негатив), нейтральных нет
- items должен содержать ровно столько элементов, сколько заголовков получено

Ответ строго JSON без пояснений:
{
  "items": [
    {"emoji": "🔴", "effect": "давление на акции нефтяников"},
    {"emoji": "🟢", "effect": "поддержка рубля и ОФЗ"}
  ],
  "summary": "Сессия завершилась под давлением. Ключевые темы: ..."
}\
"""


@dataclass(frozen=True)
class DigestAnalysis:
    items:   list[dict]  # [{"emoji": "🟢|🔴", "effect": "..."}], len == len(headlines)
    summary: str         # 1-2 sentence session summary


async def generate_digest(headlines: list[str]) -> Optional[DigestAnalysis]:
    """
    Generate AI summary for a list of top-event headlines.
    Returns None on any failure — caller formats digest without per-item sentiment.
    """
    if not settings.openrouter_api_key or not headlines:
        return None

    user_msg = "\n".join(f"{i+1}. {h}" for i, h in enumerate(headlines))

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 600,
    }
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type":  "application/json",
    }

    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(_API_URL, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        "OpenRouter %d for digest: %s", resp.status, body[:200]
                    )
                    return None
                body = await resp.json(content_type=None)

        raw = body["choices"][0]["message"]["content"] or ""
        # Strip markdown code fences: some models wrap JSON in ```json ... ```
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]          # drop opening fence line
            raw = raw.rsplit("```", 1)[0].strip()  # drop closing fence
        data = json.loads(raw)
        return _validate(data, expected_count=len(headlines))

    except Exception:
        logger.warning("AI digest generation failed", exc_info=True)
        return None


def _validate(data: dict, expected_count: int) -> Optional[DigestAnalysis]:
    try:
        items   = data.get("items", [])
        summary = str(data.get("summary", "")).strip()

        if not isinstance(items, list) or len(items) != expected_count:
            logger.warning(
                "AI digest: expected %d items, got %d — ignoring",
                expected_count, len(items) if isinstance(items, list) else -1,
            )
            return None

        validated = []
        for item in items:
            emoji  = str(item.get("emoji", "🔴"))
            effect = str(item.get("effect", "")).strip()
            if emoji not in ("🟢", "🔴"):
                emoji = "🔴"
            validated.append({"emoji": emoji, "effect": effect})

        return DigestAnalysis(items=validated, summary=summary)
    except Exception:
        logger.warning("AI digest validation failed", exc_info=True)
        return None

"""
AI-powered news analysis via OpenRouter.

Uses the OpenAI-compatible chat completions endpoint.
Called only for articles that passed the full pipeline (publishable events),
so at most a few dozen calls per day — well within any free-tier quota.

Returns None on any failure so the pipeline always continues with static formatting.
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
_TIMEOUT = aiohttp.ClientTimeout(total=15)

_SYSTEM_PROMPT = """\
Ты обрабатываешь новостной текст и превращаешь его в короткий Telegram-пост для трейдеров и инвесторов.

ЗАДАЧА
Сжать новость, определить её влияние на рынок и выдать структурированный, лаконичный результат.

---

1. ЗАГОЛОВОК
- максимум ~8 слов
- отражает суть события
- без воды
- без точки в конце
- без кавычек

---

2. ДЕСКРИПШН (summary)
- 1 короткое предложение
- дополняет заголовок (не повторяет его)
- только факты
- без лишних деталей

---

3. СТАТУС (ЭМОДЗИ)
🟢 — позитив
🔴 — негатив

Логика:
Позитив — рост экономики, смягчение ЦБ, рост сырья, снижение рисков, деэскалация
Негатив — санкции, ужесточение, падение рынков, эскалация, неопределённость
Нейтральных оценок нет — всегда выбирай преобладающее направление

---

4. ДЛЯ РЫНКА (market_effect)
- 1 короткое предложение
- прямой эффект (давление / поддержка / рост / падение)

---

5. ВЛИЯЕТ НА (affects) — ВСЕГДА НА РУССКОМ
stocks / equities → акции
ruble → рубль
bonds / OFZ → ОФЗ
commodities → сырьё
oil → нефть
gold → золото
forex → валюты

Формат: акции · рубль · ОФЗ · сырьё
Если новость не оказывает конкретного влияния на активы — верни пустую строку ""

---

6. ПРАВИЛА
- никакого английского
- без категории
- коротко и по делу
- стиль сухой, аналитический

---

Отвечай СТРОГО JSON, без пояснений:
{
  "title": "короткий заголовок (~8 слов, без точки, без кавычек)",
  "impact": "positive | negative",
  "emoji": "🟢 | 🔴",
  "summary": "1 предложение — что произошло (не повторяет заголовок)",
  "market_effect": "прямой эффект на рынок (1 предложение)",
  "affects": "акции · рубль · ОФЗ · сырьё"
}\
"""

_USER_TEMPLATE = "Заголовок: {title}\nТекст: {text}"

_VALID_IMPACTS = frozenset({"positive", "negative"})
_VALID_EMOJIS  = frozenset({"🟢", "🔴"})


@dataclass(frozen=True)
class AIAnalysis:
    title:         str
    impact:        str   # positive | negative
    emoji:         str   # 🟢 | 🔴
    summary:       str
    market_effect: str
    affects:       str   # "акции · рубль · ОФЗ · сырьё"


async def analyze(title: str, text: str = "") -> Optional[AIAnalysis]:
    """
    Analyze a publishable news headline via OpenRouter.
    Returns AIAnalysis on success, None on any failure.
    """
    if not settings.openrouter_api_key:
        return None

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": _USER_TEMPLATE.format(
                title=title, text=text or title,
            )},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 300,
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
                        "OpenRouter %d for: %.60s — %s",
                        resp.status, title, body[:200],
                    )
                    return None
                body = await resp.json(content_type=None)

        raw  = body["choices"][0]["message"]["content"] or ""
        data = json.loads(raw)
        return _validate(data)

    except Exception:
        logger.warning("AI analysis failed for: %.60s", title, exc_info=True)
        return None


def _validate(data: dict) -> Optional[AIAnalysis]:
    """Coerce and validate LLM response. Returns None if any required field is missing."""
    try:
        ai_title      = str(data.get("title", "")).strip()
        summary       = str(data.get("summary", "")).strip()
        market_effect = str(data.get("market_effect", "")).strip()
        affects       = str(data.get("affects", "")).strip()

        if not ai_title or not summary or not market_effect:
            return None

        impact = str(data.get("impact", "negative")).lower()
        emoji  = str(data.get("emoji", "🔴"))

        if impact not in _VALID_IMPACTS:
            impact = "negative"
        if emoji not in _VALID_EMOJIS:
            emoji = "🔴"

        return AIAnalysis(
            title=ai_title,
            impact=impact,
            emoji=emoji,
            summary=summary,
            market_effect=market_effect,
            affects=affects,
        )
    except Exception:
        return None

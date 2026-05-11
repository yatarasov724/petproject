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

6. ТИКЕРЫ MOEX (tickers)
Полный список допустимых тикеров:

Нефть и газ:
GAZP (Газпром, Миллер), LKOH (Лукойл, Алекперов), ROSN (Роснефть, Сечин),
NVTK (Новатэк, Михельсон), TATN (Татнефть), SNGS (Сургутнефтегаз),
ENPG (Эн+), TRNFP (Транснефть), BANEP (Башнефть)

Банки и финансы:
SBER (Сбербанк, Сбер, Греф), VTBR (ВТБ), TCSG (Тинькофф, Т-Банк),
CBOM (МКБ, Московский кредитный банк), BSPB (Банк Санкт-Петербург),
AFKS (АФК Система), SVCB (Совкомбанк), SPBE (СПБ Биржа), RENI (Ренессанс Страхование)

Металлы и горная добыча:
GMKN (Норникель, Потанин), CHMF (Северсталь, Мордашов), NLMK (НЛМК),
MAGN (ММК, Магнитогорский), PLZL (Полюс, Полюс Золото), ALRS (Алроса),
POLY (Полиметалл, Polymetal), MTLR (Мечел), SELG (Селигдар),
RUAL (Русал, алюминий), RASP (Распадская, уголь)

Электроэнергетика:
IRAO (Интер РАО), HYDR (РусГидро), FEES (ФСК ЕЭС, Россети)

IT и телеком:
YNDX (Яндекс, Yandex), MTSS (МТС), RTKM (Ростелеком),
VKCO (ВКонтакте, VK), POSI (Позитив Текнолоджис),
HHRU (Хедхантер, HeadHunter), OZON (Ozon, Озон)

Транспорт:
FLOT (Совкомфлот, танкеры), AFLT (Аэрофлот)

Ритейл, агро, прочее:
MGNT (Магнит), FIVE (X5, Пятёрочка, Перекрёсток), FIXP (Fix Price),
PHOR (ФосАгро), AGRO (РусАгро), MOEX (Мосбиржа, Московская биржа),
SGZH (Сегежа)

Недвижимость:
SMLT (Самолёт), PIKK (ПИК), LSRG (ЛСР), ETLN (Эталон)

Правила:
- Только тикеры из списка выше
- Только если компания ПРЯМО упомянута в новости ИЛИ применяется одно из правил ниже
- ОБЯЗАТЕЛЬНО: нефтяные котировки / цена нефти / ОПЕК / нефтяной рынок → ["LKOH", "ROSN"]
- ОБЯЗАТЕЛЬНО: цена газа / газовый рынок (без конкретной компании) → ["GAZP", "NVTK"]
- ОБЯЗАТЕЛЬНО: золото / цена золота (без компании) → ["PLZL"]
- Макроэкономика, ЦБ, санкции без конкретной компании → пустой массив []
- Максимум 3 тикера

---

7. ПРАВИЛА
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
  "affects": "акции · рубль · ОФЗ · сырьё",
  "tickers": ["SBER", "GAZP"]
}\
"""

_USER_TEMPLATE = "Заголовок: {title}\nТекст: {text}"

_VALID_IMPACTS = frozenset({"positive", "negative"})
_VALID_EMOJIS  = frozenset({"🟢", "🔴"})
_VALID_TICKERS = frozenset({
    # Нефть и газ
    "GAZP", "LKOH", "ROSN", "NVTK", "TATN", "SNGS", "ENPG", "TRNFP", "BANEP",
    # Банки и финансы
    "SBER", "VTBR", "TCSG", "CBOM", "BSPB", "AFKS", "SVCB", "SPBE", "RENI",
    # Металлы и горная добыча
    "GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "POLY", "MTLR", "SELG", "RUAL", "RASP",
    # Электроэнергетика
    "IRAO", "HYDR", "FEES",
    # IT и телеком
    "YNDX", "MTSS", "RTKM", "VKCO", "POSI", "HHRU", "OZON",
    # Транспорт
    "FLOT", "AFLT",
    # Ритейл, агро, прочее
    "MGNT", "FIVE", "FIXP", "PHOR", "AGRO", "MOEX", "SGZH",
    # Недвижимость
    "SMLT", "PIKK", "LSRG", "ETLN",
})


@dataclass(frozen=True)
class AIAnalysis:
    title:         str
    impact:        str         # positive | negative
    emoji:         str         # 🟢 | 🔴
    summary:       str
    market_effect: str
    affects:       str         # "акции · рубль · ОФЗ · сырьё"
    tickers:       list[str]   # validated MOEX tickers, e.g. ["SBER", "GAZP"]


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
        "max_tokens": 400,
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

        raw_tickers = data.get("tickers", [])
        if isinstance(raw_tickers, list):
            tickers = [t for t in raw_tickers if isinstance(t, str) and t.upper() in _VALID_TICKERS]
        else:
            tickers = []

        return AIAnalysis(
            title=ai_title,
            impact=impact,
            emoji=emoji,
            summary=summary,
            market_effect=market_effect,
            affects=affects,
            tickers=tickers,
        )
    except Exception:
        return None

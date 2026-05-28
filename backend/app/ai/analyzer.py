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

6. КОНТЕКСТ (context)
Если переданы «Последние события по этим тикерам» — напиши 1 предложение:
почему данное событие важно именно сейчас, в контексте этих событий.
Стиль: «На фоне …», «После …», «Вопреки …».
Если последних событий нет или они не релевантны — верни пустую строку "".
Максимум 15 слов.

---

7. ТИКЕРЫ MOEX (tickers)
Полный список допустимых тикеров:

Нефть и газ:
GAZP (Газпром, Миллер), LKOH (Лукойл, Алекперов), ROSN (Роснефть, Сечин),
NVTK (Новатэк, Михельсон), TATN (Татнефть), SNGS (Сургутнефтегаз),
ENPG (Эн+), TRNFP (Транснефть), BANEP (Башнефть), RNFT (Русснефть),
NMTP (НМТП, Новороссийский морской торговый порт)

Банки и финансы:
SBER (Сбербанк, Сбер, Греф), VTBR (ВТБ), TCSG (Тинькофф, Т-Банк),
CBOM (МКБ, Московский кредитный банк), BSPB (Банк Санкт-Петербург),
AFKS (АФК Система), SVCB (Совкомбанк), SPBE (СПБ Биржа), RENI (Ренессанс Страхование),
MTSB (МТС-Банк — это БАНК, не оператор связи МТС!)

Металлы и горная добыча:
GMKN (Норникель, Потанин), CHMF (Северсталь, Мордашов), NLMK (НЛМК),
MAGN (ММК, Магнитогорский), PLZL (Полюс, Полюс Золото), ALRS (Алроса),
POLY (Полиметалл, Polymetal), MTLR (Мечел), SELG (Селигдар),
RUAL (Русал, алюминий), RASP (Распадская, уголь)

Электроэнергетика:
IRAO (Интер РАО), HYDR (РусГидро), FEES (ФСК ЕЭС, Россети — только головная компания!),
MSRS (Россети Московский регион), MRKV (Россети Волга),
MRKU (Россети Урал), MRKP (Россети Центр и Приволжье), MRKC (Россети Центр)

IT и телеком:
YNDX (Яндекс, Yandex), MTSS (МТС — оператор связи, НЕ МТС-Банк!), RTKM (Ростелеком),
VKCO (ВКонтакте, VK), POSI (Позитив Текнолоджис),
HHRU (Хедхантер, HeadHunter), OZON (Ozon-маркетплейс), DIAS (Диасофт)

Транспорт:
FLOT (Совкомфлот, танкеры), AFLT (Аэрофлот)

Ритейл, агро, прочее:
MGNT (Магнит), FIVE (X5, Пятёрочка, Перекрёсток), FIXP (Fix Price),
PHOR (ФосАгро), AGRO (РусАгро), MOEX (Мосбиржа, Московская биржа),
SGZH (Сегежа), MGKL (МГКЛ, Мосгорломбард), OZPH (Озон Фармацевтика)

Недвижимость:
SMLT (Самолёт), PIKK (ПИК), LSRG (ЛСР), ETLN (Эталон)

Правила:
- Только тикеры из списка выше — НИКАКИХ других
- Только если компания ПРЯМО упомянута в новости ИЛИ применяется одно из правил ниже
- ОБЯЗАТЕЛЬНО: нефтяные котировки / цена нефти / ОПЕК / нефтяной рынок → ["LKOH", "ROSN"]
- ОБЯЗАТЕЛЬНО: цена газа / газовый рынок (без конкретной компании) → ["GAZP", "NVTK"]
- ОБЯЗАТЕЛЬНО: золото / цена золота (без компании) → ["PLZL"]
- Макроэкономика, ЦБ, санкции без конкретной компании → пустой массив []
- Максимум 3 тикера

КРИТИЧЕСКИ ВАЖНО — ЗАПРЕЩЁННЫЕ ОШИБКИ:
❌ МГКЛ → НЕ MTSS, НЕ FEES, НЕ AFLT. МГКЛ = MGKL (Мосгорломбард)
❌ МТС-Банк → НЕ MTSS. МТС-Банк = MTSB
❌ Озон Фармацевтика → НЕ OZON. Озон Фармацевтика = OZPH
❌ AutoZone (американская компания) → [] (не торгуется на MOEX, тикера нет)
❌ Иностранная компания (Goldman Sachs, Evercore, Apple, ...) → []
❌ НМТП → НЕ VTBR. НМТП = NMTP
❌ Русснефть → НЕ LKOH, НЕ ROSN. Русснефть = RNFT
❌ Если компания есть в новости, но её тикера НЕТ в списке выше → обязательно []
❌ Не угадывай тикер похожей компании — лучше вернуть [], чем ошибиться

---

8. ПРАВИЛА
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
  "tickers": ["SBER", "GAZP"],
  "context": "На фоне падения цен на газ в Европе"
}\
"""

_USER_TEMPLATE = "Заголовок: {title}\nТекст: {text}"

_VALID_IMPACTS = frozenset({"positive", "negative"})
_VALID_EMOJIS  = frozenset({"🟢", "🔴"})
_VALID_TICKERS = frozenset({
    # Нефть и газ
    "GAZP", "LKOH", "ROSN", "NVTK", "TATN", "SNGS", "ENPG", "TRNFP", "BANEP",
    "RNFT", "NMTP",
    # Банки и финансы
    "SBER", "VTBR", "TCSG", "CBOM", "BSPB", "AFKS", "SVCB", "SPBE", "RENI", "MTSB",
    # Металлы и горная добыча
    "GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "POLY", "MTLR", "SELG", "RUAL", "RASP",
    # Электроэнергетика
    "IRAO", "HYDR", "FEES", "MSRS", "MRKV", "MRKU", "MRKP", "MRKC",
    # IT и телеком
    "YNDX", "MTSS", "RTKM", "VKCO", "POSI", "HHRU", "OZON", "DIAS",
    # Транспорт
    "FLOT", "AFLT",
    # Ритейл, агро, прочее
    "MGNT", "FIVE", "FIXP", "PHOR", "AGRO", "MOEX", "SGZH", "MGKL", "OZPH",
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
    context:       str = ""    # why this matters now (from RAG), may be empty


async def analyze(title: str, text: str = "", recent_context: list[str] | None = None) -> Optional[AIAnalysis]:
    """
    Analyze a publishable news headline via OpenRouter.
    Returns AIAnalysis on success, None on any failure.
    """
    if not settings.openrouter_api_key:
        return None

    user_content = _USER_TEMPLATE.format(title=title, text=text or title)
    if recent_context:
        events_block = "\n".join(f"• {t}" for t in recent_context[:5])
        user_content += f"\n\nПоследние события по этим тикерам:\n{events_block}"

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_content},
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

        context = str(data.get("context", "")).strip()

        return AIAnalysis(
            title=ai_title,
            impact=impact,
            emoji=emoji,
            summary=summary,
            market_effect=market_effect,
            affects=affects,
            tickers=tickers,
            context=context,
        )
    except Exception:
        return None

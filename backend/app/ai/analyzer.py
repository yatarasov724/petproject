"""
AI-powered news analysis via OpenRouter (free tier).

Uses openai/gpt-oss-120b:free — no balance required, 200 req/day limit.
Called only for articles that passed the full pipeline (publishable events),
so at most a few dozen calls per day — well within free quota.

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
_MODEL   = "openai/gpt-oss-120b:free"
_TIMEOUT = aiohttp.ClientTimeout(total=20)

_SYSTEM_PROMPT = """\
Ты — «Бычок», AI-помощник по российскому фондовому рынку для частных инвесторов. Аудитория — обычные люди, многие новички. Ты объясняешь рыночные новости простым человеческим языком: спокойно, по делу, без воды, жаргона, паники и пампа. Дружелюбный, но уверенный — как толковый знакомый, который разбирается в рынке, а не гуру с сигналами.

ЗАДАЧА
На вход приходит новость. Преврати её в короткий структурированный пост. Ты про факты и контекст, а не про советы.

---

1. СУТЬ (summary)
- 1 строка, своими словами
- только факт, без оценки
- без воды и жаргона
- максимум ~15 слов

---

2. ЧТО ЗА ЭТИМ СТОИТ (what_behind)
- 1–2 строки
- что это значит для бумаги: разовое или тренд, как бьётся с прежней картиной, на что влияет
- без призывов к действию
- если переданы «Последние события по этим тикерам» — используй их как контекст

---

3. НА ЧТО СМОТРЕТЬ (watch_for)
- 1 строка
- нейтральный ориентир на будущее: следующий отчёт, метрика, событие
- без призывов действовать

---

4. ТОНАЛЬНОСТЬ (sentiment)
- "positive" — новость в целом позитивна для акций/рынка
- "negative" — новость в целом негативна для акций/рынка
- Только одно из двух значений: positive или negative

---

5. ТИКЕРЫ MOEX (tickers)
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
❌ AutoZone (американская компания, тикер AZO) → [] (не торгуется на MOEX)
❌ Иностранная компания (Goldman Sachs, Evercore, Apple, FalconX, Coinbase, Binance...) → []
❌ Крипто-компании и крипто-биржи (FalconX, Coinbase, Bybit, Kraken, OKX...) → []
❌ «Озоновый слой», «слой озона», «озоновая дыра» — природное явление, НЕ компания → []
❌ НМТП → НЕ VTBR. НМТП = NMTP
❌ Русснефть → НЕ LKOH, НЕ ROSN. Русснефть = RNFT
❌ Если компания есть в новости, но её тикера НЕТ в списке выше → обязательно []
❌ Не угадывай тикер похожей компании — лучше вернуть [], чем ошибиться

---

ПРАВИЛА
- никакого английского в тексте ответа
- стиль простой, человеческий, без воды
- без советов, сигналов и призывов к действию

Отвечай СТРОГО JSON, без пояснений:
{
  "summary": "суть одной строкой (~15 слов)",
  "what_behind": "что за этим стоит (1–2 строки)",
  "watch_for": "нейтральный ориентир на будущее (1 строка)",
  "sentiment": "positive или negative",
  "tickers": ["SBER", "GAZP"]
}\
"""

_USER_TEMPLATE = "Заголовок: {title}\nТекст: {text}"

_VALID_TICKERS = frozenset({
    "GAZP", "LKOH", "ROSN", "NVTK", "TATN", "SNGS", "ENPG", "TRNFP", "BANEP",
    "RNFT", "NMTP",
    "SBER", "VTBR", "TCSG", "CBOM", "BSPB", "AFKS", "SVCB", "SPBE", "RENI", "MTSB",
    "GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "POLY", "MTLR", "SELG", "RUAL", "RASP",
    "IRAO", "HYDR", "FEES", "MSRS", "MRKV", "MRKU", "MRKP", "MRKC",
    "YNDX", "MTSS", "RTKM", "VKCO", "POSI", "HHRU", "OZON", "DIAS",
    "FLOT", "AFLT",
    "MGNT", "FIVE", "FIXP", "PHOR", "AGRO", "MOEX", "SGZH", "MGKL", "OZPH",
    "SMLT", "PIKK", "LSRG", "ETLN",
})


@dataclass(frozen=True)
class AIAnalysis:
    summary:     str
    what_behind: str
    watch_for:   str
    tickers:     list[str]
    sentiment:   Optional[str]  # "positive" | "negative" | None


async def analyze(title: str, text: str = "", recent_context: list[str] = []) -> Optional[AIAnalysis]:
    """
    Analyze a publishable news headline via OpenRouter free tier.
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

        raw = body["choices"][0]["message"]["content"] or ""
        if not raw.strip():
            logger.warning("OpenRouter empty content for: %.60s", title)
            return None
        data = json.loads(raw)
        return _validate(data)

    except Exception:
        logger.warning("AI analysis failed for: %.60s", title, exc_info=True)
        return None


def _validate(data: dict) -> Optional[AIAnalysis]:
    try:
        summary     = str(data.get("summary", "")).strip()
        what_behind = str(data.get("what_behind", "")).strip()
        watch_for   = str(data.get("watch_for", "")).strip()

        if not summary or not what_behind:
            return None

        raw_tickers = data.get("tickers", [])
        if isinstance(raw_tickers, list):
            tickers = [t for t in raw_tickers if isinstance(t, str) and t.upper() in _VALID_TICKERS]
        else:
            tickers = []

        raw_sentiment = str(data.get("sentiment", "")).strip().lower()
        sentiment = raw_sentiment if raw_sentiment in ("positive", "negative") else None

        return AIAnalysis(
            summary=summary,
            what_behind=what_behind,
            watch_for=watch_for,
            tickers=tickers,
            sentiment=sentiment,
        )
    except Exception:
        return None

"""
AI-powered news analysis via OpenRouter (free tier).

Uses openai/gpt-oss-120b:free — no balance required, 200 req/day limit.
Called only for articles that passed the full pipeline (publishable events),
so at most a few dozen calls per day — well within free quota.

Returns None on any failure so the pipeline always continues with static formatting.
"""

import json
from datetime import date
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
- 1 строка
- конкретное рыночное следствие: какие сектора или бумаги под давлением или выигрывают и почему
- формат: «[конкретная причина] — [следствие для рынка/акций]»
- запрещено: «ситуация сохраняется», «давление продолжается», «риски остаются», «ограничения в силе» — не несут информации
- если переданы «Последние события по этим тикерам» — используй их как контекст

---

3. НА ЧТО СМОТРЕТЬ (watch_for)
- 1 строка
- назови конкретное следующее событие или метрику: заседание ЦБ, дата отчёта, переговоры, данные по экспорту
- НЕЛЬЗЯ начинать с «Следить за», «Следим за» — сразу называй что именно
- если конкретной даты нет — укажи метрику или индикатор: объём экспорта, ставка, квартальный отчёт
- запрещено: «следить за официальными заявлениями» — слишком размыто

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
SBER (Сбербанк, Сбер, Греф), VTBR (ВТБ), T (Тинькофф, Т-Банк, Т-Технологии),
CBOM (МКБ, Московский кредитный банк), BSPB (Банк Санкт-Петербург),
AFKS (АФК Система), SVCB (Совкомбанк), SPBE (СПБ Биржа), RENI (Ренессанс Страхование),
MBNK (МТС-Банк — это БАНК, не оператор связи МТС!)

Металлы и горная добыча:
GMKN (Норникель, Потанин), CHMF (Северсталь, Мордашов), NLMK (НЛМК),
MAGN (ММК, Магнитогорский), PLZL (Полюс, Полюс Золото), ALRS (Алроса),
MTLR (Мечел), SELG (Селигдар),
RUAL (Русал, алюминий), RASP (Распадская, уголь)

Электроэнергетика:
IRAO (Интер РАО), HYDR (РусГидро), FEES (ФСК ЕЭС, Россети — только головная компания!),
MSNG (Мосэнерго — НЕ путать с MSRS!), MSRS (Россети Московский регион), MRKV (Россети Волга),
MRKU (Россети Урал), MRKP (Россети Центр и Приволжье), MRKC (Россети Центр)

IT и телеком:
YDEX (Яндекс, Yandex), MTSS (МТС — оператор связи, НЕ МТС-Банк!), RTKM (Ростелеком),
VKCO (ВКонтакте, VK), POSI (Позитив Текнолоджис),
HEAD (Хедхантер, HeadHunter), OZON (Ozon-маркетплейс), DIAS (Диасофт)

Транспорт:
FLOT (Совкомфлот, танкеры), AFLT (Аэрофлот)

Удобрения и химия:
AKRN (Акрон), PHOR (ФосАгро)

Ритейл, агро, прочее:
MGNT (Магнит), X5 (X5, Пятёрочка, Перекрёсток), FIXR (Fix Price),
RAGR (РусАгро), MOEX (Мосбиржа, Московская биржа),
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
- IPO-активность без конкретной компании → []
- Политика, военные новости, СВО, геополитика без упоминания компании → []
- Максимум 3 тикера

КРИТИЧЕСКИ ВАЖНО — ЗАПРЕЩЁННЫЕ ОШИБКИ:
❌ МГКЛ → НЕ MTSS, НЕ FEES, НЕ AFLT. МГКЛ = MGKL (Мосгорломбард)
❌ МТС-Банк → НЕ MTSS. МТС-Банк = MBNK
❌ Озон Фармацевтика → НЕ OZON. Озон Фармацевтика = OZPH
❌ AutoZone (американская компания, тикер AZO) → [] (не торгуется на MOEX)
❌ Иностранная компания (Goldman Sachs, Evercore, Apple, FalconX, Coinbase, Binance...) → []
❌ Крипто-компании и крипто-биржи (FalconX, Coinbase, Bybit, Kraken, OKX...) → []
❌ «Озоновый слой», «слой озона», «озоновая дыра» — природное явление, НЕ компания → []
❌ НМТП → НЕ VTBR. НМТП = NMTP
❌ Русснефть → НЕ LKOH, НЕ ROSN. Русснефть = RNFT
❌ Мосэнерго → НЕ MSRS. Мосэнерго = MSNG
❌ IPO/SPO без конкретной компании → НЕ SBER, НЕ MOEX. Верни []
❌ Если компания есть в новости, но её тикера НЕТ в списке выше → обязательно []
❌ Не угадывай тикер похожей компании — лучше вернуть [], чем ошибиться

---

ПРАВИЛА
- никакого английского в тексте ответа
- стиль простой, человеческий, без воды
- без советов, сигналов и призывов к действию
- ТОЧНОСТЬ ВРЕМЕННЫХ ССЫЛОК: если в заголовке сравниваются два года (например «В 2026Г ... К 2025Г»), это значит «в 2026 году относительно 2025 года» — НЕ «план на 2025 год». Не переформулируй сравнение прошлых периодов как будущую цель. Пример правильно: «Сбербанк в 2026 году планирует нарастить прибыль по сравнению с 2025 годом». Пример НЕПРАВИЛЬНО: «Сбербанк планирует рост прибыли к 2025 году» (звучит как старый прогноз).

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
    "SBER", "VTBR", "T", "CBOM", "BSPB", "AFKS", "SVCB", "SPBE", "RENI", "MBNK",
    "GMKN", "CHMF", "NLMK", "MAGN", "PLZL", "ALRS", "MTLR", "SELG", "RUAL", "RASP",
    "IRAO", "HYDR", "FEES", "MSNG", "MSRS", "MRKV", "MRKU", "MRKP", "MRKC",
    "YDEX", "MTSS", "RTKM", "VKCO", "POSI", "HEAD", "OZON", "DIAS",
    "FLOT", "AFLT",
    "AKRN", "PHOR",
    "MGNT", "X5", "FIXR", "RAGR", "MOEX", "SGZH", "MGKL", "OZPH",
    "SMLT", "PIKK", "LSRG", "ETLN",
})


@dataclass(frozen=True)
class AIAnalysis:
    summary:     str
    what_behind: str
    watch_for:   str
    tickers:     list[str]
    sentiment:   Optional[str] = None  # "positive" | "negative" | None


async def analyze(title: str, text: str = "", recent_context: list[str] = []) -> Optional[AIAnalysis]:
    """
    Analyze a publishable news headline via OpenRouter free tier.
    Returns AIAnalysis on success, None on any failure.
    """
    if not settings.openrouter_api_key:
        return None

    today = date.today()
    today_str = today.strftime("%d.%m.%Y")
    current_year = today.year
    system_content = _SYSTEM_PROMPT.replace(
        "ТОЧНОСТЬ ВРЕМЕННЫХ ССЫЛОК:",
        f"ТОЧНОСТЬ ВРЕМЕННЫХ ССЫЛОК (сейчас {current_year} год, 2025 — прошлый год):",
    )
    user_content = "Сегодня " + today_str + ".\n\n" + _USER_TEMPLATE.format(title=title, text=text or title)
    if recent_context:
        events_block = "\n".join(f"• {t}" for t in recent_context[:5])
        user_content += f"\n\nПоследние события по этим тикерам:\n{events_block}"

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": system_content},
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

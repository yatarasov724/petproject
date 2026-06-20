"""
Keyword-based scoring for stock-market-relevant news.

Design
──────
Score is computed from the article TITLE ONLY (not content).
Reasons:
  - RSS content fields contain boilerplate from the source site (nav menus, footers)
  - Title is the most concentrated signal; content rarely adds unique keywords
  - Simpler, faster, easier to tune

Signal categories
─────────────────
  GEOPOLITICS   — sanctions, war escalation, OPEC, trade restrictions, embargoes
  CORPORATE     — IPO, SPO, buyback, dividends, M&A, earnings, guidance, restructuring
  MACRO         — CB rate decision, inflation, GDP, budget, macro data releases
  REGULATION    — sector regulatory decisions, FAS, licenses, government mandates

Score breakdown
───────────────
  base_score      — highest tier matched (50 / 25 / 10 / 0)
  keyword_bonus   — additional keyword hits beyond the first (+5 each, capped at +15)
  source_modifier — multi-source confirmation (+10 for 2 sources, +20 for 3+)
  type_modifier   — event type premium (SANCTIONS +15, WAR +15, etc.)

Publish threshold: 35
  Tier1 alone (50) → always publishes
  Tier2 alone (25) → just below threshold; needs type_modifier (+15→40) or 2nd source (+10→35)
  Tier3 + 3 sources (10+20=30) → still below; needs keyword_bonus too (10+20+5=35)
  Noise (0)        → never publishes

What passes:
  - Any news about MOEX-listed companies (dividends, earnings, M&A, IPO, buyback)
  - Macro events affecting markets (rate decisions, inflation, budget)
  - Geopolitical events with direct market consequences (sanctions, OPEC, trade)
  - Regulatory changes affecting sectors

What doesn't pass:
  - Local city news, social events, sports, culture
  - PR news without market angle
  - Anything matching NOISE_PATTERNS
"""

import re
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# ОПЕК (oil cartel) must not match "опека/опекой" (care/custody in Russian).
#  is a Unicode word boundary: 'опек' followed by Cyrillic letter won't match.
_OPEC_RU_RE = re.compile(r'\bопек\b', re.UNICODE)


def _kw_match(kw: str, text: str) -> bool:
    """Match keyword in lowercased text, with special word-boundary check for 'опек'."""
    if kw not in text:
        return False
    if kw == "опек":
        return bool(_OPEC_RU_RE.search(text))
    return True

# ── thresholds ────────────────────────────────────────────────────────────────

PUBLISH_THRESHOLD = 35   # cluster must reach this to be sent to Telegram
ARTICLE_MIN_SCORE = 10   # article must score at least this to enter the pipeline at all

# ── tier base scores ──────────────────────────────────────────────────────────

_TIER1_BASE = 50
_TIER2_BASE = 25
_TIER3_BASE = 10
_KEYWORD_BONUS = 5       # each additional keyword match after the first
_MAX_KEYWORD_BONUS = 15  # cap on keyword bonus


# ── tier 1: critical events ───────────────────────────────────────────────────
# Events that directly and immediately affect price discovery.

TIER1_KEYWORDS: list[str] = [
    # ── sanctions ──
    # stem "санкци" catches: санкции, санкций, санкциях, санкционный, санкционных
    "санкци",
    "sdn list",
    "заморозка активов",    # точная фраза — исключает погодные "заморозки"
    "заморозки активов",
    "заморозку активов",
    "заморозке активов",
    "заморозка счетов",
    "заморозки счетов",
    "заморозка средств",
    "блокировка активов",
    "блокировки активов",
    "блокировку активов",
    "блокировке активов",
    "блокировка счетов",
    "блокировку счетов",
    "блокировке счетов",
    "блокировка средств",
    "блокировку средств",
    "блокировка торгов",
    "заблокировали активы",
    "заблокировали счета",
    "заблокировали торги",
    "запрет на экспорт",
    "эмбарго",
    "embargo",
    "ценовой потолок",
    "price cap",
    "отключение swift",
    "swift",

    # ── OPEC / oil production ──
    "опек",                # опек, опек+, опека (любой контекст с упоминанием ОПЕК)
    "opec",
    "сокращение добычи",
    "квота на добыч",

    # ── war / military escalation ──
    "мобилизаци",          # мобилизация, мобилизации, мобилизацию
    "объявлена война",
    "объявил войну",
    "чрезвычайное положение",
    "военное положение",
    # "ядерн" перенесено в Tier2 — риторика про ядерное оружие слишком частая,
    # реальная эскалация покрывается конкретными фразами ниже
    "ядерный удар",
    "применение ядерного",
    "ядерное оружие примен",
    "ядерная атак",
    "наступление на",

    # ── market structure ──
    "остановка торгов",
    "приостановка торгов",
    "приостановлен",        # торги приостановлены
    "мосбиржа приостановил",
    "биржа закрыта",
    "делистинг",
    "принудительный выкуп",

    # ── sovereign / credit crisis ──
    "дефолт",              # invariable
    "невыплат",            # невыплата, невыплаты долга

    # ── nationalization / confiscation ──
    "национализаци",       # национализация, национализации
    "принудительная продажа",
    "принудительной продаж",
    "изъятие активов",
    "изъятию активов",
    "конфискац",           # конфискация, конфискации

    # ── emergency CB / capital controls ──
    "экстренное заседание",
    "внеплановое заседание",
    "валютные ограничени",  # ограничения, ограничению
    "ограничения на вывод",
    "вывод капитала",
    "девальваци",          # девальвация, девальвации, девальвацию

    # ── central bank rate decisions (Tier1 — наиболее значимое внутреннее событие) ──
    # Решения ЦБ по ставке влияют на весь рынок мгновенно — как санкции.
    "ключевую ставку",
    "ключевой ставке",
    "ключевой ставки",
    "ключевая ставка",
    "повысил ставку",
    "снизил ставку",
    "сохранил ставку",
    "поднял ставку",
    "повышение ставки",
    "снижение ставки",
    "изменение ставки",
    "заседание цб",
    "решение по ставке",
    "решении по ставке",
    "ставка рефинансирован",
    "денежно-кредитн",
]


# ── tier 2: significant events ────────────────────────────────────────────────
# Events with clear, near-term market impact.

TIER2_KEYWORDS: list[str] = [
    # ── central bank / board meetings (не ставка — она в Tier1) ──
    "заседание банка",
    "заседание совета директоров",
    "заседание правления",
    "заседание набсовета",

    # ── earnings & financials ──
    "чистая прибыль",
    "чистой прибыл",       # прибыли, прибыль (genitive)
    "чистый убыток",
    "чистого убытк",       # убытка
    "выручк",              # выручка, выручки, выручку — high-frequency stem
    "ebitda",
    "финансовые результаты",
    "финансовых результат",
    "финансовой отчётност",
    "рекордная прибыль",
    "рекордный дивиденд",
    "рекордные дивиденд",
    "рекордная выручк",
    "рекордный убыток",
    "рекордные продажи",
    "рекордный объём добыч",
    # "рекордн" убран — матчил "рекордный снегопад", "рекордный урожай" и т.п.
    "прибыль выросла",
    "прибыль снизилась",
    "прибыль увеличил",
    "прибыль сократил",
    "убыток вырос",
    "нарастил прибыл",     # нарастил прибыль/прибыли
    "нарастила прибыл",
    "увеличил прибыл",
    "увеличила прибыл",
    "снизил прибыл",
    "снизила прибыл",
    "сократил прибыл",
    "сократила прибыл",
    "операционную прибыл", # операционную прибыль
    "операционные результаты",
    "производственные результаты",
    "отчётност",           # отчётность, отчётности
    "годовой отчёт",
    "квартальный отчёт",
    "msfo",
    "мсфо",
    "рсбу",
    "отчитал",             # отчитался, отчиталась по мсфо

    # ── guidance ──
    "прогноз компани",     # компании, компанией
    "прогноз менеджмент",
    "пересмотрел прогноз",
    "повысил прогноз",
    "снизил прогноз",
    "guidance",
    "ориентир по прибыл",
    "ориентир по выручк",

    # ── dividends ──
    # stem "дивиденд" catches all forms: дивиденды, дивидендов, дивидендам, дивидендная
    "дивиденд",
    "рекомендация совета директоров",
    "рекомендовал дивиденд",

    # ── buyback / SPO / capital ──
    "buyback",
    "обратный выкуп",
    "обратного выкупа",
    "spo",
    "вторичное размещение",
    "доп. эмисси",         # доп. эмиссия, эмиссии
    "допэмисси",
    "дополнительная эмисси",
    "размещение новых акций",

    # ── commodities ──
    "цена нефти",
    "цены на нефть",
    "нефть подорожал",
    "нефть упал",
    "нефть дорожает",
    "нефть дешевеет",
    "стоимость нефти",
    "brent",
    "urals",
    "цена газа",
    "цены на газ",
    "стоимость газа",
    "цена угля",
    "цены на уголь",
    "золото подорожало",
    "золото подешевело",
    "стоимость золот",
    "никель",
    "медь",
    "алюминий",
    "цена удобрен",
    "зерновая сделка",

    # ── M&A ──
    "слияние",
    "поглощени",           # поглощение, поглощении
    "приобрела компани",   # приобрела компания (M&A контекст)
    "приобрел компани",
    "приобрела долю",
    "приобрел долю",
    "приобретение актив",  # приобретение активов/актива
    "приобрела акци",      # приобрела акции
    "приобрел акци",
    # "приобрет" убран — матчил "МО приобрело вертолёты", "музей приобрёл картину"
    "купила долю",
    "купил долю",
    "продала актив",
    "продал актив",
    "продала долю",
    "продал долю",
    "сделка m&a",
    "покупка бизнеса",

    # ── analyst ratings ──
    "целевую цену",          # повысили целевую цену акций
    "целевая цена",
    "таргет по акци",        # таргет по акциям
    "рейтинг акци",          # рейтинг акций повышен
    "рекомендацию покупат",  # рекомендацию покупать
    "повысил рейтинг",
    "снизил рейтинг",

    # ── IPO / listing ──
    "ipo",
    "листинг",
    "размещение акций",
    "выход на биржу",
    "первичное размещение",
    "выйдет на биржу",
    "выходит на биржу",

    # ── trade / supply ──
    "экспортн",            # экспортные пошлины, экспортных пошлин
    "импортн",             # импортные пошлины
    "пошлин",              # пошлины, пошлину, пошлин
    "торговые ограничени",
    "торговых ограничени",
    "цепочки поставок",

    # ── nuclear / escalation (Tier2, не Tier1 — риторика слишком частая) ──
    # Реальная ядерная эскалация покрыта в Tier1 точными фразами выше.
    # Здесь: упоминание ядерного оружия в политическом/переговорном контексте.
    "ядерн",               # ядерный, ядерного, ядерную — Tier2: требует 2-го источника
]

# Список мажорных компаний ММВБ, перенесённых из Tier2.
# Упоминание компании без рыночного действия (дивиденды, M&A и т.д.) само по себе
# недостаточно для публикации — компания должна идти вместе с другим ключевым словом.
_MOEX_COMPANY_KEYWORDS: list[str] = [
    "газпром", "лукойл", "роснефть", "новатэк", "сбербанк", "сбер", "полюс",
    "норникел",            # норникель/норникеля/норникелю (все падежи)
    "магнит", "яндекс", "х5 group", "x5 group",
    "северстал",           # северсталь/северстали (все падежи)
    "нлмк", "алроса", "русагро", "фосагро",
    "мосбирж",             # мосбиржа/мосбиржи/мосбирже (все падежи)
    "московская биржа",
    "аэрофлот", "сегежа", "пик груп", "группа пик", "en+", "русал", "мтс",
    "озон", "ozon", "fix price", "тинькофф", "т-банк", "вк груп",
    "банк санкт-петербург", "транснефть", "татнефть", "башнефть",
    "интер рао", "русгидро", "ммк",
    # ── дополнительные компании ММВБ ──
    "ростелеком",          # RTKM
    "втб",                 # VTBR
    "мечел",               # MTLR
    "сургутнефтегаз",      # SNGS
    "совкомбанк",          # SVCB
    "группа самолёт",      # SMLT (не "самолёт" — слишком общее)
    "позитив технолоджиз", # POSI (не "позитив" — ложные срабатывания)
    "акрон",               # AKRN
    "делимобиль",          # DELI
    "хэндерсон",           # HNFG
]


# ── tier 3: background events ─────────────────────────────────────────────────
# Events with potential but less direct market impact.

TIER3_KEYWORDS: list[str] = [
    # ── macro data ──
    "ввп",
    "инфляци",             # инфляция, инфляции, инфляцию
    "безработиц",          # безработица, безработицы
    "промышленное производство",
    "торговый баланс",
    "платёжный баланс",
    "индекс цен",
    "потребительские цены",
    "потребительских цен",
    "росстат",
    "pmi",
    "деловая активность",
    "деловой активности",

    # ── budget / fiscal ──
    "федеральный бюджет",
    "федерального бюджет",
    "дефицит бюджет",      # дефицит бюджета, бюджетный дефицит
    "профицит бюджет",
    "налоговые поступлени",
    "минфин",
    "фнб",
    "налоговые льгот",
    "изменение налог",
    "налог на прибыл",

    # ── regulatory ──
    "фас",
    "антимонопольн",
    "лицензи",             # лицензия, лицензии, лицензирование
    "штраф",               # invariable
    "предписани",          # предписание, предписании
    "регулятор",           # invariable
    "новые требовани",
    "новые правила",
    "законопроект",
    "закон вступил в силу",

    # ── geopolitical background ──
    "переговор",           # переговоры, переговорах, переговоров
    "саммит",
    "встреча лидеров",
    "дипломатическ",
    "прекращение огня",
    "ceasefire",
    "мирные переговор",

    # ── corporate events ──
    "совет директоров",
    "совета директоров",
    "годовое собрание",
    "внеочередное собрание",
    "смена генерального директора",
    "новый ceo",
    "назначен председател",
    "реорганизаци",
    "банкротств",          # банкротство, банкротства, банкротстве
    "ликвидаци",

    # ── sector/macro trends ──
    "ставка по ипотек",
    "ипотечный рынок",
    "рынок труда",
    "потребительский спрос",
    "кредитовани",
    "банковский сектор",
    "нефтяной сектор",
    "газовый сектор",

    # ── market / trading ──
    "фондовый рынок",
    "фондового рынка",
    "биржевые торги",
    "торги на мосбирже",
    "индекс мосбиржи",
    "индекс ртс",
    "акции выросли",
    "акции упали",
    "акции подорожали",
    "акции подешевели",
    "рынок акций",

    # ── major MOEX companies (перенесено из Tier2) ──
    # Упоминание компании даёт +10 — само по себе ниже порога публикации.
    # Чтобы опубликоваться, нужно хотя бы одно Tier1/Tier2 ключевое слово рядом.
    *_MOEX_COMPANY_KEYWORDS,
]


# ── noise patterns ────────────────────────────────────────────────────────────
# Headlines matching these are almost certainly irrelevant to markets.
# Checked first — if match found, score = 0 immediately.

NOISE_PATTERNS: list[str] = [
    # culture & entertainment
    "фестиваль",
    "кинофестиваль",
    "театр",
    "концерт",
    "выставка",
    "музей",
    "литература",
    "поэзия",
    "кино",
    "сериал",
    "телешоу",

    # sports
    "чемпионат",
    "футбол",
    "хоккей",
    "олимпиада",
    "теннис",
    "баскетбол",
    "кубок мира",
    "чемпион мира",
    "лига чемпионов",
    "гонки формулы",

    # weather / nature (unless supply chain context)
    "погода",
    "ураган",
    "землетрясение",
    "наводнение",
    "пожар в лесу",
    "заморозки в",          # погодные заморозки ("заморозки в регионе")
    "снег и заморозк",      # прямая формулировка прогноза погоды
    "морозы ожидаются",
    "похолодание",

    # social / celebrity
    "звезда",
    "певец",
    "актёр",
    "блогер",
    "инфлюенсер",
    "рэпер",
    "шоумен",

    # purely local politics without market angle
    "мэр города",
    "губернатор назначил",
    "праздник",
    "юбилей",
    "день города",
    "день рождения",
    "открытие памятника",
    "открыли парк",

    # crime & accidents (without market angle)
    "убийств",              # убийство, убийства
    "ограблени",            # ограбление банка → исключение, но редко в финансовых лентах
    "дтп",
    "авиакатастроф",
    "крушение поезда",
    "столкновение автомобил",

    # health / medicine (without market angle)
    "заболеваемость",
    "вакцинаци",            # вакцинация, вакцины (без биотех-контекста)
    "вспышка гриппа",
    "вспышка кори",
    "эпидемия гриппа",

    # elections / voting (no direct market trigger)
    "явка на выборах",
    "итоги голосования",
    "результаты выборов",   # региональных/муниципальных

    # non-market corporate events (часто проскакивают через компании Tier3)
    "меняет название",
    "сменил название",
    "сменила название",
    "сменило название",
    "переименован",
    "переименована",
    "ответил на жалобу",
    "ответила на жалобу",
    "освободили из тюрьмы",
    "вышел из тюрьмы",
    "вышла из тюрьмы",
    "задержан полицией",
    "арестован по",
]


# ── event types ───────────────────────────────────────────────────────────────

class EventType(str, Enum):
    SANCTIONS        = "SANCTIONS"
    WAR_ESCALATION   = "WAR_ESCALATION"
    DEFAULT          = "DEFAULT"
    NATIONALIZATION  = "NATIONALIZATION"
    RATE_DECISION    = "RATE_DECISION"
    EARNINGS         = "EARNINGS"
    DIVIDENDS        = "DIVIDENDS"
    COMMODITY_SHOCK  = "COMMODITY_SHOCK"
    OPEC             = "OPEC"
    M_AND_A          = "M_AND_A"
    IPO              = "IPO"
    SPO_BUYBACK      = "SPO_BUYBACK"
    MACRO_DATA       = "MACRO_DATA"
    TRADE            = "TRADE"
    REGULATION       = "REGULATION"
    ANALYST_RATING   = "ANALYST_RATING"
    CORPORATE        = "CORPORATE"
    GEOPOLITICAL     = "GEOPOLITICAL"
    NOISE            = "NOISE"
    UNKNOWN          = "UNKNOWN"


# Extra points per event type on top of base tier score.
_TYPE_MODIFIERS: dict[EventType, int] = {
    EventType.SANCTIONS:       15,   # direct asset impact
    EventType.WAR_ESCALATION:  15,   # macro risk spike
    EventType.DEFAULT:         15,   # credit event
    EventType.NATIONALIZATION: 15,   # ownership shock
    EventType.OPEC:            10,   # direct commodity price impact
    EventType.COMMODITY_SHOCK: 10,   # supply/demand immediate
    EventType.RATE_DECISION:    5,   # priced-in but significant
    EventType.DIVIDENDS:        5,   # direct equity value
    EventType.M_AND_A:          5,
    EventType.IPO:             10,   # primary offering — high-certainty market event
    EventType.SPO_BUYBACK:      5,   # capital structure event
    EventType.TRADE:            5,
    EventType.EARNINGS:         5,   # company + earnings = 25+5+5(company kw) = 35
    EventType.MACRO_DATA:       0,
    EventType.ANALYST_RATING:   5,   # price target + company = 25+5+5 = 35
    EventType.REGULATION:       0,
    EventType.CORPORATE:        0,
    EventType.GEOPOLITICAL:     0,
    EventType.UNKNOWN:          0,
    EventType.NOISE:          -50,   # ensure noise never reaches threshold
}

# Keyword sets for event classification (priority-ordered).
# These use stems (partial strings) so they match all Russian inflected forms.
_TYPE_SIGNALS: list[tuple[EventType, list[str]]] = [
    (EventType.SANCTIONS,      ["санкци", "sdn", "эмбарго", "embargo", "price cap", "ценовой потолок", "swift", "заморозка активов", "заморозки активов", "заблокир"]),
    (EventType.WAR_ESCALATION, ["мобилизаци", "военное положение", "чрезвычайное положение",
                                 "ядерный удар", "ядерная атак", "применение ядерного", "наступление"]),
    (EventType.DEFAULT,        ["дефолт", "невыплат"]),
    (EventType.NATIONALIZATION,["национализаци", "принудительная продажа", "изъятие", "конфискац"]),
    (EventType.OPEC,           ["опек", "opec", "сокращение добычи"]),
    (EventType.RATE_DECISION,  ["ключевую ставку", "ключевой ставке", "ключевой ставки", "ключевая ставка",
                                 "повысил ставку", "снизил ставку", "сохранил ставку", "поднял ставку",
                                 "повышение ставки", "снижение ставки", "решение по ставке",
                                 "заседание цб", "заседание банка"]),
    (EventType.COMMODITY_SHOCK,["нефть подорожал", "нефть упал", "нефть дорожает", "нефть дешевеет",
                                 "цена нефти", "цены на нефть", "brent", "urals",
                                 "цена газа", "цены на газ", "цена угля"]),
    (EventType.DIVIDENDS,      ["дивиденд"]),
    (EventType.EARNINGS,       ["чистая прибыль", "чистой прибыл", "чистый убыток", "выручк", "ebitda",
                                 "финансовые результаты", "финансовых результат", "мсфо", "msfo", "рсбу",
                                 "отчётност", "отчитал", "производственные результаты", "операционные результаты",
                                 "нарастил прибыл", "нарастила прибыл", "увеличил прибыл", "увеличила прибыл",
                                 "снизил прибыл", "снизила прибыл", "сократил прибыл", "сократила прибыл",
                                 "операционную прибыл"]),
    (EventType.SPO_BUYBACK,    ["spo", "вторичное размещение", "обратный выкуп", "обратного выкупа",
                                 "buyback", "допэмисси", "дополнительная эмисси", "доп. эмисси"]),
    (EventType.M_AND_A,        ["слияние", "поглощени", "приобрела долю", "приобрел долю",
                                 "приобретение актив", "купила долю", "купил долю",
                                 "продала актив", "продал актив", "сделка m&a", "покупка бизнеса"]),
    (EventType.IPO,            ["ipo", "листинг", "размещение акций", "выход на биржу",
                                 "первичное размещение", "выйдет на биржу"]),
    (EventType.TRADE,          ["пошлин", "торговые ограничени", "зерновая сделка"]),
    (EventType.REGULATION,     ["фас", "антимонопольн", "лицензи", "предписани",
                                 "законопроект", "закон вступил"]),
    (EventType.MACRO_DATA,     ["ввп", "инфляци", "безработиц", "росстат", "pmi",
                                 "дефицит бюджет", "профицит бюджет", "минфин"]),
    (EventType.ANALYST_RATING, ["целевую цену", "целевая цена", "таргет по акци",
                                 "рейтинг акци", "рекомендацию покупат",
                                 "повысил рейтинг", "снизил рейтинг"]),
    (EventType.CORPORATE,      ["совет директоров", "годовое собрание", "новый ceo", "guidance"]),
    (EventType.GEOPOLITICAL,   ["переговор", "саммит", "дипломатическ", "прекращение огня", "ceasefire", "ядерн"]),
]


# ── result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScoreResult:
    score:          int
    tier:           str        # "tier1" | "tier2" | "tier3" | "noise"
    event_type:     EventType
    base_score:     int
    keyword_bonus:  int
    source_bonus:   int
    type_bonus:     int
    matched_keywords: list[str]


# ── public API ────────────────────────────────────────────────────────────────

def compute_score(title: str, source_count: int = 1) -> ScoreResult:
    """
    Score a news title.
    Only the title is used — content is deliberately ignored (see module docstring).
    source_count reflects how many distinct sources reported this cluster so far.
    """
    text = title.lower()

    # ── noise check (fast path) ───────────────────────────────────────────
    for pattern in NOISE_PATTERNS:
        if pattern in text:
            return ScoreResult(
                score=0, tier="noise",
                event_type=EventType.NOISE,
                base_score=0, keyword_bonus=0, source_bonus=0, type_bonus=0,
                matched_keywords=[pattern],
            )

    # ── keyword scanning ──────────────────────────────────────────────────
    all_hits: list[tuple[str, int]] = []   # (keyword, tier_score)

    for kw in TIER1_KEYWORDS:
        if _kw_match(kw, text):
            all_hits.append((kw, _TIER1_BASE))

    for kw in TIER2_KEYWORDS:
        if _kw_match(kw, text):
            all_hits.append((kw, _TIER2_BASE))

    for kw in TIER3_KEYWORDS:
        if _kw_match(kw, text):
            all_hits.append((kw, _TIER3_BASE))

    if not all_hits:
        return ScoreResult(
            score=0, tier="noise",
            event_type=EventType.UNKNOWN,
            base_score=0, keyword_bonus=0, source_bonus=0, type_bonus=0,
            matched_keywords=[],
        )

    # Best single hit determines the tier
    all_hits.sort(key=lambda x: -x[1])
    base_score = all_hits[0][1]
    matched_keywords = [h[0] for h in all_hits]

    tier = (
        "tier1" if base_score >= _TIER1_BASE else
        "tier2" if base_score >= _TIER2_BASE else
        "tier3"
    )

    # Keyword bonus: each additional hit (any tier) adds _KEYWORD_BONUS
    keyword_bonus = min(
        (len(all_hits) - 1) * _KEYWORD_BONUS,
        _MAX_KEYWORD_BONUS,
    )

    # Source count modifier
    source_bonus = 20 if source_count >= 3 else (10 if source_count >= 2 else 0)

    # Event type
    event_type  = classify_event_type(text)
    type_bonus  = _TYPE_MODIFIERS.get(event_type, 0)

    total = base_score + keyword_bonus + source_bonus + type_bonus
    total = max(0, min(total, 100))   # clamp [0, 100]

    return ScoreResult(
        score=total,
        tier=tier,
        event_type=event_type,
        base_score=base_score,
        keyword_bonus=keyword_bonus,
        source_bonus=source_bonus,
        type_bonus=type_bonus,
        matched_keywords=matched_keywords,
    )


def classify_event_type(text: str) -> EventType:
    """
    Classify the event type by looking for type-specific signals in text.
    Returns the first (highest priority) match from _TYPE_SIGNALS.
    """
    for event_type, signals in _TYPE_SIGNALS:
        for signal in signals:
            if _kw_match(signal, text):
                return event_type
    return EventType.UNKNOWN


def is_publishable(score: int) -> bool:
    return score >= PUBLISH_THRESHOLD

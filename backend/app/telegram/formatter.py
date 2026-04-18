"""
Telegram message formatter.

Message format (MarkdownV2):
─────────────────────────────
  *BADGE*

  Заголовок события

  Влияет на: сектор · актив · актив
  [Читать →](url)

Design decisions:
  - No raw score (35/100) — not meaningful to end user
  - No source count — internal implementation detail
  - No source name in meta — visible via the link
  - "Влияет на:" line answers: what market segments does this touch?
  - Badge answers: what category of event is this?
  - UPDATE events get ↻ prefix on the badge
"""

import sqlite3
from app.pipeline.scorer import EventType, ScoreResult
from app.pipeline.publish_decision import Decision

# ── badge labels ──────────────────────────────────────────────────────────────
# Short, uppercase category label shown as the first line of each message.

_BADGE: dict[EventType, str] = {
    EventType.SANCTIONS:        "САНКЦИИ",
    EventType.WAR_ESCALATION:   "ЭСКАЛАЦИЯ",
    EventType.DEFAULT:          "ДЕФОЛТ",
    EventType.NATIONALIZATION:  "НАЦИОНАЛИЗАЦИЯ",
    EventType.RATE_DECISION:    "СТАВКА ЦБ",
    EventType.EARNINGS:         "ОТЧЁТНОСТЬ",
    EventType.DIVIDENDS:        "ДИВИДЕНДЫ",
    EventType.COMMODITY_SHOCK:  "СЫРЬЁ",
    EventType.OPEC:             "ОПЕК",
    EventType.M_AND_A:          "СДЕЛКА M&A",
    EventType.IPO:              "IPO",
    EventType.SPO_BUYBACK:      "SPO / ВЫКУП",
    EventType.MACRO_DATA:       "МАКРО",
    EventType.TRADE:            "ТОРГОВЛЯ",
    EventType.REGULATION:       "РЕГУЛЯТОРИКА",
    EventType.CORPORATE:        "КОРПОРАТИВ",
    EventType.GEOPOLITICAL:     "ГЕОПОЛИТИКА",
    EventType.UNKNOWN:          "РЫНКИ",
    EventType.NOISE:            "РЫНКИ",
}

# ── "Влияет на:" lines ────────────────────────────────────────────────────────
# Describes which market segments are affected. Shown to the reader as context.
# These are static per event type — good enough for MVP without LLM.

_AFFECTS: dict[EventType, str] = {
    EventType.SANCTIONS:        "акции · рубль · ОФЗ · commodities",
    EventType.WAR_ESCALATION:   "акции · рубль · commodities · риск-сентимент",
    EventType.DEFAULT:          "ОФЗ · рубль · акции · кредитный риск",
    EventType.NATIONALIZATION:  "акции · equity risk · сектор",
    EventType.RATE_DECISION:    "облигации · акции · рубль · ипотека",
    EventType.EARNINGS:         "акции компании · мультипликаторы · сектор",
    EventType.DIVIDENDS:        "акции компании · дивдоходность · реестр",
    EventType.COMMODITY_SHOCK:  "нефтяной сектор · металлы · энергетика · commodities",
    EventType.OPEC:             "нефть · акции нефтяников · рубль · commodities",
    EventType.M_AND_A:          "акции участников · сектор · оценки",
    EventType.IPO:              "новый эмитент · ликвидность · индексы MOEX",
    EventType.SPO_BUYBACK:      "акции компании · free float · давление на цену",
    EventType.MACRO_DATA:       "ОФЗ · рубль · ставки · ожидания рынка",
    EventType.TRADE:            "акции экспортёров · commodities · пошлины · рубль",
    EventType.REGULATION:       "сектор · акции затронутых компаний · compliance",
    EventType.CORPORATE:        "акции компании · управление · сектор",
    EventType.GEOPOLITICAL:     "риск-сентимент · акции · рубль · ОФЗ",
    EventType.UNKNOWN:          "рынки",
    EventType.NOISE:            "рынки",
}


def format_message(
    cluster: sqlite3.Row,
    score_result: ScoreResult,
    decision: Decision,
    article_url: str = "",
    source_name: str = "",
) -> str:
    """
    Returns a MarkdownV2-formatted Telegram message.
    All dynamic fields are passed through _esc() exactly once before assembly.
    """
    badge   = _BADGE.get(score_result.event_type, "РЫНКИ")
    affects = _AFFECTS.get(score_result.event_type, "рынки")

    if decision == Decision.UPDATE:
        badge = f"↻ {badge}"

    title = cluster["canonical_title"]

    parts = [
        f"*{_esc(badge)}*",
        "",
        _esc(title),
    ]

    return "\n".join(parts)


def _esc(text: str) -> str:
    """
    Escape MarkdownV2 special characters.
    Apply to each raw string exactly once — never to an already-escaped string.
    See: https://core.telegram.org/bots/api#markdownv2-style
    """
    special = r"\_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text

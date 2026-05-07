"""
Telegram message formatter.

Message format with AI analysis (MarkdownV2):
─────────────────────────────────────────────
  🔴 *СТАВКА ЦБ*

  ЦБ повысил ключевую ставку до 21 процента

  _Что произошло:_ Банк России поднял ставку с 19% до 21%
  _Для рынка:_ давление на акции и облигации, укрепление рубля

  Влияет на: облигации · акции · рубль · ипотека
  [Читать →](url)

Fallback (no AI):
─────────────────
  *СТАВКА ЦБ*

  ЦБ повысил ключевую ставку до 21 процента

  Влияет на: облигации · акции · рубль · ипотека
  [Читать →](url)

Design decisions:
  - No raw score — not meaningful to end user
  - No source count — internal implementation detail
  - "Влияет на:" is static per event type (reliable without LLM)
  - UPDATE events get ↻ prefix on the badge
  - AI analysis uses emoji prefix + AI-normalized title + summary + market_effect
"""

import sqlite3
from typing import Optional

from app.ai.analyzer import AIAnalysis
from app.pipeline.scorer import EventType, ScoreResult
from app.pipeline.publish_decision import Decision

# ── badge labels ──────────────────────────────────────────────────────────────

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

_AFFECTS: dict[EventType, str] = {
    EventType.SANCTIONS:        "акции · рубль · ОФЗ · сырьё",
    EventType.WAR_ESCALATION:   "акции · рубль · сырьё",
    EventType.DEFAULT:          "ОФЗ · рубль · акции",
    EventType.NATIONALIZATION:  "акции · сектор",
    EventType.RATE_DECISION:    "ОФЗ · акции · рубль",
    EventType.EARNINGS:         "акции",
    EventType.DIVIDENDS:        "акции",
    EventType.COMMODITY_SHOCK:  "нефть · сырьё · акции",
    EventType.OPEC:             "нефть · акции · рубль",
    EventType.M_AND_A:          "акции",
    EventType.IPO:              "акции",
    EventType.SPO_BUYBACK:      "акции",
    EventType.MACRO_DATA:       "ОФЗ · рубль · акции",
    EventType.TRADE:            "акции · сырьё · рубль",
    EventType.REGULATION:       "акции · сектор",
    EventType.CORPORATE:        "акции",
    EventType.GEOPOLITICAL:     "акции · рубль · ОФЗ",
    EventType.UNKNOWN:          "акции",
    EventType.NOISE:            "акции",
}


def format_message(
    cluster: sqlite3.Row,
    score_result: ScoreResult,
    decision: Decision,
    ai_analysis: Optional[AIAnalysis] = None,
) -> str:
    """
    Returns a MarkdownV2-formatted Telegram message.
    All dynamic fields pass through _esc() exactly once before assembly.

    With AI analysis:
      🟢 *ЗАГОЛОВОК*
      Дескрипшн

      _Для рынка:_ эффект

      Влияет на: акции · рубль · ОФЗ

    Fallback (no AI):
      *↻ BADGE* (UPDATE) or *BADGE*

      Заголовок

      Влияет на: акции · рубль · ОФЗ
    """
    ticker_line = _format_tickers(cluster["tickers"])

    if ai_analysis:
        prefix     = "↻ " if decision == Decision.UPDATE else ""
        title_line = f"{ai_analysis.emoji} *{_esc(prefix + ai_analysis.title)}*"
        parts = [
            title_line,
            "",
            f"_Для рынка:_ {_esc(ai_analysis.market_effect)}",
        ]
        if ticker_line:
            parts += ["", ticker_line]
        elif ai_analysis.affects:
            parts += ["", f"Влияет на: {_esc(ai_analysis.affects)}"]
    else:
        badge = _BADGE.get(score_result.event_type, "РЫНКИ")
        if decision == Decision.UPDATE:
            badge = f"↻ {badge}"
        parts = [
            f"*{_esc(badge)}*",
            "",
            _esc(cluster["canonical_title"]),
        ]
        if ticker_line:
            parts += ["", ticker_line]
        else:
            affects = _AFFECTS.get(score_result.event_type, "акции")
            parts += ["", f"Влияет на: {_esc(affects)}"]

    return "\n".join(parts)


def _format_tickers(tickers: Optional[str]) -> str:
    """Format comma-separated tickers as '$GAZP · $SBER', or empty string."""
    if not tickers:
        return ""
    return " · ".join(f"${t}" for t in tickers.split(",") if t)


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

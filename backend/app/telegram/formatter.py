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

from typing import Any
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from app.ai.analyzer import AIAnalysis
from app.pipeline.scorer import EventType, ScoreResult
from app.pipeline.publish_decision import Decision

if TYPE_CHECKING:
    from app.ai.digest import DigestAnalysis

_MONTHS_RU = (
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)

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

_DIRECTION_LABEL: dict[str, str] = {
    "buy":  "🟢 Покупать",
    "sell": "🔴 Продавать",
    "hold": "🟡 Держать",
}


def format_message(
    cluster: Any,
    score_result: ScoreResult,
    decision: Decision,
    ai_analysis: Optional[AIAnalysis] = None,
    correlations: Optional[list] = None,
) -> str:
    """Compact Bloomberg-style MarkdownV2 channel message.

    With AI:
      🔴 *СТАВКА ЦБ* · 14:32

      *ЦБ повысил ставку до 21%*
      _Давление на акции и облигации_

      $SBER $VTBR

    Fallback:
      📰 *СТАВКА ЦБ* · 14:32

      *ЦБ повысил ключевую ставку до 21%*

      $SBER $VTBR
    """
    is_update = decision == Decision.UPDATE
    badge     = _BADGE.get(score_result.event_type, "РЫНКИ")

    if ai_analysis and ai_analysis.tickers:
        ticker_line = " ".join(f"\\${t}" for t in ai_analysis.tickers)
    else:
        ticker_line = _format_tickers_compact(cluster["tickers"])

    if ai_analysis:
        emoji     = "🔄" if is_update else ai_analysis.emoji
        badge_str = f"↻ {badge}" if is_update else badge
        parts     = [
            f"{emoji} *{_esc(badge_str)}*",
            "",
            f"*{_esc(ai_analysis.title)}*",
        ]
        if ai_analysis.market_effect:
            parts.append(f"_{_esc(ai_analysis.market_effect)}_")
    else:
        emoji     = "🔄" if is_update else "📰"
        badge_str = f"↻ {badge}" if is_update else badge
        parts     = [
            f"{emoji} *{_esc(badge_str)}*",
            "",
            f"*{_esc(cluster['canonical_title'])}*",
        ]

    if ticker_line:
        parts += ["", ticker_line]
    elif not ai_analysis:
        affects = _AFFECTS.get(score_result.event_type, "акции")
        parts += ["", f"_{_esc(affects)}_"]

    return "\n".join(parts)


def format_digest(
    clusters: list[Any],
    ai_digest: Optional["DigestAnalysis"],
    label: str,
) -> str:
    """
    Format the daily digest as a MarkdownV2 Telegram message.

    label — display time, e.g. "18:30" or "22:00" (MSK).

    With AI:
      📋 *ДАЙДЖЕСТ — 10 МАЯ, 18:30*

      🔴 Газпром снизил дивиденды — давление на акции
      🟢 ЦБ сохранил ставку — поддержка для рынка

      _Итоги сессии: преобладает давление на рынок._

    Without AI:
      📋 *ДАЙДЖЕСТ — 10 МАЯ, 18:30*

      • Газпром снизил дивиденды
      • ЦБ сохранил ставку
    """
    now_msk  = datetime.now(timezone.utc)
    day      = now_msk.day
    month    = _MONTHS_RU[now_msk.month]
    date_str = f"{day} {month}"

    header = f"📋 *ДАЙДЖЕСТ — {_esc(date_str)}, {_esc(label)}*"
    lines  = [header, ""]

    for i, cluster in enumerate(clusters):
        title = cluster["canonical_title"]
        if ai_digest and i < len(ai_digest.items):
            item   = ai_digest.items[i]
            emoji  = item["emoji"]
            effect = item["effect"]
            lines.append(f"{emoji} {_esc(title)} — {_esc(effect)}")
        else:
            lines.append(f"• {_esc(title)}")

    if ai_digest and ai_digest.summary:
        lines += ["", f"_{_esc(ai_digest.summary)}_"]

    return "\n".join(lines)


def format_trade_dm(title: str, tickers: list, signal: Any) -> str:
    """
    Format a TradeSignal as a MarkdownV2 Telegram DM.

    title   — raw canonical title (will be escaped)
    tickers — list of MOEX tickers, e.g. ["SBER", "VTBR"]
    signal  — TradeSignal instance
    """
    ticker_str  = " · ".join(f"${t}" for t in tickers)
    short_label = _DIRECTION_LABEL.get(signal.short_direction, "🟡 Держать")

    parts = [
        f"📊 *{ticker_str}* — {_esc(title)}",
        "",
        f"⏱ *Краткосрочно:* {short_label}",
    ]

    if signal.short_reason:
        parts.append(_esc(signal.short_reason))

    if signal.long_direction:
        long_label = _DIRECTION_LABEL.get(signal.long_direction, "🟡 Держать")
        parts += [
            "",
            f"📅 *Долгосрочно:* {long_label}",
            _esc(signal.long_reason),
        ]

    parts += [
        "",
        "⚠️ _Не является инвестиционной рекомендацией_",
    ]

    return "\n".join(parts)


def _format_tickers(tickers: Optional[str]) -> str:
    """Format comma-separated tickers as '$GAZP · $SBER', or empty string."""
    if not tickers:
        return ""
    return " · ".join(f"${t}" for t in tickers.split(",") if t)


def _format_tickers_compact(tickers: Optional[str]) -> str:
    """Format comma-separated tickers as '$GAZP $SBER' (space-separated)."""
    if not tickers:
        return ""
    return " ".join(f"\\${t}" for t in tickers.split(",") if t)


def _format_correlations(correlations: list) -> str:
    """
    Format historical correlation line, e.g.:
      📊 По истории: $LKOH -3.2% · $ROSN -2.1% за 24ч (n=6)
    Returns empty string if no correlations.
    """
    if not correlations:
        return ""
    shown = correlations[:3]
    n = max(c.sample_count for c in shown)
    parts = []
    for c in shown:
        val = f"{c.avg_24h_pct:+.1f}%"
        parts.append(f"${c.ticker} {_esc(val)}")
    stats = " · ".join(parts)
    return f"📊 По истории: {stats} за 24ч \\(n\\={n}\\)"


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

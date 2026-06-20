"""
Telegram message formatter.

Message format with AI analysis (MarkdownV2):
─────────────────────────────────────────────
  🟢 *ЦБ сохранил ставку на уровне 21%*

  _Рынок ждал снижения, но ЦБ не увидел оснований. Давление на банки сохраняется._

  _Следующее заседание ЦБ — 25 июля._

  $SBER $VTBR

Fallback (no AI):
─────────────────
  📰 *ЦБ сохранил ключевую ставку на уровне 21%*

  $SBER $VTBR
"""

from typing import Any
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from app.ai.analyzer import AIAnalysis
from app.pipeline.scorer import ScoreResult
from app.pipeline.publish_decision import Decision

if TYPE_CHECKING:
    from app.ai.digest import DigestAnalysis

_MONTHS_RU = (
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)


def _depth(score: int) -> str:
    """Return format depth based on importance score."""
    if score >= 50:
        return "full"
    if score >= 30:
        return "medium"
    return "compact"


def format_message(
    cluster: Any,
    score_result: ScoreResult,
    decision: Decision,
    ai_analysis: Optional[AIAnalysis] = None,
    correlations: Optional[list] = None,
) -> str:
    """Telegram channel message in MarkdownV2."""
    is_update = decision == Decision.UPDATE
    depth = _depth(score_result.score)
    if ai_analysis and ai_analysis.tickers:
        ticker_line = " ".join(f"\\${t}" for t in ai_analysis.tickers)
    else:
        ticker_line = _format_tickers_compact(cluster["tickers"])

    if ai_analysis:
        return _format_with_ai(ai_analysis, is_update, depth, ticker_line, correlations)
    else:
        return _format_fallback(cluster, is_update, ticker_line)


def _format_with_ai(
    ai: AIAnalysis,
    is_update: bool,
    depth: str,
    ticker_line: str,
    correlations: Optional[list],
) -> str:
    if is_update:
        prefix = "🔄 "
    elif ai.sentiment == "positive":
        prefix = "🟢 "
    elif ai.sentiment == "negative":
        prefix = "🔴 "
    else:
        prefix = ""

    parts = [f"{prefix}*{_esc(ai.summary)}*"]

    if depth in ("full", "medium") and ai.what_behind:
        parts += ["", f"_{_esc(ai.what_behind)}_"]

    if depth == "full" and ai.watch_for:
        parts += ["", f"_{_esc(ai.watch_for)}_"]

    if ticker_line:
        parts += ["", ticker_line]

    return "\n".join(parts)


def _format_fallback(cluster: Any, is_update: bool, ticker_line: str) -> str:
    emoji = "🔄" if is_update else "📰"
    parts = [f"{emoji} *{_esc(cluster['canonical_title'])}*"]
    if ticker_line:
        parts += ["", ticker_line]
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


def format_ticker_dm(canonical_title: str, tickers: list, ai_analysis: AIAnalysis) -> str:
    """
    Format an AI-enriched post as a MarkdownV2 Telegram DM for portfolio subscribers.

    canonical_title — raw title (used only as fallback if summary is empty)
    tickers         — list of MOEX tickers, e.g. ["SBER", "VTBR"]
    ai_analysis     — AIAnalysis instance
    """
    ticker_str = " ".join(f"\\${t}" for t in tickers)
    summary = ai_analysis.summary or canonical_title

    if ai_analysis.sentiment == "positive":
        prefix = "🟢 "
    elif ai_analysis.sentiment == "negative":
        prefix = "🔴 "
    else:
        prefix = ""
    parts = [f"{prefix}*{_esc(summary)}*"]
    if ai_analysis.what_behind:
        parts += ["", f"Контекст: _{_esc(ai_analysis.what_behind)}_"]
    if ai_analysis.watch_for:
        parts += ["", f"Следим за: _{_esc(ai_analysis.watch_for)}_"]
    if ticker_str:
        parts += ["", ticker_str]

    return "\n".join(parts)


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

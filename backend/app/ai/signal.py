"""
Trade signal generation.

build_signal() → TradeSignal
  - Short-term: derived from historical price correlations (TickerCorrelation)
    or ai_analysis.impact as fallback when no data.
  - Long-term: one AI call (~50 tokens via OpenRouter).
    Returns empty strings on any failure (fallback-safe).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp

from app.core.config import settings

if TYPE_CHECKING:
    from app.ai.analyzer import AIAnalysis
    from app.pipeline.price_history import TickerCorrelation

logger = logging.getLogger(__name__)

_API_URL = "https://openrouter.ai/api/v1/chat/completions"
_MODEL   = "openai/gpt-oss-120b:free"
_TIMEOUT = aiohttp.ClientTimeout(total=15)


@dataclass(frozen=True)
class TradeSignal:
    short_direction: str  # "buy" | "sell" | "hold"
    short_reason:    str  # human-readable; empty string → "insufficient data" in formatter
    long_direction:  str  # "buy" | "sell" | "hold" — empty if AI unavailable
    long_reason:     str  # AI one-liner — empty if AI unavailable


async def build_signal(
    correlations: list,
    ai_analysis:  AIAnalysis,
    title:        str,
    event_type:   str,
) -> TradeSignal:
    """Build short+long trade signal. Never raises."""
    short_direction, short_reason = _short_signal(correlations, ai_analysis)
    try:
        long_direction, long_reason = await _ai_long_term(title, event_type, short_direction)
    except Exception:
        long_direction, long_reason = "", ""
    return TradeSignal(
        short_direction=short_direction,
        short_reason=short_reason,
        long_direction=long_direction,
        long_reason=long_reason,
    )


def _short_signal(
    correlations: list,
    ai_analysis:  AIAnalysis,
) -> tuple[str, str]:
    """Return (direction, reason). reason="" means 'insufficient data'."""
    if correlations:
        avg = sum(c.avg_24h_pct for c in correlations) / len(correlations)
        if avg < -1.5:
            direction = "sell"
        elif avg > 1.5:
            direction = "buy"
        else:
            direction = "hold"
        reason = f"По истории: {avg:+.1f}% за сутки после подобных новостей"
        return direction, reason

    # Fallback: derive from AI impact
    if ai_analysis.impact == "negative":
        return "sell", ""
    if ai_analysis.impact == "positive":
        return "buy", ""
    return "hold", ""


async def _ai_long_term(
    title:           str,
    event_type:      str,
    short_direction: str,
) -> tuple[str, str]:
    """Call AI for long-term outlook. Returns ("", "") on any failure."""
    if not settings.openrouter_api_key:
        return "", ""

    prompt = (
        f"Новость: {title}\n"
        f"Тип события: {event_type}\n"
        f"Краткосрочный сигнал: {short_direction}\n"
        "Каков долгосрочный прогноз (горизонт недели–месяц)?\n"
        '{"direction": "buy|sell|hold", "reason": "одно предложение ≤12 слов"}'
    )
    payload = {
        "model": _MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 80,
    }
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type":  "application/json",
    }

    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(_API_URL, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    return "", ""
                body = await resp.json(content_type=None)
        raw  = body["choices"][0]["message"]["content"] or ""
        data = json.loads(raw)
        direction = str(data.get("direction", "")).lower().strip()
        reason    = str(data.get("reason", "")).strip()
        if direction not in ("buy", "sell", "hold"):
            return "", ""
        return direction, reason
    except Exception:
        logger.warning("_ai_long_term failed for: %.60s", title, exc_info=True)
        return "", ""

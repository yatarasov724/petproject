"""Tests for trade signal generation logic (app/ai/signal.py)."""
import pytest
from unittest.mock import AsyncMock, patch

from app.pipeline.price_history import TickerCorrelation


def _corr(ticker: str, pct: float, n: int = 5) -> TickerCorrelation:
    return TickerCorrelation(ticker=ticker, avg_24h_pct=pct, sample_count=n)


def _ai(impact: str = "negative"):
    from app.ai.analyzer import AIAnalysis
    return AIAnalysis(
        title="ЦБ повысил ставку",
        impact=impact,
        emoji="🔴" if impact == "negative" else "🟢",
        summary="...",
        market_effect="...",
        affects="акции",
        tickers=["SBER"],
    )


# ── _short_signal ──────────────────────────────────────────────────────────

def test_short_signal_sell_on_negative_history():
    from app.ai.signal import _short_signal
    direction, reason = _short_signal([_corr("SBER", -2.0)], _ai())
    assert direction == "sell"
    assert "По истории" in reason
    assert "-2.0%" in reason


def test_short_signal_buy_on_positive_history():
    from app.ai.signal import _short_signal
    direction, reason = _short_signal([_corr("SBER", 2.5)], _ai("positive"))
    assert direction == "buy"
    assert "+2.5%" in reason


def test_short_signal_hold_on_small_change():
    from app.ai.signal import _short_signal
    direction, reason = _short_signal([_corr("SBER", 0.5)], _ai())
    assert direction == "hold"
    assert "По истории" in reason


def test_short_signal_averages_multiple_tickers():
    from app.ai.signal import _short_signal
    # avg(-3.0, -1.0) = -2.0 → sell
    direction, reason = _short_signal([_corr("SBER", -3.0), _corr("VTBR", -1.0)], _ai())
    assert direction == "sell"
    assert "-2.0%" in reason


def test_short_signal_threshold_exact_negative():
    from app.ai.signal import _short_signal
    # exactly -1.5 → hold (threshold is strictly less than -1.5)
    direction, _ = _short_signal([_corr("SBER", -1.5)], _ai())
    assert direction == "hold"


def test_short_signal_threshold_just_below():
    from app.ai.signal import _short_signal
    direction, _ = _short_signal([_corr("SBER", -1.51)], _ai())
    assert direction == "sell"


def test_short_signal_fallback_negative_impact():
    from app.ai.signal import _short_signal
    direction, reason = _short_signal([], _ai("negative"))
    assert direction == "sell"
    assert reason == ""


def test_short_signal_fallback_positive_impact():
    from app.ai.signal import _short_signal
    direction, reason = _short_signal([], _ai("positive"))
    assert direction == "buy"
    assert reason == ""


def test_short_signal_fallback_unknown_impact():
    from app.ai.signal import _short_signal
    from app.ai.analyzer import AIAnalysis
    ai_neutral = AIAnalysis(
        title="...", impact="", emoji="🔴",
        summary="", market_effect="", affects="", tickers=[],
    )
    direction, reason = _short_signal([], ai_neutral)
    assert direction == "hold"
    assert reason == ""


# ── build_signal ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_build_signal_long_term_from_ai():
    from app.ai.signal import build_signal
    corrs = [_corr("SBER", -2.0)]
    with patch("app.ai.signal._ai_long_term", AsyncMock(return_value=("hold", "Цикл временный"))):
        signal = await build_signal(corrs, _ai(), "Заголовок", "rate_decision")
    assert signal.short_direction == "sell"
    assert signal.long_direction == "hold"
    assert signal.long_reason == "Цикл временный"


@pytest.mark.asyncio
async def test_build_signal_no_long_when_ai_unavailable():
    from app.ai.signal import build_signal
    with patch("app.ai.signal._ai_long_term", AsyncMock(return_value=("", ""))):
        signal = await build_signal([], _ai(), "Заголовок", "rate_decision")
    assert signal.long_direction == ""
    assert signal.long_reason == ""


@pytest.mark.asyncio
async def test_build_signal_ai_exception_is_graceful():
    from app.ai.signal import build_signal
    with patch("app.ai.signal._ai_long_term", AsyncMock(side_effect=Exception("network"))):
        signal = await build_signal([], _ai(), "Заголовок", "rate_decision")
    assert signal.long_direction == ""
    assert signal.long_reason == ""

# Trade Signal DM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send a personal DM with a short/long-term trade signal (buy/sell/hold) to users who have a relevant ticker in their portfolio.

**Architecture:** New `app/ai/signal.py` builds a `TradeSignal` from historical correlations and an AI long-term call; `format_trade_dm()` in `formatter.py` renders it as MarkdownV2; `notify_with_ai()` in `portfolio.py` replaces its current message body with the new format; `orchestrator.py` passes `correlations` and `event_type` to `notify_with_ai()`.

**Tech Stack:** Python 3.11, aiohttp, pytest-asyncio, OpenRouter API (same `openai/gpt-oss-120b:free` model as `analyzer.py`)

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| **Create** | `app/ai/signal.py` | `TradeSignal` dataclass, `build_signal()`, `_short_signal()`, `_ai_long_term()` |
| **Modify** | `app/telegram/formatter.py` | Add `format_trade_dm(title, tickers, signal) -> str` |
| **Modify** | `app/bot/portfolio.py` | Update `notify_with_ai()` signature + body to use signal |
| **Modify** | `app/pipeline/orchestrator.py` | Pass `correlations` and `event_type` to `notify_with_ai()` |
| **Create** | `tests/test_signal.py` | Unit tests for signal logic and DM formatting |
| **Modify** | `tests/test_portfolio.py` | Update `test_notify_with_ai_*` tests to match new format |
| **Modify** | `tests/conftest.py` | Add `_no_real_signal_ai` autouse fixture |

All paths are relative to `/opt/newsparser/backend/`.

---

### Task 1: Write failing tests for `_short_signal()` and `build_signal()`

**Files:**
- Create: `tests/test_signal.py`

- [ ] **Step 1: Create the test file**

```python
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
    # exactly -1.5 → hold (threshold is strictly less than)
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
    # When impact is neither positive nor negative, default to hold
    ai = _ai("negative")
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
```

- [ ] **Step 2: Run tests to verify they fail (module missing)**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_signal.py -v 2>&1 | head -30
```
Expected: `ModuleNotFoundError: No module named 'app.ai.signal'`

---

### Task 2: Create `app/ai/signal.py` to make signal tests pass

**Files:**
- Create: `app/ai/signal.py`

- [ ] **Step 1: Create the file**

```python
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
    correlations: list[TickerCorrelation],
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
    correlations: list[TickerCorrelation],
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
```

- [ ] **Step 2: Run the signal tests**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_signal.py -v 2>&1 | tail -20
```
Expected: all 12 tests PASS (the `build_signal` tests mock `_ai_long_term`; no real API call)

- [ ] **Step 3: Commit**

```bash
cd /opt/newsparser/backend
git add app/ai/signal.py tests/test_signal.py
git commit -m "feat: add TradeSignal + build_signal with short/long term logic"
```

---

### Task 3: Write failing tests for `format_trade_dm()` and add to `test_signal.py`

**Files:**
- Modify: `tests/test_signal.py` (append)

- [ ] **Step 1: Append formatter tests to `tests/test_signal.py`**

Add after the last existing test in the file:

```python
# ── format_trade_dm ───────────────────────────────────────────────────────────

from app.ai.signal import TradeSignal


def test_format_trade_dm_structure_with_long_term():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal(
        short_direction="sell",
        short_reason="По истории: -3.2% за сутки после подобных новостей",
        long_direction="hold",
        long_reason="Цикл ужесточения временный",
    )
    text = format_trade_dm("ЦБ повысил ставку", ["SBER"], signal)
    assert "📊" in text
    assert "SBER" in text
    assert "ЦБ" in text
    assert "⏱" in text
    assert "Краткосрочно" in text
    assert "🔴" in text           # sell
    assert "По истории" in text
    assert "📅" in text
    assert "Долгосрочно" in text
    assert "🟡" in text           # hold
    assert "Цикл ужесточения" in text
    assert "⚠️" in text
    assert "инвестиционной рекомендацией" in text


def test_format_trade_dm_multiple_tickers_in_header():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("buy", "По истории: +2.1%", "", "")
    text = format_trade_dm("Заголовок", ["SBER", "VTBR"], signal)
    assert "SBER" in text
    assert "VTBR" in text


def test_format_trade_dm_no_long_block_when_empty():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("hold", "По истории: +0.4%", "", "")
    text = format_trade_dm("Заголовок", ["SBER"], signal)
    assert "Долгосрочно" not in text
    assert "📅" not in text


def test_format_trade_dm_insufficient_data_shown_when_no_reason():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("sell", "", "", "")
    text = format_trade_dm("Заголовок", ["SBER"], signal)
    assert "недостаточно данных" in text


def test_format_trade_dm_buy_shows_green_emoji():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("buy", "По истории: +2.0%", "", "")
    text = format_trade_dm("Заголовок", ["SBER"], signal)
    assert "🟢" in text


def test_format_trade_dm_sell_shows_red_emoji():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("sell", "По истории: -2.0%", "", "")
    text = format_trade_dm("Заголовок", ["SBER"], signal)
    assert "🔴" in text


def test_format_trade_dm_hold_shows_yellow_emoji():
    from app.telegram.formatter import format_trade_dm
    signal = TradeSignal("hold", "По истории: +0.5%", "", "")
    text = format_trade_dm("Заголовок", ["SBER"], signal)
    assert "🟡" in text
```

- [ ] **Step 2: Run new tests to confirm they fail**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_signal.py::test_format_trade_dm_structure_with_long_term -v
```
Expected: `AttributeError: module 'app.telegram.formatter' has no attribute 'format_trade_dm'`

---

### Task 4: Add `format_trade_dm()` to `app/telegram/formatter.py`

**Files:**
- Modify: `app/telegram/formatter.py`

- [ ] **Step 1: Add direction map constant and `format_trade_dm` function**

Add after the `_AFFECTS` dict (before `def format_message`):

```python
_DIRECTION_LABEL: dict[str, str] = {
    "buy":  "🟢 Покупать",
    "sell": "🔴 Продавать",
    "hold": "🟡 Держать",
}
```

Add after the `format_digest` function (before `def _format_tickers`):

```python
def format_trade_dm(title: str, tickers: list[str], signal: Any) -> str:
    """
    Format a TradeSignal as a MarkdownV2 Telegram DM.

    title   — raw canonical title (will be escaped)
    tickers — list of MOEX tickers, e.g. ["SBER", "VTBR"]
    signal  — TradeSignal instance
    """
    ticker_str    = " · ".join(f"${t}" for t in tickers)
    short_label   = _DIRECTION_LABEL.get(signal.short_direction, "🟡 Держать")

    parts = [
        f"📊 *{ticker_str}* — {_esc(title)}",
        "",
        f"⏱ *Краткосрочно:* {short_label}",
    ]

    if signal.short_reason:
        parts.append(_esc(signal.short_reason))
    else:
        parts.append("_" + _esc("(недостаточно данных)") + "_")

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
```

- [ ] **Step 2: Run all format tests**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_signal.py -v 2>&1 | tail -25
```
Expected: all 19 tests PASS

- [ ] **Step 3: Commit**

```bash
cd /opt/newsparser/backend
git add app/telegram/formatter.py tests/test_signal.py
git commit -m "feat: add format_trade_dm() to formatter"
```

---

### Task 5: Update conftest and `test_portfolio.py` for new `notify_with_ai()` format

**Files:**
- Modify: `tests/conftest.py`
- Modify: `tests/test_portfolio.py`

- [ ] **Step 1: Add `_no_real_signal_ai` autouse fixture to `conftest.py`**

Add after the existing `_no_real_ai` fixture:

```python
@pytest.fixture(autouse=True)
def _no_real_signal_ai():
    """Prevent background OpenRouter HTTP calls from signal long-term AI in every test."""
    with patch("app.ai.signal._ai_long_term", return_value=("", "")):
        yield
```

Note: this uses `patch` with a synchronous return value but `_ai_long_term` is async. Use `AsyncMock`:

```python
from unittest.mock import AsyncMock, patch

@pytest.fixture(autouse=True)
def _no_real_signal_ai():
    """Prevent OpenRouter calls from _ai_long_term in every test."""
    with patch("app.ai.signal._ai_long_term", new=AsyncMock(return_value=("", ""))):
        yield
```

- [ ] **Step 2: Update the three `test_notify_with_ai_*` tests in `test_portfolio.py`**

Replace the three existing `test_notify_with_ai_*` tests (lines 203–305) with:

```python
@pytest.mark.asyncio
async def test_notify_with_ai_sends_trade_signal_dm(db):
    queries.set_user_tickers(db, 111, ["SBER"])

    from app.ai.analyzer import AIAnalysis
    from app.ai.signal import TradeSignal
    ai = AIAnalysis(
        title="ЦБ сохранил ставку",
        impact="positive",
        emoji="🟢",
        summary="Банк России оставил ставку на уровне 21%",
        market_effect="поддержка для ОФЗ и банков",
        affects="ОФЗ · акции",
        tickers=["SBER"],
        context="",
    )
    mock_signal = TradeSignal("buy", "По истории: +1.8%", "hold", "Краткосрочная волатильность спадёт")

    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 42

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
        patch("app.bot.portfolio.build_signal", AsyncMock(return_value=mock_signal)),
        patch.object(db, "close"),
    ):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("SBER", ai, cluster_id=1, canonical_title="ЦБ сохранил ставку")

    assert len(sent_texts) == 1
    msg = sent_texts[0]
    assert "📊" in msg
    assert "SBER" in msg
    assert "Краткосрочно" in msg
    assert "инвестиционной рекомендацией" in msg


@pytest.mark.asyncio
async def test_notify_with_ai_skips_no_subscribers():
    """No DM sent when no user subscribed to the tickers."""
    from app.ai.analyzer import AIAnalysis
    ai = AIAnalysis(
        title="Заголовок",
        impact="negative",
        emoji="🔴",
        summary="...",
        market_effect="...",
        affects="",
        tickers=["NVTK"],
        context="",
    )
    mock_dm = AsyncMock(return_value=None)
    with patch("app.bot.portfolio.send_dm", mock_dm):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("NVTK", ai, cluster_id=5, canonical_title="Заголовок")

    mock_dm.assert_not_called()


@pytest.mark.asyncio
async def test_notify_with_ai_includes_all_tickers_in_dm(db):
    """Header shows all tickers when AI analysis has multiple."""
    queries.set_user_tickers(db, 111, ["SBER"])

    from app.ai.analyzer import AIAnalysis
    from app.ai.signal import TradeSignal
    ai = AIAnalysis(
        title="Банки под давлением",
        impact="negative",
        emoji="🔴",
        summary="...",
        market_effect="...",
        affects="акции",
        tickers=["SBER", "VTBR"],
        context="",
    )
    mock_signal = TradeSignal("sell", "", "", "")

    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 42

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
        patch("app.bot.portfolio.build_signal", AsyncMock(return_value=mock_signal)),
        patch.object(db, "close"),
    ):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("SBER", ai, cluster_id=1, canonical_title="Банки под давлением")

    assert "VTBR" in sent_texts[0]
```

- [ ] **Step 3: Run updated portfolio tests (they should fail on missing `build_signal` import)**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_portfolio.py::test_notify_with_ai_sends_trade_signal_dm -v 2>&1 | tail -15
```
Expected: `ImportError` or `AttributeError` — `app.bot.portfolio` doesn't have `build_signal` yet

---

### Task 6: Update `app/bot/portfolio.py`

**Files:**
- Modify: `app/bot/portfolio.py`

- [ ] **Step 1: Replace the file content**

Full new content of `app/bot/portfolio.py`:

```python
"""
Portfolio notification dispatch.

Called from orchestrator (fire-and-forget asyncio.Task) after a successful publish.
Opens its own DB connection so it can safely run after the poll cycle closes the main one.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.ai.analyzer import AIAnalysis

import logging

from app.ai.signal import build_signal
from app.core import metrics
from app.db import queries
from app.db.database import get_db
from app.telegram.client import send_dm
from app.telegram.formatter import format_trade_dm, _esc

logger = logging.getLogger(__name__)


async def notify(tickers_raw: str, canonical_title: str, cluster_id: int, score: int | None = None) -> None:
    """
    Send DMs to all users subscribed to any ticker in this cluster.

    tickers_raw — comma-separated MOEX ticker string, e.g. "GAZP,SBER".
    """
    tickers = [t.strip() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    db = get_db()
    try:
        user_ids = queries.get_subscribed_users(db, tickers)
    finally:
        db.close()

    if not user_ids:
        metrics.inc(metrics.PORTFOLIO_NO_SUBS)
        logger.debug(
            "portfolio notify: 0 subscribers for tickers=%s cluster_id=%d",
            ",".join(tickers), cluster_id,
            extra={"event": "portfolio_no_subs", "tickers": ",".join(tickers), "cluster_id": cluster_id},
        )
        return

    tickers_line = " · ".join(f"\\${t}" for t in tickers)
    title = _esc(canonical_title)
    text = f"*{title}*\n\n{tickers_line}"

    for user_id in user_ids:
        msg_id = await send_dm(user_id, text)
        ok = msg_id is not None
        if ok:
            metrics.inc(metrics.PORTFOLIO_DM_SENT)
        else:
            metrics.inc(metrics.PORTFOLIO_DM_FAILED)
        logger.info(
            "portfolio notify %s: user_id=%d cluster_id=%d tickers=%s",
            "ok" if ok else "failed",
            user_id,
            cluster_id,
            ",".join(tickers),
            extra={
                "event":      "portfolio_notify_ok" if ok else "portfolio_notify_failed",
                "user_id":    user_id,
                "cluster_id": cluster_id,
            },
        )


async def notify_with_ai(
    tickers_raw:    str,
    ai_analysis:    "AIAnalysis",
    cluster_id:     int,
    canonical_title: str = "",
    correlations:   list | None = None,
    event_type:     str = "",
) -> None:
    """Send trade-signal DM to all users subscribed to any ticker in this cluster."""
    tickers = [t.strip() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    db = get_db()
    try:
        user_ids = queries.get_subscribed_users(db, tickers)
    finally:
        db.close()

    if not user_ids:
        metrics.inc(metrics.PORTFOLIO_NO_SUBS)
        return

    if ai_analysis.tickers and canonical_title:
        from app.pipeline.ticker_validator import validate_tickers
        safe = validate_tickers(",".join(ai_analysis.tickers), canonical_title)
        dm_tickers = [t for t in safe.split(",") if t] if safe else tickers
    else:
        dm_tickers = ai_analysis.tickers if ai_analysis.tickers else tickers

    signal = await build_signal(correlations or [], ai_analysis, canonical_title, event_type)
    text = format_trade_dm(canonical_title, dm_tickers, signal)

    for user_id in user_ids:
        msg_id = await send_dm(user_id, text)
        ok = msg_id is not None
        metrics.inc(metrics.PORTFOLIO_DM_SENT if ok else metrics.PORTFOLIO_DM_FAILED)
        logger.info(
            "portfolio notify_with_ai %s: user_id=%d cluster_id=%d",
            "ok" if ok else "failed",
            user_id,
            cluster_id,
            extra={
                "event":      "portfolio_notify_ai_ok" if ok else "portfolio_notify_ai_failed",
                "user_id":    user_id,
                "cluster_id": cluster_id,
            },
        )
```

- [ ] **Step 2: Run all portfolio tests**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_portfolio.py -v 2>&1 | tail -30
```
Expected: all tests PASS (including updated `test_notify_with_ai_*`)

- [ ] **Step 3: Commit**

```bash
cd /opt/newsparser/backend
git add app/bot/portfolio.py tests/test_portfolio.py tests/conftest.py
git commit -m "feat: notify_with_ai sends trade signal DM via build_signal + format_trade_dm"
```

---

### Task 7: Update `app/pipeline/orchestrator.py` to pass correlations and event_type

**Files:**
- Modify: `app/pipeline/orchestrator.py`

- [ ] **Step 1: Update the `notify_with_ai` call inside `_ai_enrich()`**

In `_ai_enrich()`, find the block at lines ~484–486:

```python
        if tickers_raw:
            from app.bot.portfolio import notify_with_ai
            await notify_with_ai(tickers_raw, ai_analysis, cluster["id"], canonical_title)
```

Replace with:

```python
        if tickers_raw:
            from app.bot.portfolio import notify_with_ai
            await notify_with_ai(
                tickers_raw, ai_analysis, cluster["id"], canonical_title,
                correlations=correlations,
                event_type=score_result.event_type.value,
            )
```

- [ ] **Step 2: Run the full pipeline test suite**

```bash
cd /opt/newsparser/backend && python -m pytest tests/test_pipeline.py tests/test_portfolio.py tests/test_signal.py -v 2>&1 | tail -30
```
Expected: all PASS

- [ ] **Step 3: Commit**

```bash
cd /opt/newsparser/backend
git add app/pipeline/orchestrator.py
git commit -m "feat: pass correlations and event_type to notify_with_ai from orchestrator"
```

---

### Task 8: Run full test suite and deploy

**Files:** none

- [ ] **Step 1: Run the full test suite**

```bash
cd /opt/newsparser/backend && python -m pytest tests/ -v --tb=short 2>&1 | tail -40
```
Expected: no failures

- [ ] **Step 2: Build and restart the Docker container**

```bash
cd /opt/newsparser && docker compose build --no-cache backend && docker compose up -d
```
Expected: container starts without error

- [ ] **Step 3: Verify DRY_RUN signal delivery (manual)**

```bash
# Check logs for the portfolio notify path
docker compose logs backend --since 5m | grep -E "portfolio_notify_ai|trade_signal|format_trade"
```

- [ ] **Step 4: Verify fallback (no OpenRouter key)**

```bash
# Temporarily verify with key unset: long-term block must be absent
docker compose logs backend --since 1m | grep portfolio_notify_ai_ok
```

- [ ] **Step 5: Final commit if any fixes needed, else done**

```bash
cd /opt/newsparser
git log --oneline -5
```

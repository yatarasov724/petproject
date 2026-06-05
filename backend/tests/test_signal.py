"""Tests for format_ticker_dm (app/telegram/formatter.py)."""
from app.ai.analyzer import AIAnalysis


def _ai(
    summary: str = "ЦБ повысил ставку до 22%",
    what_behind: str = "Шестое повышение подряд — цикл ужесточения продолжается.",
    watch_for: str = "Следующее заседание ЦБ — 25 июля.",
    tickers: list | None = None,
) -> AIAnalysis:
    return AIAnalysis(
        summary=summary,
        what_behind=what_behind,
        watch_for=watch_for,
        tickers=tickers or ["SBER"],
    )


# ── format_ticker_dm ──────────────────────────────────────────────────────────

def test_format_ticker_dm_structure():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("ЦБ повысил ставку", ["SBER"], _ai())
    assert "Контекст:" in text
    assert "Следим за:" in text
    assert "SBER" in text


def test_format_ticker_dm_contains_summary():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("заголовок", ["SBER"], _ai(summary="Банк России повысил ставку до 22%"))
    assert "Банк России" in text


def test_format_ticker_dm_contains_what_behind():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("заголовок", ["SBER"], _ai(what_behind="Шестое повышение подряд"))
    assert "Шестое повышение" in text


def test_format_ticker_dm_contains_watch_for():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("заголовок", ["SBER"], _ai(watch_for="Следующее заседание — 25 июля"))
    assert "Следующее заседание" in text


def test_format_ticker_dm_multiple_tickers():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("заголовок", ["SBER", "VTBR"], _ai())
    assert "SBER" in text
    assert "VTBR" in text


def test_format_ticker_dm_no_investment_disclaimer():
    from app.telegram.formatter import format_ticker_dm
    text = format_ticker_dm("заголовок", ["SBER"], _ai())
    assert "инвестиционной рекомендацией" not in text
    assert "купи" not in text.lower()
    assert "продай" not in text.lower()
    assert "держи" not in text.lower()


def test_format_ticker_dm_fallback_to_canonical_title():
    from app.telegram.formatter import format_ticker_dm
    ai = AIAnalysis(summary="", what_behind="...", watch_for="...", tickers=["SBER"])
    text = format_ticker_dm("Канонический заголовок", ["SBER"], ai)
    assert "Канонический заголовок" in text


def test_format_ticker_dm_skips_empty_watch_for():
    from app.telegram.formatter import format_ticker_dm
    ai = AIAnalysis(summary="Суть", what_behind="Контекст", watch_for="", tickers=["SBER"])
    text = format_ticker_dm("заголовок", ["SBER"], ai)
    assert "Следим за:" not in text

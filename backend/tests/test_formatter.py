import pytest
from unittest.mock import MagicMock
from app.telegram.formatter import format_message, _depth
from app.pipeline.scorer import ScoreResult, EventType
from app.pipeline.publish_decision import Decision
from app.ai.analyzer import AIAnalysis


def _make_score(score: int, event_type: EventType = EventType.CORPORATE) -> ScoreResult:
    return ScoreResult(
        score=score,
        tier="tier1",
        event_type=event_type,
        base_score=score,
        keyword_bonus=0,
        source_bonus=0,
        type_bonus=0,
        matched_keywords=[],
    )


def _make_ai() -> AIAnalysis:
    return AIAnalysis(
        summary="Сбербанк повысил прогноз прибыли",
        what_behind="Рост кредитного портфеля — акции под давлением покупок",
        watch_for="Отчёт за Q2 — конец июля",
        tickers=["SBER"],
        sentiment="positive",
    )


def _make_cluster() -> dict:
    return {"canonical_title": "Сбербанк повысил прогноз прибыли", "tickers": "SBER"}


class TestDepth:
    def test_score_50_returns_full(self):
        assert _depth(50) == "full"

    def test_score_80_returns_full(self):
        assert _depth(80) == "full"

    def test_score_49_returns_medium(self):
        assert _depth(49) == "medium"

    def test_score_30_returns_medium(self):
        assert _depth(30) == "medium"

    def test_score_29_returns_compact(self):
        assert _depth(29) == "compact"

    def test_score_0_returns_compact(self):
        assert _depth(0) == "compact"


class TestFormatMessageEventTypes:
    """format_message produces distinct output per EventType."""

    def test_dividends_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.DIVIDENDS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "💰" in result

    def test_earnings_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.EARNINGS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "📊" in result

    def test_rate_decision_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.RATE_DECISION)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "🏦" in result

    def test_sanctions_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.SANCTIONS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "⚠️" in result

    def test_war_escalation_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.WAR_ESCALATION)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "⚠️" in result

    def test_ipo_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.IPO)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "🏢" in result


class TestFormatMessageScoreDepth:
    """High-score posts include more fields than low-score posts."""

    def test_full_score_includes_watch_for(self):
        cluster = _make_cluster()
        sr = _make_score(55, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "конец июля" in result  # watch_for content

    def test_medium_score_excludes_watch_for(self):
        ai = _make_ai()
        result = format_message(_make_cluster(), _make_score(35), Decision.NEW_EVENT, ai)
        assert "конец июля" not in result  # watch_for omitted

    def test_compact_score_excludes_what_behind(self):
        cluster = _make_cluster()
        sr = _make_score(20, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "кредитного портфеля" not in result  # what_behind omitted

    def test_compact_score_includes_summary(self):
        cluster = _make_cluster()
        sr = _make_score(20, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW_EVENT, ai)
        assert "Сбербанк повысил прогноз" in result

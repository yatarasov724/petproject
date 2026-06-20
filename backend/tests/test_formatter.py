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

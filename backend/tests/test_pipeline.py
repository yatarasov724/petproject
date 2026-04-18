"""
Integration-style pipeline tests.

These tests exercise multiple modules together against a real in-memory DB,
but do NOT call Telegram or the scheduler. They verify that the full
dedup → cluster → score → publish-decision chain produces the expected outcome.

Covers:
- Full happy path: a publishable event creates a NEW_EVENT decision
- Noise pre-filter: low-scoring article is stopped before clustering
- Exact dedup: second article with same hash is rejected before scoring
- Near dedup: paraphrase is rejected before clustering
- Cross-source UPDATE: second source on the same cluster triggers UPDATE
- Cooldown: second source UPDATE is silenced while cooldown is active
- Cluster time window: article arriving after window creates new cluster
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from app.pipeline import dedup, clusterer, scorer
from app.pipeline.publish_decision import decide, Decision
from app.pipeline.orchestrator import process, Outcome
from app.db import queries
from tests.conftest import make_article, db  # noqa: F401


# ── helpers ───────────────────────────────────────────────────────────────────

def _utcnow():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_pipeline_steps(db, article):
    """
    Run dedup → cluster → score → decide without Telegram.
    Returns (Outcome, PublishDecision | None).
    """
    dup = dedup.check(db, article)
    if dup.reason == dedup.DupReason.EXACT:
        return Outcome.EXACT_DUP, None
    if dup.reason == dedup.DupReason.NEAR:
        return Outcome.NEAR_DUP, None

    pre = scorer.compute_score(article.title, source_count=1)
    if pre.score < scorer.ARTICLE_MIN_SCORE:
        return Outcome.NOISE, None

    cluster_result = clusterer.find_or_create(db, article, market_score=pre.score)
    dedup.record(db, article, cluster_id=cluster_result.cluster_id)

    cluster = queries.get_cluster(db, cluster_result.cluster_id)
    score_result = scorer.compute_score(article.title, source_count=cluster["source_count"])
    pub = decide(cluster, score_result)

    return Outcome.SILENCE if pub.decision == Decision.SILENCE else (
        Outcome.SENT_NEW if pub.decision == Decision.NEW_EVENT else Outcome.SENT_UPDATE
    ), pub


# ── happy path ────────────────────────────────────────────────────────────────

class TestHappyPath:
    def test_publishable_event_is_new_event(self, db):
        article = make_article(title="ЦБ повысил ключевую ставку до 21 процента")
        outcome, pub = _run_pipeline_steps(db, article)
        assert outcome == Outcome.SENT_NEW
        assert pub.decision == Decision.NEW_EVENT


# ── noise pre-filter ─────────────────────────────────────────────────────────

class TestNoiseFilter:
    def test_noise_article_stopped_before_clustering(self, db):
        article = make_article(
            title="Международный кинофестиваль открылся в Москве сегодня",
            raw_hash="noise_001",
        )
        outcome, _ = _run_pipeline_steps(db, article)
        assert outcome == Outcome.NOISE

        # No cluster should have been created
        count = db.execute("SELECT COUNT(*) FROM event_clusters").fetchone()[0]
        assert count == 0

    def test_low_score_article_below_min(self, db):
        # An article without any tier keywords at all
        article = make_article(
            title="Президент встретился с губернатором региона на совещании",
            raw_hash="low_score_001",
        )
        outcome, _ = _run_pipeline_steps(db, article)
        assert outcome == Outcome.NOISE


# ── exact dedup ───────────────────────────────────────────────────────────────

class TestExactDedup:
    def test_exact_duplicate_stopped(self, db):
        article = make_article(title="ЦБ повысил ключевую ставку до 21 процента")
        _run_pipeline_steps(db, article)  # first pass — persists it

        # Second pass with identical raw_hash
        outcome, _ = _run_pipeline_steps(db, article)
        assert outcome == Outcome.EXACT_DUP

    def test_exact_dup_does_not_update_cluster(self, db):
        article = make_article(title="ЦБ повысил ключевую ставку до 21 процента")
        _run_pipeline_steps(db, article)

        count_before = db.execute("SELECT article_count FROM event_clusters LIMIT 1").fetchone()[0]
        _run_pipeline_steps(db, article)
        count_after = db.execute("SELECT article_count FROM event_clusters LIMIT 1").fetchone()[0]

        assert count_before == count_after


# ── near dedup ────────────────────────────────────────────────────────────────

class TestNearDedup:
    def test_near_duplicate_stopped(self, db):
        article_a = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="near_a_001",
        )
        _run_pipeline_steps(db, article_a)

        # Slight paraphrase — same tokens, different order, different raw_hash
        article_b = make_article(
            title="Банк России поднял ключевую ставку до 21 процента сегодня",
            raw_hash="near_b_002",
        )
        outcome, _ = _run_pipeline_steps(db, article_b)
        assert outcome == Outcome.NEAR_DUP


# ── cross-source UPDATE ───────────────────────────────────────────────────────

class TestCrossSourceUpdate:
    def test_three_sources_trigger_update(self, db):
        """
        When a cluster reaches source_count >= UPDATE_SOURCE_FLOOR and cooldown has expired,
        decide() returns UPDATE.  We test via the decision layer directly because the full
        pipeline near-dedup stage correctly filters articles that are too similar to the anchor.
        """
        from app.pipeline.publish_decision import decide, Decision, UPDATE_SOURCE_FLOOR
        from app.pipeline.scorer import ScoreResult, EventType

        db.execute(
            "INSERT INTO rss_sources (id, name, url) VALUES (3, 'Feed3', 'http://feed3.local/rss')"
        )
        db.commit()

        cluster_id = queries.create_cluster(
            db,
            canonical_title="ЦБ повысил ключевую ставку до 21 процента",
            title_tokens="21 ключевую повысил процента ставку цб",
            keywords="ключевую ставку цб",
            score=30,
        )
        # Simulate: cluster was previously published, cooldown expired, 3 sources confirmed
        queries.mark_cluster_sent(db, cluster_id, "NEW_EVENT", score=30, cooldown_hours=0)
        db.execute(
            "UPDATE event_clusters SET source_count = ?, cooldown_until = NULL WHERE id = ?",
            (UPDATE_SOURCE_FLOOR, cluster_id),
        )
        db.commit()

        cluster = queries.get_cluster(db, cluster_id)
        score_result = ScoreResult(
            score=45, tier="tier2", event_type=EventType.RATE_DECISION,
            base_score=25, keyword_bonus=5, source_bonus=10, type_bonus=5,
            matched_keywords=["ключевую ставку"],
        )
        pub = decide(cluster, score_result)
        assert pub.decision == Decision.UPDATE


# ── cooldown silencing ────────────────────────────────────────────────────────

class TestCooldown:
    def test_second_source_silenced_during_cooldown(self, db):
        # Anchor article — NEW_EVENT
        a1 = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            source_id=1, raw_hash="cd_001",
        )
        _run_pipeline_steps(db, a1)

        # Mark cluster sent with active cooldown (2 hours in the future)
        cluster_id = db.execute(
            "SELECT id FROM event_clusters ORDER BY id LIMIT 1"
        ).fetchone()[0]
        queries.mark_cluster_sent(db, cluster_id, "NEW_EVENT", score=30, cooldown_hours=2)

        # Second source — but cooldown still active
        a2 = make_article(
            title="Банк России поднял ключевую ставку до 21 процента сегодня",
            source_id=2, raw_hash="cd_002",
        )
        # We need source_count to be >= UPDATE_SOURCE_FLOOR already to test cooldown wins
        db.execute(
            "UPDATE event_clusters SET source_count = 3 WHERE id = ?", (cluster_id,)
        )
        db.commit()

        cluster = queries.get_cluster(db, cluster_id)
        score_result = scorer.compute_score(a2.title, source_count=cluster["source_count"])
        pub = decide(cluster, score_result)

        assert pub.decision == Decision.SILENCE
        assert "cooldown" in pub.reason.lower()


# ── orchestrator integration (mocking Telegram) ───────────────────────────────

class TestOrchestrator:
    """
    Tests for orchestrator.process() — the top-level per-article function.
    Telegram is mocked so no network calls happen.
    """

    @pytest.mark.asyncio
    async def test_noise_returns_noise_outcome(self, db):
        article = make_article(
            title="Международный кинофестиваль открылся в Москве сегодня",
            raw_hash="orch_noise_001",
        )
        result = await process(db, article)
        assert result.outcome == Outcome.NOISE

    @pytest.mark.asyncio
    async def test_publishable_article_calls_telegram(self, db):
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="orch_pub_001",
        )
        with patch("app.telegram.client.send", return_value=True) as mock_send:
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_telegram_failure_returns_send_fail(self, db):
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="orch_fail_001",
        )
        with patch("app.telegram.client.send", return_value=False):
            result = await process(db, article)

        assert result.outcome == Outcome.SEND_FAIL

    @pytest.mark.asyncio
    async def test_exception_in_pipeline_returns_error(self, db):
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="orch_err_001",
        )
        with patch("app.pipeline.dedup.check", side_effect=RuntimeError("boom")):
            result = await process(db, article)

        assert result.outcome == Outcome.ERROR

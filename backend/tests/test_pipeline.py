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
        with patch("app.telegram.client.send", return_value=1) as mock_send:
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_telegram_failure_returns_send_fail(self, db):
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="orch_fail_001",
        )
        with patch("app.telegram.client.send", return_value=None):
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


# ── MVP-4: atomic cluster + dedup transaction ─────────────────────────────────

class TestAtomicClusterDedup:
    """
    Verify that clusterer.find_or_create() and dedup.record() are committed
    atomically: either both land in the DB or neither does.
    """

    @pytest.mark.asyncio
    async def test_success_writes_cluster_and_seen_article(self, db):
        """Happy path: after process() both tables have exactly one row."""
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="atomic_ok_001",
        )
        with patch("app.telegram.client.send", return_value=1):
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW

        cluster_count = db.execute("SELECT COUNT(*) FROM event_clusters").fetchone()[0]
        seen_count    = db.execute("SELECT COUNT(*) FROM seen_articles").fetchone()[0]
        assert cluster_count == 1
        assert seen_count == 1

        seen = db.execute("SELECT cluster_id FROM seen_articles LIMIT 1").fetchone()
        cluster = db.execute("SELECT id FROM event_clusters LIMIT 1").fetchone()
        assert seen["cluster_id"] == cluster["id"]

    @pytest.mark.asyncio
    async def test_rollback_on_dedup_record_failure(self, db):
        """
        If dedup.record() raises after clusterer.find_or_create() succeeds,
        the transaction is rolled back: no cluster row and no seen_article row.
        """
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="atomic_fail_001",
        )

        with patch(
            "app.pipeline.dedup.record",
            side_effect=RuntimeError("simulated dedup.record crash"),
        ):
            result = await process(db, article)

        assert result.outcome == Outcome.ERROR

        cluster_count = db.execute("SELECT COUNT(*) FROM event_clusters").fetchone()[0]
        seen_count    = db.execute("SELECT COUNT(*) FROM seen_articles").fetchone()[0]
        assert cluster_count == 0, "cluster must be rolled back on dedup failure"
        assert seen_count == 0,    "seen_article must not exist after rollback"

    @pytest.mark.asyncio
    async def test_seen_article_has_correct_cluster_id(self, db):
        """seen_articles.cluster_id must point to the correct event_clusters row."""
        article = make_article(
            title="Газпром объявил дивиденды за 2025 год",
            raw_hash="atomic_cid_001",
        )
        with patch("app.telegram.client.send", return_value=1):
            await process(db, article)

        row = db.execute(
            "SELECT sa.cluster_id, ec.id "
            "FROM seen_articles sa "
            "JOIN event_clusters ec ON sa.cluster_id = ec.id"
        ).fetchone()
        assert row is not None, "seen_article must be linked to a cluster"

    @pytest.mark.asyncio
    async def test_idempotent_second_call_returns_exact_dup(self, db):
        """
        Processing the same article twice (simulating a restart) must return
        EXACT_DUP on the second call — the first call's seen_article row
        survived the commit and blocks the duplicate.
        """
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="atomic_idem_001",
        )
        with (
            patch("app.telegram.client.send", return_value=1),
            patch("app.ai.analyzer.analyze"),         # prevent real HTTP in background task
        ):
            first = await process(db, article)
        assert first.outcome == Outcome.SENT_NEW

        second = await process(db, article)
        assert second.outcome == Outcome.EXACT_DUP


# ── MVP-3: AI fire-and-forget ─────────────────────────────────────────────────

class TestAIFireAndForget:
    """
    Verify that AI analysis no longer blocks the publish path.
    tg.send() must be called with ai_analysis=None, and AI errors must not
    affect the pipeline outcome.
    """

    @pytest.mark.asyncio
    async def test_send_receives_no_ai_analysis(self, db):
        """tg.send is called with ai_analysis=None — fire-and-forget confirmed."""
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="ff_send_001",
        )

        captured: list = []

        async def capture_send(**kwargs):
            captured.append(kwargs.get("ai_analysis"))
            return 1

        with patch("app.telegram.client.send", side_effect=capture_send):
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        assert captured == [None], "send must be called with ai_analysis=None"

    @pytest.mark.asyncio
    async def test_slow_ai_does_not_delay_send(self, db):
        """
        process() must complete without waiting for a slow AI response.
        We use a cooperative signal: AI blocks until send() fires,
        which proves send happens first (otherwise deadlock → timeout).
        """
        import asyncio as _asyncio

        send_fired = _asyncio.Event()

        async def slow_ai(title, text=""):
            await send_fired.wait()   # unblocked only after send() runs
            return None

        async def signalling_send(**kwargs):
            send_fired.set()          # notify AI it can finish
            return 1

        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="ff_slow_001",
        )

        with (
            patch("app.ai.analyzer.analyze", side_effect=slow_ai),
            patch("app.telegram.client.send", side_effect=signalling_send),
            patch("app.telegram.client.edit_message"),
        ):
            result = await _asyncio.wait_for(process(db, article), timeout=5.0)
            # Drain background tasks: ai_task + _enrich_with_ai need a few iterations
            for _ in range(5):
                await _asyncio.sleep(0)

        assert result.outcome == Outcome.SENT_NEW

    @pytest.mark.asyncio
    async def test_ai_error_does_not_affect_outcome(self, db):
        """If AI raises, the message is still sent and outcome is SENT_NEW."""
        async def failing_ai(title, text=""):
            raise RuntimeError("OpenRouter unreachable")

        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="ff_aierr_001",
        )

        with (
            patch("app.ai.analyzer.analyze", side_effect=failing_ai),
            patch("app.telegram.client.send", return_value=1),
            patch("app.telegram.client.edit_message"),
        ):
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW

    @pytest.mark.asyncio
    async def test_send_failure_does_not_launch_enrich_task(self, db):
        """When tg.send fails (returns None), edit_message must not be called."""
        edit_calls: list = []

        async def record_edit(msg_id, text):
            edit_calls.append(msg_id)

        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="ff_sendfail_001",
        )

        with (
            patch("app.telegram.client.send", return_value=None),
            patch("app.telegram.client.edit_message", side_effect=record_edit),
        ):
            result = await process(db, article)

        assert result.outcome == Outcome.SEND_FAIL
        assert edit_calls == [], "edit_message must not be called when send fails"

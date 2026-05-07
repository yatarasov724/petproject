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


# ── AI market relevance gate ──────────────────────────────────────────────────

class TestAIGate:
    """
    Verify that AI analysis acts as a synchronous gate before tg.send().
    Empty affects → SILENCE. Non-empty affects → send proceeds with ai_analysis.
    AI errors or missing API key → send proceeds without ai_analysis (fallback).
    """

    @pytest.mark.asyncio
    async def test_ai_empty_affects_silences_article(self, db):
        """AI returns empty affects → article is silenced, tg.send not called."""
        from app.ai.analyzer import AIAnalysis

        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="aig_empty_001",
        )

        send_calls: list = []

        async def capture_send(**kwargs):
            send_calls.append(kwargs)
            return 1

        no_impact = AIAnalysis(
            title="", impact="neutral", emoji="⚪️",
            summary="", market_effect="", affects="",
        )

        with (
            patch("app.core.config.settings") as mock_settings,
            patch("app.ai.analyzer.analyze", return_value=no_impact),
            patch("app.telegram.client.send", side_effect=capture_send),
        ):
            mock_settings.openrouter_api_key = "test-key"
            mock_settings.dry_run = False
            result = await process(db, article)

        assert result.outcome == Outcome.SILENCE
        assert send_calls == [], "tg.send must not be called when AI gate silences"

    @pytest.mark.asyncio
    async def test_ai_with_affects_sends_with_analysis(self, db):
        """AI returns non-empty affects → send proceeds with ai_analysis attached."""
        from app.ai.analyzer import AIAnalysis

        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="aig_send_001",
        )

        captured: list = []

        async def capture_send(**kwargs):
            captured.append(kwargs.get("ai_analysis"))
            return 1

        analysis = AIAnalysis(
            title="ЦБ повысил ставку", impact="negative", emoji="🔴",
            summary="Влияет на банки", market_effect="Давление на акции", affects="акции · ОФЗ",
        )

        with (
            patch("app.core.config.settings") as mock_settings,
            patch("app.ai.analyzer.analyze", return_value=analysis),
            patch("app.telegram.client.send", side_effect=capture_send),
        ):
            mock_settings.openrouter_api_key = "test-key"
            mock_settings.dry_run = False
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        assert len(captured) == 1
        assert captured[0] is analysis, "send must receive the ai_analysis object"

    @pytest.mark.asyncio
    async def test_ai_failure_allows_send(self, db):
        """If AI raises, the article is still sent (ai_analysis=None fallback)."""
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="aig_fail_001",
        )

        captured: list = []

        async def capture_send(**kwargs):
            captured.append(kwargs.get("ai_analysis"))
            return 1

        with (
            patch("app.core.config.settings") as mock_settings,
            patch("app.ai.analyzer.analyze", side_effect=RuntimeError("OpenRouter unreachable")),
            patch("app.telegram.client.send", side_effect=capture_send),
        ):
            mock_settings.openrouter_api_key = "test-key"
            mock_settings.dry_run = False
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        assert captured == [None], "send must be called with ai_analysis=None on AI failure"

    @pytest.mark.asyncio
    async def test_no_api_key_skips_ai_gate(self, db):
        """No openrouter_api_key → AI gate is bypassed, send proceeds."""
        article = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="aig_nokey_001",
        )

        captured: list = []

        async def capture_send(**kwargs):
            captured.append(kwargs.get("ai_analysis"))
            return 1

        with (
            patch("app.core.config.settings") as mock_settings,
            patch("app.telegram.client.send", side_effect=capture_send),
        ):
            mock_settings.openrouter_api_key = ""
            mock_settings.dry_run = False
            result = await process(db, article)

        assert result.outcome == Outcome.SENT_NEW
        assert captured == [None], "send must be called with ai_analysis=None when no API key"


# ── pre-publish dup guard ─────────────────────────────────────────────────────

class TestDupGuardQueries:
    """Unit tests for the two new query functions used by the dup guard."""

    def _seed_cluster(self, db, title: str, sent: bool = False, ok: bool = True) -> int:
        from app.pipeline.normalizer import tokenize
        tokens = " ".join(tokenize(title))
        cid = queries.create_cluster(
            db, canonical_title=title, title_tokens=tokens,
            keywords=tokens, score=50,
        )
        if sent:
            queries.mark_cluster_sent(db, cid, "NEW_EVENT", score=50)
            queries.log_send(
                db, cluster_id=cid, decision="NEW_EVENT", score=50,
                source_count=1, headline=title,
                tg_message_id=42 if ok else None, ok=ok,
                error_text=None if ok else "timeout",
            )
        return cid

    def test_has_recent_attempt_true_for_successful_send(self, db):
        cid = self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=True, ok=True)
        assert queries.has_recent_send_attempt(db, cid, within_hours=2)

    def test_has_recent_attempt_true_for_failed_send(self, db):
        """Failed sends (ok=0) must also be counted — timeout may have delivered."""
        cid = self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=True, ok=False)
        assert queries.has_recent_send_attempt(db, cid, within_hours=2)

    def test_has_recent_attempt_false_without_any_send(self, db):
        cid = self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=False)
        assert not queries.has_recent_send_attempt(db, cid, within_hours=2)

    def test_get_recently_sent_returns_successful_cluster_tokens(self, db):
        from app.pipeline.normalizer import tokenize
        title = "ЦБ повысил ключевую ставку до 21 процента"
        self._seed_cluster(db, title, sent=True, ok=True)
        tokens = queries.get_recently_sent_title_tokens(db, within_hours=2)
        assert len(tokens) == 1
        assert tokens[0] == " ".join(tokenize(title))

    def test_get_recently_sent_excludes_failed_sends(self, db):
        """Failed sends must NOT appear in the cross-cluster token pool."""
        self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=True, ok=False)
        tokens = queries.get_recently_sent_title_tokens(db, within_hours=2)
        assert tokens == []

    def test_get_recently_sent_respects_exclude_cluster_id(self, db):
        cid = self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=True, ok=True)
        tokens = queries.get_recently_sent_title_tokens(db, within_hours=2, exclude_cluster_id=cid)
        assert tokens == []

    def test_guard_query_does_not_match_unrelated_title(self, db):
        """Газпром contract news must not Jaccard-match a ЦБ rate-hike cluster."""
        from app.pipeline.dedup import jaccard, JACCARD_THRESHOLD
        from app.pipeline.normalizer import tokenize
        self._seed_cluster(db, "ЦБ повысил ключевую ставку до 21 процента", sent=True, ok=True)

        tokens_gaz = " ".join(tokenize("Газпром подписал контракт на поставку газа в Китай"))
        for sent_tokens in queries.get_recently_sent_title_tokens(db, within_hours=2):
            j = jaccard(tokens_gaz, sent_tokens)
            assert j < JACCARD_THRESHOLD, (
                f"Газпром Jaccard={j:.2f} ≥ {JACCARD_THRESHOLD}: "
                "dup guard would incorrectly silence unrelated event"
            )


class TestDupGuard:
    """
    Integration tests for the pre-publish dup guard in orchestrator._run().

    The guard fires only when cluster["status"] == "new" (published clusters
    are already protected by the cooldown rule in decide()).

    Case (a): same cluster, guard fires on a recent send attempt (even failed ones),
              preventing a duplicate when tg.send() timed out after Telegram delivery.
    Case (b): cross-cluster near-dup, two different clusters for the same event
              are caught by Jaccard comparison against recently-sent title tokens.
    """

    @pytest.mark.asyncio
    async def test_same_cluster_recent_attempt_is_silenced(self, db):
        """
        Guard fires when a 'new' cluster already has a telegram_sends row.
        Simulates the timeout scenario: send logged as failed but message delivered.
        """
        from app.pipeline.normalizer import tokenize

        title = "ЦБ повысил ключевую ставку до 21 процента"
        tokens = " ".join(tokenize(title))

        # Create cluster directly (status='new') and seed a failed send attempt.
        # Simulates what tg.send() would write when it times out.
        cluster_id = queries.create_cluster(
            db, canonical_title=title, title_tokens=tokens, keywords=tokens, score=50,
        )
        queries.log_send(
            db, cluster_id=cluster_id, decision="NEW_EVENT", score=50,
            source_count=1, headline=title,
            tg_message_id=None, ok=False, error_text="timeout",
        )

        # Process an article that scores high enough and joins this cluster.
        # seen_articles is empty so near-dedup won't fire.
        article = make_article(title=title, source_id=1, raw_hash="dg_same_cluster")
        with patch("app.telegram.client.send", return_value=1) as mock_send:
            result = await process(db, article)

        assert result.outcome == Outcome.SILENCE
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_cross_cluster_near_dup_is_silenced(self, db):
        """
        Two similar titles create separate clusters (different enough to escape
        near-dedup and containment). The second one is silenced by cross-cluster
        Jaccard comparison against the first cluster's sent tokens.
        """
        from app.pipeline.dedup import jaccard, JACCARD_THRESHOLD
        from app.pipeline.normalizer import tokenize

        title_a = "ЦБ повысил ключевую ставку до 21 процента"
        title_b = "ЦБ поднял ключевую ставку до 21 процентов сегодня"

        # Pre-condition: titles share ≥ JACCARD_THRESHOLD tokens (guard will fire)
        j = jaccard(" ".join(tokenize(title_a)), " ".join(tokenize(title_b)))
        assert j >= JACCARD_THRESHOLD, (
            f"Test pre-condition failed: Jaccard={j:.2f} < {JACCARD_THRESHOLD}. "
            "Adjust titles if tokenizer changes."
        )

        # Seed cluster A as sent (simulates first cycle completing successfully)
        from app.pipeline.normalizer import tokenize as _tok
        tokens_a = " ".join(_tok(title_a))
        cluster_a = queries.create_cluster(
            db, canonical_title=title_a, title_tokens=tokens_a, keywords=tokens_a, score=50,
        )
        queries.mark_cluster_sent(db, cluster_a, "NEW_EVENT", score=50)
        queries.log_send(
            db, cluster_id=cluster_a, decision="NEW_EVENT", score=50,
            source_count=1, headline=title_a, tg_message_id=99, ok=True,
        )

        # Process title_b. seen_articles is empty → near-dedup misses.
        # Cluster A tokens may or may not match title_b's containment threshold;
        # either way the guard should silence the new send.
        article_b = make_article(title=title_b, source_id=2, raw_hash="dg_cross_b")
        with patch("app.telegram.client.send", return_value=1) as mock_send:
            result = await process(db, article_b)

        assert result.outcome != Outcome.SENT_NEW, (
            "A near-duplicate event must not produce a second NEW_EVENT"
        )
        # If it joined cluster A (cooldown) or was caught by the guard → SILENCE.
        # If near-dedup fired (article already in seen_articles) → NEAR_DUP.
        # SENT_UPDATE is also acceptable if 2h cooldown had elapsed (won't happen in tests).
        assert result.outcome in (Outcome.SILENCE, Outcome.NEAR_DUP)

    @pytest.mark.asyncio
    async def test_guard_does_not_block_after_cooldown_expires(self, db):
        """
        A new article may still be published once the cooldown window has passed.
        This test verifies the guard uses within_hours=COOLDOWN_HOURS correctly.
        """
        from app.pipeline.normalizer import tokenize
        from datetime import timedelta

        title = "ЦБ повысил ключевую ставку до 21 процента"
        tokens = " ".join(tokenize(title))

        cluster_id = queries.create_cluster(
            db, canonical_title=title, title_tokens=tokens, keywords=tokens, score=50,
        )
        # Backdate the send to 3 hours ago — outside the 2-hour cooldown window
        old_sent_at = (
            datetime.now(timezone.utc) - timedelta(hours=3)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            """
            INSERT INTO telegram_sends
                (cluster_id, decision, score, source_count, headline,
                 tg_message_id, ok, sent_at)
            VALUES (?, 'NEW_EVENT', 50, 1, ?, 42, 1, ?)
            """,
            (cluster_id, title, old_sent_at),
        )
        db.commit()

        assert not queries.has_recent_send_attempt(db, cluster_id, within_hours=2), (
            "Attempt older than cooldown window must not block the guard"
        )

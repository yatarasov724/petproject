"""
Tests for app.pipeline.publish_decision.

All 6 decision rules are tested in isolation.
Cluster state is constructed as a plain dict and passed through sqlite3.Row
via an in-memory query so the code receives the exact same type it sees in production.

Covers:
  Rule 1 — score below threshold → SILENCE
  Rule 2 — status == 'new' → NEW_EVENT
  Rule 3 — cooldown active → SILENCE
  Rule 4 — source_count >= UPDATE_SOURCE_FLOOR → UPDATE
  Rule 5 — score delta >= UPDATE_SCORE_DELTA → UPDATE
  Rule 6 — no condition met → SILENCE
  Edge cases: cooldown exactly expired, cooldown_until=None, published_score=None
"""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.pipeline.publish_decision import (
    decide,
    Decision,
    COOLDOWN_HOURS,
    UPDATE_SCORE_DELTA,
    UPDATE_SOURCE_FLOOR,
    FRESHNESS_HOURS,
)
from app.pipeline.scorer import compute_score, PUBLISH_THRESHOLD
from app.db import queries


# ── fixture helpers ───────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_cluster_row(db, **overrides):
    """
    Insert a cluster into the in-memory DB with given field values, then
    fetch it back as a real sqlite3.Row — exactly what production code uses.
    """
    defaults = {
        "canonical_title": "ЦБ повысил ключевую ставку",
        "title_tokens": "ключевую повысил ставку цб",
        "keywords": "ключевую повысил ставку цб",
        "best_score": 55,
        "source_count": 1,
        "article_count": 1,
        "status": "new",
        "first_seen_at": _iso(_utcnow()),
        "last_updated_at": _iso(_utcnow()),
        "last_sent_at": None,
        "cooldown_until": None,
        "published_score": None,
    }
    defaults.update(overrides)

    cur = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             article_count, status, first_seen_at, last_updated_at,
             last_sent_at, cooldown_until, published_score)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            defaults["canonical_title"], defaults["title_tokens"],
            defaults["keywords"], defaults["best_score"], defaults["source_count"],
            defaults["article_count"], defaults["status"],
            defaults["first_seen_at"], defaults["last_updated_at"],
            defaults["last_sent_at"], defaults["cooldown_until"],
            defaults["published_score"],
        ),
    )
    db.commit()
    return queries.get_cluster(db, cur.lastrowid)


def _score(score_value: int):
    """
    Build a ScoreResult with a fixed score by scaling a real headline.
    For simplicity we call compute_score with a known-scoring headline,
    then monkey-patch the score field.
    We can't easily set an arbitrary score directly, so we create a
    ScoreResult dataclass directly.
    """
    from app.pipeline.scorer import ScoreResult, EventType
    return ScoreResult(
        score=score_value,
        tier="tier2" if score_value >= 25 else "tier3",
        event_type=EventType.RATE_DECISION,
        base_score=score_value,
        keyword_bonus=0,
        source_bonus=0,
        type_bonus=0,
        matched_keywords=["ключевую ставку"],
    )


# ── tests ─────────────────────────────────────────────────────────────────────

class TestRule1_NoiseFloor:
    def test_score_below_threshold_silenced(self, db):
        cluster = _make_cluster_row(db, status="new")
        result = decide(cluster, _score(PUBLISH_THRESHOLD - 1))
        assert result.decision == Decision.SILENCE

    def test_score_zero_silenced(self, db):
        cluster = _make_cluster_row(db, status="new")
        result = decide(cluster, _score(0))
        assert result.decision == Decision.SILENCE

    def test_score_exactly_threshold_not_silenced(self, db):
        cluster = _make_cluster_row(db, status="new")
        result = decide(cluster, _score(PUBLISH_THRESHOLD))
        assert result.decision != Decision.SILENCE


class TestRule2_Freshness:
    """Unpublished clusters older than FRESHNESS_HOURS are silenced as stale."""

    def test_fresh_new_cluster_not_silenced(self, db):
        cluster = _make_cluster_row(db, status="new", first_seen_at=_iso(_utcnow()))
        result = decide(cluster, _score(50))
        assert result.decision == Decision.NEW_EVENT

    def test_stale_unpublished_cluster_silenced(self, db):
        old_time = _utcnow() - timedelta(hours=FRESHNESS_HOURS + 1)
        cluster = _make_cluster_row(db, status="new", first_seen_at=_iso(old_time))
        result = decide(cluster, _score(50))
        assert result.decision == Decision.SILENCE
        assert "stale" in result.reason.lower()

    def test_stale_already_published_cluster_can_update(self, db):
        # Published clusters should NOT be blocked by the freshness rule —
        # they were fresh when first published, and can still receive UPDATEs.
        old_time = _utcnow() - timedelta(hours=FRESHNESS_HOURS + 1)
        cluster = _make_cluster_row(
            db,
            status="published",
            first_seen_at=_iso(old_time),
            cooldown_until=None,
            published_score=30,
            source_count=UPDATE_SOURCE_FLOOR,
        )
        result = decide(cluster, _score(50))
        assert result.decision == Decision.UPDATE


class TestRule3_NewEvent:
    def test_new_cluster_high_score_is_new_event(self, db):
        cluster = _make_cluster_row(db, status="new")
        result = decide(cluster, _score(50))
        assert result.decision == Decision.NEW_EVENT

    def test_non_new_cluster_is_not_new_event(self, db):
        cluster = _make_cluster_row(db, status="published", published_score=50, source_count=1)
        result = decide(cluster, _score(55))
        # Must not be NEW_EVENT (it's already published)
        assert result.decision != Decision.NEW_EVENT


class TestRule3_Cooldown:
    def test_active_cooldown_silences(self, db):
        future = _utcnow() + timedelta(hours=1)
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=_iso(future),
            published_score=40,
            source_count=5,  # rule 4 would fire without cooldown
        )
        result = decide(cluster, _score(60))
        assert result.decision == Decision.SILENCE
        assert "cooldown" in result.reason.lower()

    def test_expired_cooldown_not_silenced(self, db):
        past = _utcnow() - timedelta(seconds=1)
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=_iso(past),
            published_score=30,
            source_count=UPDATE_SOURCE_FLOOR,
        )
        result = decide(cluster, _score(50))
        assert result.decision == Decision.UPDATE

    def test_null_cooldown_not_silenced(self, db):
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=30,
            source_count=UPDATE_SOURCE_FLOOR,
        )
        result = decide(cluster, _score(50))
        assert result.decision == Decision.UPDATE


class TestRule4_CrossSourceConfirmation:
    def test_source_floor_triggers_update(self, db):
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=30,
            source_count=UPDATE_SOURCE_FLOOR,
        )
        result = decide(cluster, _score(35))
        assert result.decision == Decision.UPDATE
        assert str(UPDATE_SOURCE_FLOOR) in result.reason

    def test_below_source_floor_no_update_from_rule4(self, db):
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=30,
            source_count=UPDATE_SOURCE_FLOOR - 1,
        )
        # Small delta — rule 5 won't fire either
        result = decide(cluster, _score(32))
        assert result.decision == Decision.SILENCE


class TestRule5_ScoreDelta:
    def test_large_delta_triggers_update(self, db):
        published_score = 30
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=published_score,
            source_count=1,   # rule 4 won't fire
        )
        new_score = published_score + UPDATE_SCORE_DELTA
        result = decide(cluster, _score(new_score))
        assert result.decision == Decision.UPDATE
        assert "→" in result.reason or "score" in result.reason.lower()

    def test_small_delta_silenced(self, db):
        published_score = 40
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=published_score,
            source_count=1,
        )
        result = decide(cluster, _score(published_score + UPDATE_SCORE_DELTA - 1))
        assert result.decision == Decision.SILENCE

    def test_null_published_score_treated_as_zero(self, db):
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=None,  # NULL in DB → treated as 0
            source_count=1,
        )
        # Score must exceed PUBLISH_THRESHOLD (rule 1) AND delta >= UPDATE_SCORE_DELTA (rule 5)
        # published_score=None → 0; new score = PUBLISH_THRESHOLD + UPDATE_SCORE_DELTA
        result = decide(cluster, _score(PUBLISH_THRESHOLD + UPDATE_SCORE_DELTA))
        assert result.decision == Decision.UPDATE


class TestRule6_NothingFired:
    def test_stalled_event_silenced(self, db):
        cluster = _make_cluster_row(
            db,
            status="published",
            cooldown_until=None,
            published_score=40,
            source_count=1,   # rule 4 won't fire
        )
        # small delta — rule 5 won't fire
        result = decide(cluster, _score(42))
        assert result.decision == Decision.SILENCE

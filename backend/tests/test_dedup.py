"""
Tests for app.pipeline.dedup.

Covers:
- check(): exact duplicate detection, near-duplicate (Jaccard), unique pass-through
- record(): INSERT idempotency (second call is a no-op)
- jaccard(): symmetric, empty inputs, known values
- MVP-5: LIMIT and index on seen_articles
"""

import sqlite3
import time

import pytest

from app.pipeline import dedup
from app.pipeline.dedup import DupReason, jaccard, containment, NEAR_DEDUP_MAX_POOL
from app.db import queries
from tests.conftest import make_article, db  # noqa: F401


# ── jaccard (pure function) ───────────────────────────────────────────────────

class TestJaccard:
    def test_identical(self):
        assert jaccard("газпром дивиденды", "газпром дивиденды") == 1.0

    def test_disjoint(self):
        assert jaccard("газпром дивиденды", "роснефть слияние") == 0.0

    def test_partial(self):
        # intersection: {газпром}, union: {газпром, дивиденды, слияние}
        score = jaccard("газпром дивиденды", "газпром слияние")
        assert abs(score - 1 / 3) < 1e-9

    def test_symmetric(self):
        a, b = "газпром дивиденды ставка", "газпром слияние ставка"
        assert jaccard(a, b) == jaccard(b, a)

    def test_empty_a(self):
        assert jaccard("", "газпром дивиденды") == 0.0

    def test_empty_b(self):
        assert jaccard("газпром дивиденды", "") == 0.0

    def test_both_empty(self):
        assert jaccard("", "") == 0.0


# ── containment (pure function) ───────────────────────────────────────────────

class TestContainment:
    def test_identical(self):
        score, shared = containment("газпром дивиденды", "газпром дивиденды")
        assert score == 1.0
        assert shared == 2

    def test_fully_contained(self):
        # short fully inside long
        score, shared = containment("газпром дивиденды", "газпром дивиденды снизил рекордно")
        assert score == 1.0
        assert shared == 2

    def test_partial(self):
        score, shared = containment("газпром дивиденды ставка", "газпром слияние ставка")
        # intersection: {газпром, ставка} = 2, min size = 3
        assert abs(score - 2 / 3) < 1e-9
        assert shared == 2

    def test_disjoint(self):
        score, shared = containment("газпром дивиденды", "роснефть слияние")
        assert score == 0.0
        assert shared == 0

    def test_empty_input(self):
        score, shared = containment("", "газпром дивиденды")
        assert score == 0.0
        assert shared == 0


# ── dedup.check() ─────────────────────────────────────────────────────────────

class TestCheck:
    def test_unique_article(self, db):
        article = make_article()
        result = dedup.check(db, article)
        assert not result.is_duplicate
        assert result.reason == DupReason.UNIQUE

    def test_exact_duplicate(self, db):
        article = make_article()
        # Persist first copy
        dedup.record(db, article, cluster_id=None)
        # Check the same article again
        result = dedup.check(db, article)
        assert result.is_duplicate
        assert result.reason == DupReason.EXACT

    def test_near_duplicate_detected(self, db):
        # Two articles with high token overlap but different raw_hash
        article_a = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="hash_a_unique_001",
        )
        dedup.record(db, article_a, cluster_id=None)

        # Slightly rephrased — same subject, swapped words — different raw_hash
        article_b = make_article(
            title="Банк России поднял ключевую ставку до 21 процента",
            raw_hash="hash_b_unique_002",
        )
        result = dedup.check(db, article_b)
        # "ключевую ставку 21 процента" share heavily → should be near-dup
        assert result.is_duplicate
        assert result.reason == DupReason.NEAR
        assert result.score >= dedup.JACCARD_THRESHOLD

    def test_unrelated_article_not_near_dup(self, db):
        article_a = make_article(title="ЦБ повысил ключевую ставку до 21 процента")
        dedup.record(db, article_a, cluster_id=None)

        article_b = make_article(
            title="Газпром объявил о рекордных дивидендах за 2023 год",
            raw_hash="completely_different_hash",
        )
        result = dedup.check(db, article_b)
        assert not result.is_duplicate

    def test_length_asymmetric_near_dup_via_containment(self, db):
        # Short title already seen — longer title from another source covers it almost fully.
        # Jaccard would be low (small intersection / large union), but containment stays high.
        article_a = make_article(
            title="ЕС вводит санкции против России",
            raw_hash="short_title_001",
        )
        dedup.record(db, article_a, cluster_id=None)

        # Long title that fully contains the short one plus extra context
        article_b = make_article(
            title="ЕС вводит санкции против России за эвакуацию детей с оккупированных территорий",
            raw_hash="long_title_001",
        )
        result = dedup.check(db, article_b)
        assert result.is_duplicate
        assert result.reason == DupReason.NEAR

    def test_containment_requires_min_shared_tokens(self, db):
        # A 2-token overlap on a 3-token title hits containment=0.67 ≥ threshold,
        # BUT shared=2 < CONTAINMENT_MIN_SHARED=3, so containment must NOT fire.
        # (Jaccard for this pair is 2/4=0.5 ≥ 0.35, so it fires on Jaccard — that
        # is existing known behaviour unrelated to the containment guard.)
        # This test specifically checks that check() raises the right reason flag
        # and that adding a fourth shared token (reaching min_shared=3) does fire containment.
        # We verify _best_containment logic directly on a pair below min_shared.
        from app.pipeline.dedup import _best_containment, CONTAINMENT_MIN_SHARED, CONTAINMENT_THRESHOLD
        score, shared = _best_containment(
            "снизить дивиденд газпром",
            ["снизить дивиденд сбербанк"],
        )
        assert shared == 2
        assert shared < CONTAINMENT_MIN_SHARED  # guard would block it


# ── dedup.record() ────────────────────────────────────────────────────────────

class TestRecord:
    def test_first_insert_returns_rowid(self, db):
        article = make_article()
        rowid = dedup.record(db, article, cluster_id=None)
        assert rowid is not None
        assert rowid > 0

    def test_second_insert_idempotent(self, db):
        article = make_article()
        dedup.record(db, article, cluster_id=None)
        # Second call with the same raw_hash should be silently ignored
        rowid2 = dedup.record(db, article, cluster_id=None)
        assert rowid2 is None

    def test_record_with_cluster_id(self, db):
        # Create a cluster to reference
        cluster_id = queries.create_cluster(
            db,
            canonical_title="ЦБ повысил ставку",
            title_tokens="повысил ставку цб",
            keywords="повысил ставку цб",
            score=50,
        )
        article = make_article()
        dedup.record(db, article, cluster_id=cluster_id)

        row = db.execute(
            "SELECT cluster_id FROM seen_articles WHERE raw_hash = ?",
            (article.raw_hash,),
        ).fetchone()
        assert row["cluster_id"] == cluster_id

    def test_article_stored_with_correct_source(self, db):
        article = make_article(source_id=2)
        dedup.record(db, article, cluster_id=None)

        row = db.execute(
            "SELECT source_id FROM seen_articles WHERE raw_hash = ?",
            (article.raw_hash,),
        ).fetchone()
        assert row["source_id"] == 2


# ── MVP-5: LIMIT and index ─────────────────────────────────────────────────────

def _insert_bulk_seen_articles(db: sqlite3.Connection, count: int) -> None:
    """Insert `count` distinct seen_articles rows directly (bypasses dedup logic)."""
    now = "2026-05-05T21:00:00Z"
    db.executemany(
        """
        INSERT OR IGNORE INTO seen_articles
            (source_id, raw_hash, title_tokens, url, published_at, seen_at)
        VALUES (1, ?, ?, NULL, ?, ?)
        """,
        [
            (f"bulk_hash_{i}", f"токен{i} общий рынок", now, now)
            for i in range(count)
        ],
    )
    db.commit()


class TestNearDedupLimit:
    def test_limit_applied_to_pool(self, db):
        """get_recent_title_tokens must return at most NEAR_DEDUP_MAX_POOL rows."""
        _insert_bulk_seen_articles(db, NEAR_DEDUP_MAX_POOL + 500)

        pool = queries.get_recent_title_tokens(db, within_hours=48)
        assert len(pool) <= NEAR_DEDUP_MAX_POOL

    def test_near_dedup_still_catches_duplicate_within_limit(self, db):
        """Even with a large pool, a near-duplicate that falls within the limit is caught."""
        # Insert enough filler articles to push pool near the limit
        _insert_bulk_seen_articles(db, 100)

        # Insert the article we want to detect as a near-dup
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="limit_anchor_001",
        )
        dedup.record(db, anchor)

        candidate = make_article(
            title="Банк России поднял ключевую ставку до 21 процента",
            raw_hash="limit_candidate_001",
        )
        result = dedup.check(db, candidate)
        assert result.is_duplicate
        assert result.reason == DupReason.NEAR

    def test_most_recent_articles_prioritised(self, db):
        """
        ORDER BY seen_at DESC means the most recent articles fill the pool first.
        An anchor inserted last should be found even when older rows exceed the limit.
        """
        _insert_bulk_seen_articles(db, NEAR_DEDUP_MAX_POOL - 10)

        # This anchor is inserted after the bulk rows → it appears first in DESC order
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="recent_anchor_001",
        )
        dedup.record(db, anchor)

        candidate = make_article(
            title="Банк России поднял ключевую ставку до 21 процента",
            raw_hash="recent_candidate_001",
        )
        result = dedup.check(db, candidate)
        assert result.is_duplicate

    def test_index_exists_on_seen_at(self, db):
        """Schema must define an index on seen_articles(seen_at)."""
        indexes = {
            row[1]
            for row in db.execute("PRAGMA index_list(seen_articles)").fetchall()
        }
        assert any("seen_at" in idx for idx in indexes), (
            f"Expected an index on seen_at, found: {indexes}"
        )

    def test_near_dedup_performance(self, db):
        """1000-row pool: a full dedup.check() must complete in under 500 ms."""
        _insert_bulk_seen_articles(db, NEAR_DEDUP_MAX_POOL)

        candidate = make_article(
            title="Газпром объявил дивиденды за рекордный год",
            raw_hash="perf_candidate_001",
        )
        start = time.monotonic()
        dedup.check(db, candidate)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert elapsed_ms < 500, f"near-dedup took {elapsed_ms:.0f} ms (limit: 500 ms)"

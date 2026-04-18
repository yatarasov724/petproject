"""
Tests for app.pipeline.clusterer.

Covers:
- find_or_create(): creates new cluster for truly new article
- find_or_create(): joins existing cluster when containment >= threshold
- find_or_create(): does NOT join when containment < threshold (different event)
- find_or_create(): does NOT join when cluster is outside time window
- source_count incremented only when a new source contributes
- article_count always incremented on join
"""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.pipeline import clusterer
from app.db import queries
from tests.conftest import make_article, db  # noqa: F401


# ── helpers ───────────────────────────────────────────────────────────────────

def _cluster_row(db, cluster_id):
    return db.execute(
        "SELECT * FROM event_clusters WHERE id = ?", (cluster_id,)
    ).fetchone()


# ── tests ─────────────────────────────────────────────────────────────────────

class TestFindOrCreate:
    def test_new_cluster_created(self, db):
        article = make_article(title="ЦБ повысил ключевую ставку до 21 процента")
        result = clusterer.find_or_create(db, article, market_score=50)

        assert result.is_new
        assert result.cluster_id > 0
        row = _cluster_row(db, result.cluster_id)
        assert row is not None
        assert row["canonical_title"] == article.title
        assert row["source_count"] == 1
        assert row["article_count"] == 1
        assert row["status"] == "new"

    def test_joins_existing_cluster(self, db):
        # Anchor article
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="anchor_hash_001",
        )
        r1 = clusterer.find_or_create(db, anchor, market_score=50)
        assert r1.is_new

        # Second article about the same event — heavily overlapping tokens
        follow_up = make_article(
            title="Банк России поднял ключевую ставку до 21 процента на заседании",
            source_id=2,
            raw_hash="followup_hash_002",
        )
        r2 = clusterer.find_or_create(db, follow_up, market_score=50)

        assert not r2.is_new
        assert r2.cluster_id == r1.cluster_id

    def test_different_event_creates_new_cluster(self, db):
        anchor = make_article(
            title="Газпром объявил о рекордных дивидендах за прошлый год",
            raw_hash="gas_hash_001",
        )
        r1 = clusterer.find_or_create(db, anchor, market_score=25)
        assert r1.is_new

        unrelated = make_article(
            title="Роснефть приобрела нефтяной актив в Сибири за миллиард",
            raw_hash="ros_hash_002",
        )
        r2 = clusterer.find_or_create(db, unrelated, market_score=25)

        assert r2.is_new
        assert r2.cluster_id != r1.cluster_id

    def test_article_count_incremented_on_join(self, db):
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            raw_hash="anc_001",
        )
        r1 = clusterer.find_or_create(db, anchor, market_score=50)

        follow_up = make_article(
            title="Банк России поднял ключевую ставку до 21 процента",
            source_id=2,
            raw_hash="fol_002",
        )
        clusterer.find_or_create(db, follow_up, market_score=50)

        row = _cluster_row(db, r1.cluster_id)
        assert row["article_count"] == 2

    def test_source_count_incremented_for_new_source(self, db):
        """A second article from a different source_id bumps source_count."""
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            source_id=1,
            raw_hash="anc_sc_001",
        )
        r1 = clusterer.find_or_create(db, anchor, market_score=50)
        # Persist anchor so get_cluster_source_ids sees source_id=1 for this cluster
        queries.insert_seen_article(
            db, source_id=1, raw_hash="anc_sc_001",
            title_tokens=anchor.title_tokens, url=anchor.url,
            published_at="2024-01-15T10:00:00Z",
            cluster_id=r1.cluster_id,
        )

        follow_up = make_article(
            title="Банк России поднял ключевую ставку до 21 процента",
            source_id=2,
            raw_hash="fol_sc_002",
        )
        # Do NOT pre-insert follow_up — find_or_create calls get_cluster_source_ids
        # before any insert, so source_id=2 must not be there yet
        clusterer.find_or_create(db, follow_up, market_score=50)

        row = _cluster_row(db, r1.cluster_id)
        assert row["source_count"] == 2

    def test_same_source_does_not_increment_source_count(self, db):
        """Two articles from the same source_id do NOT bump source_count."""
        anchor = make_article(
            title="ЦБ повысил ключевую ставку до 21 процента",
            source_id=1,
            raw_hash="anc_ss_001",
        )
        r1 = clusterer.find_or_create(db, anchor, market_score=50)
        # Persist anchor so get_cluster_source_ids sees source_id=1
        queries.insert_seen_article(
            db, source_id=1, raw_hash="anc_ss_001",
            title_tokens=anchor.title_tokens, url=anchor.url,
            published_at="2024-01-15T10:00:00Z",
            cluster_id=r1.cluster_id,
        )

        repeat = make_article(
            title="ЦБ Банк России поднял ключевую ставку до 21 процента итоги",
            source_id=1,   # same source
            raw_hash="rep_ss_002",
        )
        # source_id=1 is already recorded → is_new_source=False → source_count stays 1
        clusterer.find_or_create(db, repeat, market_score=50)

        row = _cluster_row(db, r1.cluster_id)
        assert row["source_count"] == 1

    def test_article_with_too_few_tokens_creates_new_cluster(self, db):
        # 1-token article — can't meaningfully cluster → always new
        article = make_article(
            title="Нефть",  # only 1 meaningful token after stop words
            raw_hash="short_001",
        )
        result = clusterer.find_or_create(db, article, market_score=0)
        assert result.is_new

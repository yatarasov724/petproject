"""
Tests for the source authority tier guard.

The guard silences tier-2 clusters when a tier-1 source published
a similar story (cosine >= 0.70, or Jaccard >= 0.35 fallback) within
SOURCE_AUTH_MINUTES (60 min).
"""
import struct
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock

import pytest

from app.db import queries
from app.pipeline.orchestrator import process, Outcome, SOURCE_AUTH_MINUTES
from app.pipeline.source_tiers import get_tier, TIER_1_SOURCES
from tests.conftest import make_article, db  # noqa: F401


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _unit_vec() -> bytes:
    vec = [1.0] + [0.0] * 383
    return struct.pack(f"{len(vec)}f", *vec)


def _orthogonal_vec() -> bytes:
    vec = [0.0, 1.0] + [0.0] * 382
    return struct.pack(f"{len(vec)}f", *vec)


def _ensure_source(db, name: str) -> int:
    """Insert source if it doesn't exist; return its id."""
    row = db.execute(
        "SELECT id FROM rss_sources WHERE name = %s", (name,)
    ).fetchone()
    if row:
        return row["id"]
    row = db.execute(
        "INSERT INTO rss_sources (name, url) VALUES (%s, %s) RETURNING id",
        (name, f"http://{name.lower()}.local/rss"),
    ).fetchone()
    db.commit()
    return row["id"]


def _insert_tier1_send(db, *, title: str, title_tokens: str, minutes_ago: int,
                       source_name: str = "TASS", embedding: bytes | None = None) -> int:
    """Insert a cluster + seen_article (from named source) + successful send."""
    source_id = _ensure_source(db, source_name)
    sent_at = _iso(_now() - timedelta(minutes=minutes_ago))
    row = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             status, last_sent_at, first_seen_at, embedding)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (title, title_tokens, title_tokens, 70, 1, "published", sent_at, sent_at, embedding),
    ).fetchone()
    cluster_id = row["id"]
    db.execute(
        """
        INSERT INTO seen_articles
            (source_id, raw_hash, title_tokens, url, published_at, seen_at, cluster_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (source_id, f"hash_{cluster_id}", title_tokens,
         f"http://t.local/{cluster_id}", sent_at, sent_at, cluster_id),
    )
    db.execute(
        """
        INSERT INTO telegram_sends
            (cluster_id, decision, score, source_count, headline, ok, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (cluster_id, "NEW_EVENT", 70, 1, title, 1, sent_at),
    )
    db.commit()
    return cluster_id


# ── unit: get_tier ────────────────────────────────────────────────────────────

class TestGetTier:
    def test_tass_is_tier1(self):        assert get_tier("TASS") == 1
    def test_interfax_is_tier1(self):    assert get_tier("Interfax") == 1
    def test_prime_is_tier1(self):       assert get_tier("Prime") == 1
    def test_ria_is_tier1(self):         assert get_tier("RIA") == 1
    def test_tg_moexnews_is_tier1(self): assert get_tier("TG:moexnews") == 1
    def test_tg_cbrstocks_is_tier1(self):assert get_tier("TG:cbrstocks") == 1
    def test_rbc_is_tier2(self):         assert get_tier("RBC") == 2
    def test_lenta_is_tier2(self):       assert get_tier("Lenta") == 2
    def test_tg_markettwits_is_tier2(self): assert get_tier("TG:markettwits") == 2
    def test_unknown_source_is_tier2(self): assert get_tier("SomeRandom") == 2


# ── unit: get_recent_tier1_clusters ──────────────────────────────────────────

class TestGetRecentTier1Clusters:
    def test_returns_tier1_cluster_within_window(self, db):
        _insert_tier1_send(
            db, title="ЦБ поднял ставку", title_tokens="цб поднять ставка",
            minutes_ago=30, source_name="TASS",
        )
        rows = queries.get_recent_tier1_clusters(
            db, within_minutes=60, exclude_cluster_id=9999,
            tier1_sources=TIER_1_SOURCES,
        )
        assert len(rows) == 1

    def test_excludes_cluster_outside_window(self, db):
        _insert_tier1_send(
            db, title="ЦБ поднял ставку", title_tokens="цб поднять ставка",
            minutes_ago=90, source_name="TASS",
        )
        rows = queries.get_recent_tier1_clusters(
            db, within_minutes=60, exclude_cluster_id=9999,
            tier1_sources=TIER_1_SOURCES,
        )
        assert len(rows) == 0

    def test_excludes_tier2_cluster(self, db):
        _insert_tier1_send(
            db, title="ЦБ поднял ставку", title_tokens="цб поднять ставка",
            minutes_ago=20, source_name="RBC",
        )
        rows = queries.get_recent_tier1_clusters(
            db, within_minutes=60, exclude_cluster_id=9999,
            tier1_sources=TIER_1_SOURCES,
        )
        assert len(rows) == 0

    def test_excludes_specified_cluster_id(self, db):
        cluster_id = _insert_tier1_send(
            db, title="ЦБ поднял ставку", title_tokens="цб поднять ставка",
            minutes_ago=10, source_name="Interfax",
        )
        rows = queries.get_recent_tier1_clusters(
            db, within_minutes=60, exclude_cluster_id=cluster_id,
            tier1_sources=TIER_1_SOURCES,
        )
        assert len(rows) == 0


# ── integration: orchestrator ─────────────────────────────────────────────────

@pytest.mark.asyncio
class TestSourceAuthOrchestrator:
    """
    Tests mock get_recent_tier1_clusters directly (same pattern as TestBurstGuardOrchestrator)
    to isolate the source authority guard without fighting dedup/clustering side-effects.
    get_recent_burst_clusters is mocked to [] so burst guard doesn't fire first.
    """

    async def test_tier2_silenced_when_tier1_row_returned(self, db):
        """Tier-2 article silenced when get_recent_tier1_clusters returns a similar cluster."""
        emb = _unit_vec()
        fake_tier1_row = {"title_tokens": "цб поднять ставка 21", "embedding": emb}
        article = make_article(
            source_name="RBC",
            title="Центробанк повысил ключевую ставку",
        )
        with (
            patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[]),
            patch("app.pipeline.orchestrator.queries.get_recent_tier1_clusters", return_value=[fake_tier1_row]),
            patch("app.ai.embedder.embed", return_value=emb),
            patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42),
        ):
            result = await process(db, article)
        assert result.outcome == Outcome.SILENCE

    async def test_tier1_not_silenced_by_source_auth(self, db):
        """Tier-1 article is never handed to source auth guard — guard is skipped for tier-1."""
        emb = _unit_vec()
        fake_tier1_row = {"title_tokens": "цб поднять ставка", "embedding": emb}
        article = make_article(
            source_name="Interfax",
            title="ЦБ повысил ставку",
        )
        with (
            patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[]),
            patch("app.pipeline.orchestrator.queries.get_recent_tier1_clusters", return_value=[fake_tier1_row]) as mock_sa,
            patch("app.ai.embedder.embed", return_value=emb),
            patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42),
        ):
            result = await process(db, article)
        # Source auth query must NOT be called for tier-1 sources
        mock_sa.assert_not_called()
        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

    async def test_tier2_passes_when_tier1_returns_empty(self, db):
        """Tier-2 article passes when no recent tier-1 cluster found (outside window)."""
        emb = _unit_vec()
        article = make_article(
            source_name="RBC",
            title="Центробанк повысил ключевую ставку",
        )
        with (
            patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[]),
            patch("app.pipeline.orchestrator.queries.get_recent_tier1_clusters", return_value=[]),
            patch("app.ai.embedder.embed", return_value=emb),
            patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42),
        ):
            result = await process(db, article)
        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

    async def test_tier2_passes_when_similarity_below_threshold(self, db):
        """Tier-2 article passes when tier-1 cluster has low cosine similarity."""
        emb_article = _unit_vec()
        emb_tier1   = _orthogonal_vec()   # cosine(unit, orthogonal) = 0.0 < 0.70
        fake_tier1_row = {"title_tokens": "газпром дивиденд", "embedding": emb_tier1}
        article = make_article(
            source_name="RBC",
            title="Сбербанк нарастил прибыль",
        )
        with (
            patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[]),
            patch("app.pipeline.orchestrator.queries.get_recent_tier1_clusters", return_value=[fake_tier1_row]),
            patch("app.ai.embedder.embed", return_value=emb_article),
            patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42),
        ):
            result = await process(db, article)
        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

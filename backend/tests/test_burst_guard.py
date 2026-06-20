"""
Tests for the 30-minute burst guard in the orchestrator.

The burst guard silences a new cluster when a semantically similar cluster
was published within BURST_GUARD_MINUTES. Similarity is measured by cosine
(embedding path) or jaccard (fallback, no embeddings).

Covers:
- get_recent_burst_clusters returns rows within window
- get_recent_burst_clusters returns empty list outside window
- get_recent_burst_clusters excludes the specified cluster_id
- Orchestrator silences a new cluster when burst guard query returns a similar cluster
- Orchestrator does NOT silence when burst guard query returns nothing
"""

import struct
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock

import pytest

from app.db import queries
from app.pipeline.orchestrator import process, Outcome, BURST_GUARD_MINUTES
from tests.conftest import make_article, db  # noqa: F401


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _unit_vec_bytes() -> bytes:
    """Return a 384-dim unit vector as packed float32 bytes (cosine with itself = 1.0)."""
    vec = [1.0] + [0.0] * 383
    return struct.pack(f"{len(vec)}f", *vec)


def _orthogonal_vec_bytes() -> bytes:
    """Return a 384-dim unit vector orthogonal to _unit_vec_bytes (cosine = 0.0)."""
    vec = [0.0] + [1.0] + [0.0] * 382
    return struct.pack(f"{len(vec)}f", *vec)


def _insert_cluster_with_send(
    db, *, title: str, title_tokens: str, minutes_ago: int, embedding: bytes | None = None
) -> int:
    sent_at = _iso(_utcnow() - timedelta(minutes=minutes_ago))
    row = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             status, last_sent_at, first_seen_at, embedding)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (title, title_tokens, title_tokens, 65, 1, "published", sent_at, sent_at, embedding),
    ).fetchone()
    cluster_id = row["id"]
    db.execute(
        """
        INSERT INTO telegram_sends
            (cluster_id, decision, score, source_count, headline, ok, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (cluster_id, "NEW_EVENT", 65, 1, title, 1, sent_at),
    )
    db.commit()
    return cluster_id


# ── query unit tests ──────────────────────────────────────────────────────────

class TestGetRecentBurstClusters:
    def test_returns_row_within_window(self, db):
        _insert_cluster_with_send(
            db, title="ЦБ снизил ставку", title_tokens="снизить ставка цб",
            minutes_ago=10,
        )
        rows = queries.get_recent_burst_clusters(db, within_minutes=30, exclude_cluster_id=0)
        assert len(rows) == 1
        assert rows[0]["title_tokens"] == "снизить ставка цб"

    def test_excludes_row_outside_window(self, db):
        _insert_cluster_with_send(
            db, title="ЦБ снизил ставку", title_tokens="снизить ставка цб",
            minutes_ago=BURST_GUARD_MINUTES + 10,  # 40 min ago
        )
        rows = queries.get_recent_burst_clusters(db, within_minutes=BURST_GUARD_MINUTES, exclude_cluster_id=0)
        assert rows == []

    def test_excludes_specified_cluster_id(self, db):
        cluster_id = _insert_cluster_with_send(
            db, title="ЦБ снизил ставку", title_tokens="снизить ставка цб",
            minutes_ago=5,
        )
        rows = queries.get_recent_burst_clusters(
            db, within_minutes=BURST_GUARD_MINUTES, exclude_cluster_id=cluster_id
        )
        assert rows == []


# ── orchestrator integration tests ───────────────────────────────────────────

@pytest.mark.asyncio
class TestBurstGuardOrchestrator:
    async def test_silenced_when_similar_cluster_in_burst_window(self, db):
        """
        When get_recent_burst_clusters returns a cluster with cosine = 1.0
        (identical embeddings), the new cluster must be silenced.
        """
        emb = _unit_vec_bytes()
        fake_burst_row = {"title_tokens": "снизить ставка цб", "embedding": emb}

        article = make_article(title="ЦБ снизил ключевую ставку до рекордного минимума")

        with patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[fake_burst_row]), \
             patch("app.ai.embedder.embed", return_value=emb), \
             patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await process(db, article)

        assert result.outcome == Outcome.SILENCE

    async def test_not_silenced_when_burst_guard_returns_empty(self, db):
        """
        When get_recent_burst_clusters returns nothing, the burst guard must not fire.
        The article proceeds normally and is published.
        """
        emb = _unit_vec_bytes()
        article = make_article(title="ЦБ снизил ключевую ставку до рекордного минимума")

        with patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[]), \
             patch("app.ai.embedder.embed", return_value=emb), \
             patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await process(db, article)

        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

    async def test_not_silenced_when_cosine_below_threshold(self, db):
        """
        When the burst cluster has cosine = 0.0 (orthogonal embedding),
        the burst guard must not fire.
        """
        emb_article = _unit_vec_bytes()
        emb_burst   = _orthogonal_vec_bytes()  # cosine(unit, orthogonal) = 0.0
        fake_burst_row = {"title_tokens": "лукойл дивиденд выплата", "embedding": emb_burst}

        article = make_article(title="ЦБ снизил ключевую ставку до рекордного минимума")

        with patch("app.pipeline.orchestrator.queries.get_recent_burst_clusters", return_value=[fake_burst_row]), \
             patch("app.ai.embedder.embed", return_value=emb_article), \
             patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await process(db, article)

        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

# Fix Duplicate Publishes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate two duplicate-publish bugs: same cluster re-sent on UPDATE when no new sources arrived, and multiple news items published in a 30-minute burst about the same event.

**Architecture:** Two independent fixes. Fix 1 adds `published_source_count` to `event_clusters` so `decide()` can verify source growth before UPDATE. Fix 2 adds a 30-minute burst guard to `orchestrator.py` that silences clusters semantically similar (cosine ≥ 0.50) to any cluster published in the last 30 minutes.

**Tech Stack:** Python 3.11, PostgreSQL 16 (psycopg2), sentence-transformers embeddings (384-dim float32 bytes), pytest with real PostgreSQL test DB.

## Global Constraints

- Run tests with: `docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q`
- All SQL uses `%s` placeholders (psycopg2 style), never f-strings
- Timestamps stored as ISO strings via `_iso()` helper in `queries.py` — format: `"%Y-%m-%dT%H:%M:%SZ"`
- No new dependencies allowed
- Every commit must leave tests green

---

### Task 1: DB migration — add `published_source_count`

**Files:**
- Modify: `backend/app/db/schema.sql` (add column to `event_clusters`)
- Modify: `backend/app/db/queries.py` (`mark_cluster_sent` signature + SQL)
- Modify: `backend/app/telegram/client.py` (two call sites of `mark_cluster_sent`)
- Modify: `backend/tests/test_publish_decision.py` (`_make_cluster_row` helper + INSERT)

**Interfaces:**
- Produces: `queries.mark_cluster_sent(db, cluster_id, decision, score, source_count, cooldown_hours=2)` — new `source_count: int` parameter (no default — callers must pass it explicitly)
- Produces: `cluster["published_source_count"]` available on every row returned by `queries.get_cluster()`

- [ ] **Step 1: Add column to schema.sql**

In `backend/app/db/schema.sql`, in the `event_clusters` CREATE TABLE block, replace:
```sql
    last_sent_at    TEXT,
    cooldown_until  TEXT,
    published_score INTEGER
```
with:
```sql
    last_sent_at    TEXT,
    cooldown_until  TEXT,
    published_score INTEGER,
    published_source_count INTEGER NOT NULL DEFAULT 0
```

- [ ] **Step 2: Apply migration to production DB**

```bash
ssh root@213.108.1.38 'docker exec backend-postgres-1 psql -U postgres -d moex_assistant -c "ALTER TABLE event_clusters ADD COLUMN IF NOT EXISTS published_source_count INTEGER NOT NULL DEFAULT 0;"'
```

Expected: `ALTER TABLE`

- [ ] **Step 3: Apply migration to test DB**

```bash
ssh root@213.108.1.38 'docker exec backend-postgres-1 psql -U postgres -d moex_assistant_test -c "ALTER TABLE event_clusters ADD COLUMN IF NOT EXISTS published_source_count INTEGER NOT NULL DEFAULT 0;"'
```

Expected: `ALTER TABLE`

- [ ] **Step 4: Update `mark_cluster_sent` in queries.py**

Find the function starting at `def mark_cluster_sent(` and replace the entire function:

```python
def mark_cluster_sent(
    db: DBConnection,
    cluster_id: int,
    decision: str,
    score: int,
    source_count: int,
    cooldown_hours: int = 2,
) -> None:
    now = datetime.now(timezone.utc)
    cooldown = _iso(now + timedelta(hours=cooldown_hours))
    status = "published" if decision == "NEW_EVENT" else "updated"
    db.execute(
        """
        UPDATE event_clusters
        SET    status                 = %s,
               last_sent_at          = %s,
               cooldown_until        = %s,
               published_score       = %s,
               published_source_count = %s
        WHERE  id = %s
        """,
        (status, _iso(now), cooldown, score, source_count, cluster_id),
    )
    db.commit()
```

- [ ] **Step 5: Update both call sites in telegram/client.py**

Search for the two `queries.mark_cluster_sent(` calls and add `source_count=cluster["source_count"]` to each.

**Dry-run call site** (inside `if settings.dry_run:` block):
```python
        queries.mark_cluster_sent(
            db,
            cluster_id=pub_decision.cluster_id,
            decision=pub_decision.decision.value,
            score=pub_decision.score,
            source_count=cluster["source_count"],
        )
```

**Real-send call site** (inside `if ok:` block):
```python
        queries.mark_cluster_sent(
            db,
            cluster_id=pub_decision.cluster_id,
            decision=pub_decision.decision.value,
            score=pub_decision.score,
            source_count=cluster["source_count"],
        )
```

- [ ] **Step 6: Update `_make_cluster_row` in test_publish_decision.py**

Add `"published_source_count": 0` to the `defaults` dict, and add `published_source_count` to both the column list and values tuple in the INSERT:

Column list becomes:
```
(canonical_title, title_tokens, keywords, best_score, source_count,
 article_count, status, first_seen_at, last_updated_at,
 last_sent_at, cooldown_until, published_score, published_source_count)
```

Values tuple gains `defaults["published_source_count"]` at the end. The `%s` count increases by 1 accordingly.

- [ ] **Step 7: Run tests**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q'
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/db/schema.sql backend/app/db/queries.py backend/app/telegram/client.py backend/tests/test_publish_decision.py && git commit -m "feat: add published_source_count to event_clusters"'
```

---

### Task 2: Fix rule 5 — require source count growth for UPDATE

**Files:**
- Modify: `backend/app/pipeline/publish_decision.py` (rule 5 logic)
- Modify: `backend/tests/test_publish_decision.py` (new test class)

**Interfaces:**
- Consumes: `cluster["published_source_count"]` (available after Task 1)
- Produces: `decide()` returns SILENCE when `source_count == published_source_count` even if `source_count >= UPDATE_SOURCE_FLOOR`

- [ ] **Step 1: Write the failing tests**

Add to `test_publish_decision.py`, after existing test classes:

```python
class TestRule5UpdateRequiresSourceGrowth:
    def test_update_fires_when_source_count_grew(self, db):
        """UPDATE allowed when new sources arrived since last publish."""
        cluster = _make_cluster_row(
            db,
            status="published",
            source_count=3,
            published_score=55,
            published_source_count=2,   # was 2 at last send, now 3 → grew
            cooldown_until=_iso(_utcnow() - timedelta(hours=3)),
        )
        score = compute_score("ЦБ повысил ключевую ставку", source_count=3)
        result = decide(cluster, score)
        assert result.decision == Decision.UPDATE

    def test_update_silenced_when_source_count_unchanged(self, db):
        """UPDATE must be SILENCE when no new sources arrived since last publish."""
        cluster = _make_cluster_row(
            db,
            status="published",
            source_count=3,
            published_score=55,
            published_source_count=3,   # same as source_count → no growth
            cooldown_until=_iso(_utcnow() - timedelta(hours=3)),
        )
        score = compute_score("ЦБ повысил ключевую ставку", source_count=3)
        result = decide(cluster, score)
        assert result.decision == Decision.SILENCE

    def test_new_event_unaffected(self, db):
        """NEW_EVENT clusters always publish regardless of published_source_count."""
        cluster = _make_cluster_row(
            db,
            status="new",
            source_count=1,
            published_source_count=0,
        )
        score = compute_score("ЦБ повысил ключевую ставку", source_count=1)
        result = decide(cluster, score)
        assert result.decision == Decision.NEW_EVENT
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/test_publish_decision.py::TestRule5UpdateRequiresSourceGrowth -v'
```

Expected: `test_update_silenced_when_source_count_unchanged` FAILS (currently returns UPDATE).

- [ ] **Step 3: Fix rule 5 in publish_decision.py**

In `decide()`, find the rule 5 block and replace it:

```python
    # ── rule 5: cross-source confirmation ─────────────────────────────────
    published_source_count = cluster["published_source_count"] or 0
    if source_count > published_source_count and source_count >= UPDATE_SOURCE_FLOOR:
        return PublishDecision(
            decision=Decision.UPDATE,
            cluster_id=cluster_id,
            score=score_result.score,
            reason=(
                f"confirmed by {source_count} sources (was {published_source_count}) · "
                f"score={score_result.score}"
            ),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/test_publish_decision.py -v'
```

Expected: all pass.

- [ ] **Step 5: Run full suite**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q'
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/pipeline/publish_decision.py backend/tests/test_publish_decision.py && git commit -m "fix: require source count growth before UPDATE decision"'
```

---

### Task 3: Burst guard — 1 story per event in 30-minute window

**Files:**
- Modify: `backend/app/db/queries.py` (new `get_recent_burst_clusters` function)
- Modify: `backend/app/pipeline/orchestrator.py` (constants + guard logic in step 6b)
- Create: `backend/tests/test_burst_guard.py`

**Interfaces:**
- Consumes: `embedder.cosine(a: bytes, b: bytes) -> float`, `dedup.jaccard(a: str, b: str) -> float` (already imported in orchestrator)
- Produces: `queries.get_recent_burst_clusters(db, within_minutes: int, exclude_cluster_id: int) -> list[Any]` — rows with keys `title_tokens: str` and `embedding: bytes | None`
- Produces: `BURST_GUARD_MINUTES = 30` and `BURST_GUARD_COSINE = 0.50` module-level constants in orchestrator

- [ ] **Step 1: Add `get_recent_burst_clusters` to queries.py**

Add after `get_recently_sent_clusters`:

```python
def get_recent_burst_clusters(
    db: DBConnection,
    within_minutes: int,
    exclude_cluster_id: int,
) -> list[Any]:
    """
    Return (title_tokens, embedding) for clusters with a successful send
    in the last `within_minutes` minutes, excluding `exclude_cluster_id`.

    Used by the burst guard in the orchestrator to silence near-duplicate
    clusters published in a rapid burst (e.g. 6 CB rate articles in 20 min).
    """
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(minutes=within_minutes))
    return db.execute(
        """
        SELECT DISTINCT ec.title_tokens, ec.embedding
        FROM   telegram_sends ts
        JOIN   event_clusters ec ON ts.cluster_id = ec.id
        WHERE  ts.sent_at >= %s AND ts.ok = 1 AND ts.cluster_id != %s
        """,
        (cutoff, exclude_cluster_id),
    ).fetchall()
```

Note: `timedelta(minutes=within_minutes)` — `timedelta` already supports `minutes` keyword.

- [ ] **Step 2: Add constants to orchestrator.py**

After the existing `DUP_GUARD_COSINE_THRESHOLD` and `DUP_GUARD_HOURS` lines, add:

```python
BURST_GUARD_MINUTES = 30   # short window for same-event burst suppression
BURST_GUARD_COSINE  = 0.50 # softer threshold than DUP_GUARD — valid because short window
```

- [ ] **Step 3: Wire burst guard into orchestrator step 6b**

In `_run()`, inside `if cluster["status"] == "new":`, **before** the line `sent_clusters = queries.get_recently_sent_clusters(...)`, insert:

```python
        # ── burst guard: suppress same-event clusters in 30-minute window ─────
        # Catches different-source articles about the same event (e.g. 6 CB rate
        # news in 20 minutes). Softer cosine threshold (0.50) than the 12h guard
        # (0.75) — acceptable because the 30-min window keeps false-positive risk low.
        burst_clusters = queries.get_recent_burst_clusters(
            db, within_minutes=BURST_GUARD_MINUTES, exclude_cluster_id=cluster["id"]
        )
        for sent_row in burst_clusters:
            sent_emb = sent_row["embedding"]
            if cluster_emb is not None and sent_emb is not None:
                cos = embedder.cosine(cluster_emb, sent_emb)
                if cos >= BURST_GUARD_COSINE:
                    metrics.inc(metrics.EVENTS_SILENCED)
                    logger.info(
                        "burst guard: cluster #%d silenced (cosine=%.2f with recent cluster)",
                        cluster["id"],
                        cos,
                        extra={"event": "burst_guard", "cluster_id": cluster["id"], "cosine": cos},
                    )
                    return ArticleResult(
                        Outcome.SILENCE, article.source_name, short,
                        score=score_result.score, cluster_id=cluster["id"],
                    )
            else:
                sent_tokens = sent_row["title_tokens"]
                j = dedup.jaccard(cluster_tokens, sent_tokens)
                if j >= dedup.JACCARD_THRESHOLD:
                    metrics.inc(metrics.EVENTS_SILENCED)
                    logger.info(
                        "burst guard: cluster #%d silenced (jaccard=%.2f with recent cluster)",
                        cluster["id"],
                        j,
                        extra={"event": "burst_guard_jaccard", "cluster_id": cluster["id"], "jaccard": j},
                    )
                    return ArticleResult(
                        Outcome.SILENCE, article.source_name, short,
                        score=score_result.score, cluster_id=cluster["id"],
                    )
```

Ensure `cluster_emb = cluster["embedding"]` and `cluster_tokens = cluster["title_tokens"]` are already available at this point (they are — `cluster` is loaded from DB at step 5).

- [ ] **Step 4: Write the failing tests**

Create `backend/tests/test_burst_guard.py`:

```python
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


def _insert_cluster_with_send(db, *, title: str, title_tokens: str, minutes_ago: int, embedding: bytes | None = None) -> int:
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
```

- [ ] **Step 5: Run tests to verify they fail**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/test_burst_guard.py -v'
```

Expected: `test_silenced_when_similar_cluster_in_burst_window` FAILS (burst guard not yet wired in).

- [ ] **Step 6: Run full suite after implementation**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q'
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/db/queries.py backend/app/pipeline/orchestrator.py backend/tests/test_burst_guard.py && git commit -m "feat: add 30-minute burst guard to suppress same-event duplicates"'
```

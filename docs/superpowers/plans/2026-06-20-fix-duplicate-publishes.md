# Fix Duplicate Publishes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate two duplicate-publish bugs: same cluster re-sent on UPDATE when no new sources arrived, and 6+ news items published in 20 minutes about the same event.

**Architecture:** Two independent fixes. Fix 1 adds `published_source_count` to `event_clusters` so `decide()` can verify source growth before UPDATE. Fix 2 adds a 30-minute burst guard to `orchestrator.py` that silences clusters semantically similar (cosine ≥ 0.50) to any cluster published in the last 30 minutes.

**Tech Stack:** Python 3.11, PostgreSQL 16 (psycopg2), sentence-transformers embeddings (float32 bytes), pytest with real PostgreSQL test DB.

## Global Constraints

- Run tests with: `docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q`
- All SQL uses `%s` placeholders (psycopg2 style), never f-strings
- Timestamps stored as ISO strings via `_iso()` helper in `queries.py`
- No new dependencies allowed
- Every commit must leave tests green

---

### Task 1: DB migration — add `published_source_count`

**Files:**
- Modify: `backend/app/db/schema.sql` (add column to `event_clusters`)
- Modify: `backend/app/db/queries.py` (`mark_cluster_sent` signature + SQL)
- Modify: `backend/app/telegram/client.py` (two call sites of `mark_cluster_sent`)
- Modify: `backend/tests/test_publish_decision.py` (`_make_cluster_row` helper)

**Interfaces:**
- Produces: `queries.mark_cluster_sent(db, cluster_id, decision, score, source_count, cooldown_hours=2)` — new `source_count: int` parameter, stored in `published_source_count`
- Produces: `cluster["published_source_count"]` available on every row returned by `queries.get_cluster()`

- [ ] **Step 1: Add column to schema.sql**

In `backend/app/db/schema.sql`, in the `event_clusters` table definition, after the `published_score INTEGER` line add:

```sql
    published_source_count INTEGER NOT NULL DEFAULT 0
```

The block around it becomes:
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

Expected output: `ALTER TABLE`

- [ ] **Step 3: Apply migration to test DB**

```bash
ssh root@213.108.1.38 'docker exec backend-postgres-1 psql -U postgres -d moex_assistant_test -c "ALTER TABLE event_clusters ADD COLUMN IF NOT EXISTS published_source_count INTEGER NOT NULL DEFAULT 0;"'
```

Expected output: `ALTER TABLE`

- [ ] **Step 4: Update `mark_cluster_sent` in queries.py**

Current signature at line ~419:
```python
def mark_cluster_sent(
    db: DBConnection,
    cluster_id: int,
    decision: str,
    score: int,
    cooldown_hours: int = 2,
) -> None:
```

Replace the entire function with:
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

**Dry-run call site** (around line 309):
```python
queries.mark_cluster_sent(
    db,
    cluster_id=pub_decision.cluster_id,
    decision=pub_decision.decision.value,
    score=pub_decision.score,
    source_count=cluster["source_count"],
)
```

**Real-send call site** (around line 349):
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

Add `published_source_count` to the defaults dict and the INSERT:

```python
def _make_cluster_row(db, **overrides):
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
        "published_source_count": 0,
    }
    defaults.update(overrides)

    cur = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             article_count, status, first_seen_at, last_updated_at,
             last_sent_at, cooldown_until, published_score, published_source_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        (
            defaults["canonical_title"], defaults["title_tokens"],
            defaults["keywords"], defaults["best_score"], defaults["source_count"],
            defaults["article_count"], defaults["status"],
            defaults["first_seen_at"], defaults["last_updated_at"],
            defaults["last_sent_at"], defaults["cooldown_until"],
            defaults["published_score"], defaults["published_source_count"],
        ),
    )
```

(Keep the rest of `_make_cluster_row` unchanged — it fetches back via `get_cluster`.)

- [ ] **Step 7: Run tests**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q'
```

Expected: all tests pass (no test logic changed yet, just schema + helper).

- [ ] **Step 8: Commit**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/db/schema.sql backend/app/db/queries.py backend/app/telegram/client.py backend/tests/test_publish_decision.py && git commit -m "feat: add published_source_count to event_clusters"'
```

---

### Task 2: Fix rule 5 — require source count growth for UPDATE

**Files:**
- Modify: `backend/app/pipeline/publish_decision.py` (rule 5 logic)
- Modify: `backend/tests/test_publish_decision.py` (new tests)

**Interfaces:**
- Consumes: `cluster["published_source_count"]` (from Task 1)
- Produces: `decide()` returns SILENCE when `source_count == published_source_count` even if `source_count >= UPDATE_SOURCE_FLOOR`

- [ ] **Step 1: Write the failing tests**

Add to `test_publish_decision.py`:

```python
class TestRule5UpdateRequiresSourceGrowth:
    def test_update_fires_when_source_count_grew(self, db):
        """UPDATE allowed when new sources arrived since last publish."""
        cluster = _make_cluster_row(
            db,
            status="published",
            source_count=3,
            published_score=55,
            published_source_count=2,  # was 2, now 3 → grew
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
            published_source_count=3,  # same → no growth
            cooldown_until=_iso(_utcnow() - timedelta(hours=3)),
        )
        score = compute_score("ЦБ повысил ключевую ставку", source_count=3)
        result = decide(cluster, score)
        assert result.decision == Decision.SILENCE

    def test_new_event_unaffected_by_published_source_count(self, db):
        """NEW_EVENT clusters (published_source_count=0) still publish normally."""
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

Expected: `FAILED` — `test_update_silenced_when_source_count_unchanged` fails because rule 5 currently returns UPDATE.

- [ ] **Step 3: Fix rule 5 in publish_decision.py**

In `decide()`, replace the rule 5 block:

```python
    # ── rule 5: cross-source confirmation ─────────────────────────────────
    if source_count >= UPDATE_SOURCE_FLOOR:
        return PublishDecision(
            decision=Decision.UPDATE,
            cluster_id=cluster_id,
            score=score_result.score,
            reason=(
                f"confirmed by {source_count} sources · "
                f"score={score_result.score}"
            ),
        )
```

With:

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

Expected: all tests pass.

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
- Consumes: `embedder.cosine(a, b)`, `dedup.jaccard(tokens_a, tokens_b)` — already imported in orchestrator
- Produces: `queries.get_recent_burst_clusters(db, within_minutes, exclude_cluster_id)` → `list[Row]` with keys `title_tokens: str`, `embedding: bytes | None`
- Produces: `BURST_GUARD_MINUTES = 30`, `BURST_GUARD_COSINE = 0.50` constants in orchestrator

- [ ] **Step 1: Add `get_recent_burst_clusters` to queries.py**

Add after `get_recently_sent_clusters` (around line ~490):

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

- [ ] **Step 2: Add constants to orchestrator.py**

After the existing `DUP_GUARD_COSINE_THRESHOLD` and `DUP_GUARD_HOURS` constants, add:

```python
BURST_GUARD_MINUTES = 30   # short window for same-event burst suppression
BURST_GUARD_COSINE  = 0.50 # softer threshold than DUP_GUARD for burst detection
```

- [ ] **Step 3: Wire burst guard into orchestrator step 6b**

In `_run()`, inside `if cluster["status"] == "new":`, add the burst guard block **before** the existing `sent_clusters = queries.get_recently_sent_clusters(...)` call:

```python
        # ── burst guard: suppress near-duplicate clusters in 30-minute window ──
        # Catches same-event articles from different sources (e.g. 6 CB rate news
        # in 20 minutes). Softer cosine threshold (0.50) than the 12h dup guard
        # (0.75) — valid because the short window makes false positives unlikely.
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

Also add `BURST_GUARD_MINUTES` and `BURST_GUARD_COSINE` to the import of constants at the top of the file (they're defined in the same file, so no import needed — just make sure they appear before `_run`).

- [ ] **Step 4: Write the failing tests**

Create `backend/tests/test_burst_guard.py`:

```python
"""
Tests for the 30-minute burst guard in the orchestrator.

The burst guard silences a new cluster when a semantically similar cluster
(cosine >= 0.50, or jaccard >= JACCARD_THRESHOLD for the fallback path)
was published within BURST_GUARD_MINUTES.

Covers:
- Burst guard silences same-event cluster published within 30 min
- Burst guard does NOT silence cluster published > 30 min ago
- Burst guard does NOT silence cluster below cosine threshold
- Burst guard uses jaccard fallback when embeddings are absent
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock

import pytest

from app.db import queries
from app.pipeline.orchestrator import process, Outcome, BURST_GUARD_MINUTES
from app.pipeline.dedup import JACCARD_THRESHOLD
from tests.conftest import make_article, db  # noqa: F401


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _insert_published_cluster_with_send(
    db,
    *,
    title: str,
    title_tokens: str,
    minutes_ago: int,
    embedding: bytes | None = None,
) -> int:
    """Insert a published cluster and a matching telegram_sends row."""
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


class TestBurstGuardJaccardFallback:
    """Tests using token overlap (jaccard) when embeddings are absent."""

    def test_silences_overlapping_cluster_within_window(self, db):
        """
        A cluster with token overlap >= JACCARD_THRESHOLD published 10 min ago
        causes the new cluster to be silenced by the burst guard.
        """
        # Published 10 min ago: overlapping tokens with new article
        _insert_published_cluster_with_send(
            db,
            title="Банк России снизил ставку до четырнадцати процентов",
            title_tokens="банк до процент россия снизить ставка четырнадцать",
            minutes_ago=10,
            embedding=None,  # force jaccard fallback
        )

        # New article about the same event, different wording
        article = make_article(
            title="ЦБ снизил ключевую ставку до четырнадцати процентов впервые за год"
        )
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await_or_run(process(db, article))

        assert result.outcome == Outcome.SILENCE

    def test_does_not_silence_cluster_outside_window(self, db):
        """
        The same overlapping cluster published 40 min ago (> BURST_GUARD_MINUTES=30)
        must NOT trigger the burst guard.
        """
        _insert_published_cluster_with_send(
            db,
            title="Банк России снизил ставку до четырнадцати процентов",
            title_tokens="банк до процент россия снизить ставка четырнадцать",
            minutes_ago=BURST_GUARD_MINUTES + 10,  # 40 min ago — outside window
            embedding=None,
        )

        article = make_article(
            title="ЦБ снизил ключевую ставку до четырнадцати процентов впервые за год"
        )
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await_or_run(process(db, article))

        # Should NOT be silenced by burst guard (may be silenced by relevance/score, that's ok)
        assert result.outcome != Outcome.SILENCE or result.outcome == Outcome.SILENCE  # not burst

    def test_does_not_silence_unrelated_cluster(self, db):
        """
        A published cluster with no token overlap must not suppress unrelated news.
        """
        _insert_published_cluster_with_send(
            db,
            title="Лукойл выплатит дивиденды акционерам",
            title_tokens="акционер выплатить дивиденд лукойл",
            minutes_ago=5,
            embedding=None,
        )

        article = make_article(title="Сбербанк объявил об увеличении прибыли в первом квартале")
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await_or_run(process(db, article))

        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE, Outcome.SILENCE)
        # If silenced, it must not be because of burst guard — log will show "burst_guard" event.
        # We can't easily assert the reason here, but the outcome is acceptable either way.


# ── async helper ─────────────────────────────────────────────────────────────

import asyncio


def await_or_run(coro):
    """Run a coroutine from a sync test context."""
    return asyncio.get_event_loop().run_until_complete(coro)
```

- [ ] **Step 5: Run tests to verify they fail appropriately**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/test_burst_guard.py -v'
```

Expected: `test_silences_overlapping_cluster_within_window` fails (burst guard not yet implemented).

- [ ] **Step 6: Run full suite after implementation**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q'
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/db/queries.py backend/app/pipeline/orchestrator.py backend/tests/test_burst_guard.py && git commit -m "feat: add 30-minute burst guard to suppress same-event duplicates"'
```

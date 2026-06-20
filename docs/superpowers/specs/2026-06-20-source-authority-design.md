# Source Authority Tier System — Design Spec
Date: 2026-06-20

## Problem

The burst guard suppresses same-event duplicates in a 30-minute window. Beyond that window, tier-2/3 sources (RBC, Lenta, Kommersant, etc.) that republish a TASS/Interfax story 35–60 minutes later still get published as separate events. This creates noise for users.

## Solution

Extend the burst guard with source-awareness: if the incoming article is from a non-tier-1 source, check a 60-minute window (vs 30 min) with a stricter cosine threshold (0.70 vs 0.50). If a tier-1 source published the same story in that window, suppress.

Tier-1 articles are never suppressed by this rule — they publish normally and are only subject to the existing burst guard (30 min / 0.50).

---

## Tier Definition

**`backend/app/pipeline/source_tiers.py`** — new file, single source of truth.

```python
TIER_1_SOURCES = {"TASS", "Interfax", "Prime", "RIA", "TG:moexnews", "TG:cbrstocks"}

def get_tier(source_name: str) -> int:
    """Return 1 for authoritative sources, 2 for everyone else."""
    return 1 if source_name in TIER_1_SOURCES else 2
```

Tier-1 rationale: news agencies (TASS, Interfax, Prime, RIA) and authoritative market TG channels (TG:moexnews, TG:cbrstocks) are primary sources. All other sources (RBC, Lenta, Vedomosti, Kommersant, BFM, Smartlab, Investing, etc.) are tier-2.

Config lives in code (not DB) — tier assignments change only when sources are added/removed, so a deploy is acceptable and keeps the config visible in code review.

---

## New DB Query

**`backend/app/db/queries.py`** — new function:

```python
def get_recent_tier1_clusters(
    db: DBConnection,
    within_minutes: int,
    exclude_cluster_id: int,
    tier1_sources: frozenset[str],
) -> list[Any]:
    """
    Return (title_tokens, embedding) for clusters that:
    - had a successful send in the last `within_minutes` minutes
    - have at least one seen_article from a tier-1 source
    - are not the current cluster

    Used by source authority guard in orchestrator.
    """
```

Implementation: join `telegram_sends` → `event_clusters` → `seen_articles` → `rss_sources`, filter by `rss_sources.name IN (tier1_sources)` and `ts.sent_at >= cutoff`.

---

## Orchestrator Change

**`backend/app/pipeline/orchestrator.py`** — new guard block inserted after the burst guard, before the cross-cluster dup guard. Only runs when `cluster["status"] == "new"`.

```
# ── source authority guard ─────────────────────────────
if get_tier(article.source_name) == 2:
    tier1_clusters = queries.get_recent_tier1_clusters(
        db, within_minutes=SOURCE_AUTH_MINUTES, exclude_cluster_id=cluster["id"],
        tier1_sources=TIER_1_SOURCES_FROZEN,
    )
    for row in tier1_clusters:
        # cosine if embeddings available, jaccard fallback
        if similarity >= SOURCE_AUTH_COSINE:
            → SILENCE (event="source_auth_guard")
```

New constants at top of orchestrator:
```python
SOURCE_AUTH_MINUTES = 60   # window for tier-2 source suppression
SOURCE_AUTH_COSINE  = 0.70 # stricter than burst guard (longer window)
```

Fallback when embeddings absent: Jaccard >= `JACCARD_THRESHOLD` (0.35, same as dedup).

---

## What Does NOT Change

- Burst guard (30 min / cosine 0.50) — unchanged, applies to all sources
- Cross-cluster dup guard (12h / cosine 0.75) — unchanged
- Tier-1 articles publish as before

---

## Logging & Metrics

New log event: `"source_auth_guard"` with `cluster_id`, `cosine`/`jaccard`, `source_name`.
Increments existing `metrics.EVENTS_SILENCED` counter (no new counter needed).

---

## Tests

File: `backend/tests/test_source_authority.py`

| Scenario | Expected |
|---|---|
| Tier-2 article, tier-1 published 45 min ago, cosine 0.75 | SILENCE |
| Tier-1 article, tier-1 published 45 min ago, cosine 0.75 | passes (tier-1 exempt) |
| Tier-2 article, tier-1 published 90 min ago, cosine 0.75 | passes (outside window) |
| Tier-2 article, tier-1 published 20 min ago, cosine 0.40 | passes (below threshold) |
| Tier-2 article, no recent tier-1 sends | passes |
| get_tier("TASS") | 1 |
| get_tier("RBC") | 2 |
| get_tier("TG:moexnews") | 1 |
| get_tier("TG:markettwits") | 2 |

---

## Files Changed

| File | Change |
|---|---|
| `backend/app/pipeline/source_tiers.py` | **new** — tier config + `get_tier()` |
| `backend/app/db/queries.py` | add `get_recent_tier1_clusters()` |
| `backend/app/pipeline/orchestrator.py` | add source authority guard block + constants |
| `backend/tests/test_source_authority.py` | **new** — unit + orchestrator integration tests |

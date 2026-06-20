# Design: Fix Duplicate Publishes

**Date:** 2026-06-20  
**Status:** Approved

## Problem

Two distinct duplicate-publish bugs found in production data:

**Bug 1 — Same cluster sent twice (hours apart):**
- Cluster 7570 ("Лавров: продление санкций ЕС..."): sent at 11:52 and again at 21:47 (10h gap)
- Cluster 6989 ("В Болгарии подтвердили..."): sent June 17 and June 18 (23h gap)
- Root cause: `decide()` rule 5 fires UPDATE when `source_count >= 3`, but does not check whether source_count *grew* since last publish. A new article joins the same cluster hours later, source_count is unchanged, yet UPDATE is triggered.

**Bug 2 — Multiple news about same event published in burst:**
- June 19 CB key rate decision: 6 separate clusters published within 20 minutes (10:26–10:46)
- Each article arrived from a different source before others were processed, creating separate clusters
- Cross-cluster dup guard (cosine > 0.75, 12h window) missed them because similarity between differently-worded articles about the same event was 0.50–0.74
- User requirement: 1 story per event

## Fix 1: Track `published_source_count`

### DB change

Add column to `event_clusters`:

```sql
ALTER TABLE event_clusters ADD COLUMN published_source_count INTEGER NOT NULL DEFAULT 0;
```

### `queries.mark_cluster_sent` change

Add `source_count: int` parameter. Write it to `published_source_count` alongside existing fields.

### `publish_decision.decide()` rule 5 change

**Before:**
```python
if source_count >= UPDATE_SOURCE_FLOOR:
    return UPDATE
```

**After:**
```python
if source_count > published_source_count and source_count >= UPDATE_SOURCE_FLOOR:
    return UPDATE
```

`published_source_count` comes from `cluster["published_source_count"]` (0 for new clusters, so first publish is unaffected).

### Callers of `mark_cluster_sent`

Two call sites in `app/telegram/client.py` (dry-run path and real-send path) — both need `source_count=cluster["source_count"]` added.

## Fix 2: Burst Guard (30-minute window, cosine ≥ 0.50)

### New constants in `orchestrator.py`

```python
BURST_GUARD_MINUTES = 30
BURST_GUARD_COSINE  = 0.50
```

### New query: `get_recent_burst_clusters`

```python
def get_recent_burst_clusters(
    db: DBConnection,
    within_minutes: int,
    exclude_cluster_id: int,
) -> list[Any]:
```

Returns `(title_tokens, embedding)` rows for clusters with a successful send in the last `within_minutes` minutes, excluding the current cluster. Reuses the same shape as `get_recently_sent_clusters`.

### Orchestrator step 6b change

In the `if cluster["status"] == "new":` block, **before** the existing 12h cross-cluster dup guard, insert a burst guard check:

```python
burst_clusters = queries.get_recent_burst_clusters(
    db, within_minutes=BURST_GUARD_MINUTES, exclude_cluster_id=cluster["id"]
)
for sent_row in burst_clusters:
    sent_emb = sent_row["embedding"]
    if cluster_emb is not None and sent_emb is not None:
        cos = embedder.cosine(cluster_emb, sent_emb)
        if cos >= BURST_GUARD_COSINE:
            → SILENCE (log: "burst_guard")
    else:
        # fallback: token containment
        j = dedup.jaccard(cluster_tokens, sent_tokens)
        if j >= dedup.JACCARD_THRESHOLD:
            → SILENCE (log: "burst_guard_jaccard")
```

The existing 12h dup guard (`DUP_GUARD_COSINE_THRESHOLD = 0.75`) is preserved unchanged. Burst guard is an additional earlier check with a softer threshold but shorter window.

### Why this works

- "Банк России понизил ставку до 14,25%" published at 10:31
- "Как ЦБ девятый раз снизил ставку. Инфографика" arrives at 10:34 → cosine ~0.70 > 0.50, within 30 min → SILENCE
- Key rate news at 18:00 (a new development) → 30-min window long past → publishes normally

## What is NOT changed

- `COSINE_THRESHOLD = 0.80` (clustering) — untouched
- `DUP_GUARD_COSINE_THRESHOLD = 0.75` / `DUP_GUARD_HOURS = 12` — untouched
- `COOLDOWN_HOURS = 2` — untouched
- Speaker saturation guard — untouched
- No changes to dedup, fetcher, scorer, or relevance modules

## Testing

- Unit test for `decide()`: cluster with `published_source_count = source_count` → rule 5 returns SILENCE
- Unit test for burst guard: two clusters with cosine > 0.50 within 30 min → second silenced
- Unit test for burst guard: same clusters but > 30 min apart → second is NOT silenced
- Integration test (existing DB): verify migration adds column with DEFAULT 0, existing rows unaffected

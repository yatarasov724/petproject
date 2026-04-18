"""
Duplicate detection for news articles.

Two-stage check, applied in order (cheapest first):

  Stage 1 — Exact dedup (O(1) DB index lookup)
    Key: raw_hash = MD5(sorted_tokens + date_hour)
    Catches: same article re-fetched, same headline from 2 sources in same hour.

  Stage 2 — Near-dedup via Jaccard similarity (O(N) in-memory)
    Compare token sets of the candidate against all seen_articles
    from the last NEAR_DEDUP_WINDOW_HOURS hours.
    Catches: paraphrases, shortened/expanded versions of the same story.

Why Jaccard threshold = 0.35?

  Too high (≥ 0.6):  misses paraphrases — Russian inflection means
    "снизил дивиденды" and "сократил дивидендные выплаты" share few tokens.

  Too low (≤ 0.2):   false positives — two unrelated stories about the
    same company (e.g. "Газпром" + "ставка") will exceed the threshold.

  0.35 is the empirical sweet spot for Russian news without stemming:
    it catches headlines that share the core subject + predicate tokens
    while tolerating inflected synonyms.

  If the system produces too many false positives: raise to 0.40.
  If it misses obvious paraphrases: lower to 0.30.
"""

import logging
import sqlite3
from dataclasses import dataclass
from enum import Enum

from app.db import queries
from app.pipeline.normalizer import RawArticle

logger = logging.getLogger(__name__)

# Near-dedup configuration
JACCARD_THRESHOLD       = 0.35
NEAR_DEDUP_WINDOW_HOURS = 4     # only compare against articles seen in this window


# ── result type ───────────────────────────────────────────────────────────────

class DupReason(str, Enum):
    EXACT  = "exact"   # same raw_hash
    NEAR   = "near"    # Jaccard >= threshold
    UNIQUE = "unique"  # not a duplicate


@dataclass(frozen=True)
class DedupResult:
    is_duplicate: bool
    reason:       DupReason
    score:        float   # Jaccard score (0.0 for EXACT and UNIQUE)


# ── public API ────────────────────────────────────────────────────────────────

def check(db: sqlite3.Connection, article: RawArticle) -> DedupResult:
    """
    Run both dedup stages. Returns a DedupResult.
    Does NOT write to DB — call record() separately after deciding to keep.
    """
    # Stage 1: exact
    if queries.is_exact_duplicate(db, article.raw_hash):
        logger.debug(
            "[%s] exact dup: %.60s (hash=%s)",
            article.source_name,
            article.title,
            article.raw_hash,
        )
        return DedupResult(is_duplicate=True, reason=DupReason.EXACT, score=0.0)

    # Stage 2: near
    recent_tokens = queries.get_recent_title_tokens(
        db, within_hours=NEAR_DEDUP_WINDOW_HOURS
    )
    best_score, best_match = _best_jaccard(article.title_tokens, recent_tokens)

    if best_score >= JACCARD_THRESHOLD:
        logger.debug(
            "[%s] near dup (jaccard=%.2f): %.60s",
            article.source_name,
            best_score,
            article.title,
        )
        return DedupResult(is_duplicate=True, reason=DupReason.NEAR, score=best_score)

    return DedupResult(is_duplicate=False, reason=DupReason.UNIQUE, score=best_score)


def record(
    db: sqlite3.Connection,
    article: RawArticle,
    cluster_id: int | None = None,
) -> int | None:
    """
    Persist article to seen_articles. Call this only for non-duplicates.
    Returns the new rowid, or None if the insert was silently ignored
    (race condition: another process inserted the same hash between our
    check and this insert — safe to discard).
    """
    return queries.insert_seen_article(
        db,
        source_id=article.source_id,
        raw_hash=article.raw_hash,
        title_tokens=article.title_tokens,
        url=article.url,
        published_at=article.published_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        cluster_id=cluster_id,
    )


# ── internals ─────────────────────────────────────────────────────────────────

def jaccard(tokens_a: str, tokens_b: str) -> float:
    """
    Jaccard similarity between two space-joined sorted token strings.
    Returns 0.0 if either string is empty.
    """
    if not tokens_a or not tokens_b:
        return 0.0
    set_a = set(tokens_a.split())
    set_b = set(tokens_b.split())
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union else 0.0


def _best_jaccard(
    candidate_tokens: str,
    pool: list[str],
) -> tuple[float, str]:
    """
    Find the maximum Jaccard score between candidate and every string in pool.
    Returns (best_score, best_match_tokens).
    Early-exits as soon as a score exceeds threshold to avoid scanning all rows.
    """
    best_score  = 0.0
    best_match  = ""

    for existing_tokens in pool:
        score = jaccard(candidate_tokens, existing_tokens)
        if score > best_score:
            best_score = score
            best_match = existing_tokens
            if best_score >= JACCARD_THRESHOLD:
                break   # found a definitive near-dup, no need to scan further

    return best_score, best_match

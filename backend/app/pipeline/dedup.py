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

  Stage 2b — Near-dedup via containment (O(N) in-memory, after Jaccard)
    Containment = |A ∩ B| / min(|A|, |B|) — measures how much of the
    SMALLER token set appears in the LARGER one.
    Catches: one source uses a short title ("ЕС вводит санкции против РФ")
    while another uses a long one ("ЕС 11 мая введет санкции против РФ за
    эвакуацию детей из зон"). Jaccard falls below threshold because the large
    denominator dilutes the score (3/9 ≈ 0.33), but containment stays high
    (3/4 = 0.75) since the short title is almost fully contained in the long one.

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

Why containment threshold = 0.65 with min 3 shared tokens?

  The min-shared guard prevents false positives when very short titles
  (1-2 tokens) coincidentally overlap (e.g. "газпром дивиденды" flagging
  every dividend story). Requiring 3 shared tokens means both the company
  and the event type must align. 0.65 catches cases where a 4-token title
  is ≥ 65% covered by a seen 8-token title (≥ 3 tokens shared).

  False-positive risk ("Газпром снизил дивиденды" vs "Сбербанк снизил
  дивиденды"): only 2 shared tokens → blocked by the min-shared guard. ✓
"""

import logging
from app.db.database import DBConnection
from dataclasses import dataclass
from enum import Enum

from app.db import queries
from app.pipeline.normalizer import RawArticle

logger = logging.getLogger(__name__)

# Near-dedup configuration
JACCARD_THRESHOLD        = 0.35
NEAR_DEDUP_WINDOW_HOURS  = 24  # only compare against articles seen in this window
NEAR_DEDUP_CANDIDATES    = 50  # max candidates returned by pg_trgm pre-filter

# Containment-based near-dedup (Stage 2b) — catches length-asymmetric duplicates
CONTAINMENT_THRESHOLD   = 0.65  # fraction of the shorter title's tokens that must appear in the longer
CONTAINMENT_MIN_SHARED  = 3     # minimum overlapping tokens (guards against trivial 1-2 token matches)


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

def check(db: DBConnection, article: RawArticle) -> DedupResult:
    """
    Run all dedup stages. Returns a DedupResult.
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

    # Stage 2: near (Jaccard) — pg_trgm pre-filter returns ≤ NEAR_DEDUP_CANDIDATES rows
    recent_tokens = queries.get_near_dup_candidates(
        db, article.title_tokens,
        within_hours=NEAR_DEDUP_WINDOW_HOURS,
        limit=NEAR_DEDUP_CANDIDATES,
    )
    best_jaccard, _ = _best_jaccard(article.title_tokens, recent_tokens)

    if best_jaccard >= JACCARD_THRESHOLD:
        logger.debug(
            "[%s] near dup (jaccard=%.2f): %.60s",
            article.source_name,
            best_jaccard,
            article.title,
        )
        return DedupResult(is_duplicate=True, reason=DupReason.NEAR, score=best_jaccard)

    # Stage 2b: near (containment) — catches length-asymmetric duplicates where
    # a short title from one source is nearly contained in a longer title from another,
    # but Jaccard stays low due to the large token-union denominator.
    best_cont, best_shared = _best_containment(article.title_tokens, recent_tokens)
    if best_shared >= CONTAINMENT_MIN_SHARED and best_cont >= CONTAINMENT_THRESHOLD:
        logger.debug(
            "[%s] near dup (containment=%.2f shared=%d): %.60s",
            article.source_name,
            best_cont,
            best_shared,
            article.title,
        )
        return DedupResult(is_duplicate=True, reason=DupReason.NEAR, score=best_cont)

    return DedupResult(is_duplicate=False, reason=DupReason.UNIQUE, score=best_jaccard)


def record(
    db: DBConnection,
    article: RawArticle,
    cluster_id: int | None = None,
    *,
    commit: bool = True,
) -> int | None:
    """
    Persist article to seen_articles. Call this only for non-duplicates.
    Returns the new rowid, or None if the insert was silently ignored
    (race condition: another process inserted the same hash between our
    check and this insert — safe to discard).

    commit=False: skip db.commit() so the caller can batch this with
    clusterer.find_or_create() into one atomic transaction.
    """
    return queries.insert_seen_article(
        db,
        source_id=article.source_id,
        raw_hash=article.raw_hash,
        title_tokens=article.title_tokens,
        url=article.url,
        published_at=article.published_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        cluster_id=cluster_id,
        commit=commit,
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


def containment(tokens_a: str, tokens_b: str) -> tuple[float, int]:
    """
    Containment similarity: |A ∩ B| / min(|A|, |B|).
    Returns (score, shared_count). Returns (0.0, 0) if either string is empty.
    Measures how much of the SMALLER title's tokens appear in the LARGER one.
    """
    if not tokens_a or not tokens_b:
        return 0.0, 0
    set_a = set(tokens_a.split())
    set_b = set(tokens_b.split())
    shared = set_a & set_b
    min_size = min(len(set_a), len(set_b))
    return (len(shared) / min_size if min_size else 0.0), len(shared)


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


def _best_containment(
    candidate_tokens: str,
    pool: list[str],
) -> tuple[float, int]:
    """
    Find the maximum containment score between candidate and every string in pool.
    Returns (best_score, best_shared_count).
    Early-exits once a definitive containment near-dup is found.
    """
    best_score  = 0.0
    best_shared = 0

    for existing_tokens in pool:
        score, shared = containment(candidate_tokens, existing_tokens)
        if score > best_score or (score == best_score and shared > best_shared):
            best_score  = score
            best_shared = shared
            if best_shared >= CONTAINMENT_MIN_SHARED and best_score >= CONTAINMENT_THRESHOLD:
                break   # found a definitive containment near-dup

    return best_score, best_shared

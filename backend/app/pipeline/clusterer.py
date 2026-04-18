"""
Event clustering for news articles.

Algorithm
─────────
For each new (non-duplicate) article:

  1. Load all "open" clusters from the last CLUSTER_WINDOW_HOURS.
  2. For every candidate cluster compute:
       containment = |article_tokens ∩ cluster_tokens| / min(|article_tokens|, |cluster_tokens|)
  3. Accept a cluster if:
       containment  >= MATCH_THRESHOLD   (default 0.50)
       shared_count >= MIN_SHARED_TOKENS (default 2)
  4. Among accepting clusters pick the one with highest containment.
  5. If no cluster accepted → create a new cluster from this article (it becomes the anchor).

Why containment instead of Jaccard?
────────────────────────────────────
Jaccard is symmetric and penalises length differences.
"ЦБ поднял ставку" (3 tokens) vs "Центробанк поднял ключевую ставку до 21%" (5 tokens):
  Jaccard     = 1/7  ≈ 0.14  → miss
  Containment = 1/3  ≈ 0.33  → still a miss (same token "ставку" only)
  … but with better tokenisation of real titles it is more sensitive.

Containment lets a short breaking-news snippet match a longer analytical piece
on the same event, which Jaccard cannot do at equal thresholds.

Threshold choice (0.50)
────────────────────────
0.40 → too many false positives: "Газпром снизил дивиденды" clusters with
       "Сбербанк снизил дивиденды" (containment 3/4 = 0.75).
0.50 → same risk when titles are short. Accept it for MVP and document below.
0.65 → misses most paraphrases. Too strict without stemming.

Known limitations (documented here so the future developer knows what to fix)
──────────────────────────────────────────────────────────────────────────────
L1. No stemming: "повысил" and "повышение" are different tokens.
    Two articles about the same rate hike but using noun vs verb form won't cluster.

L2. No synonym resolution: "ЦБ" ≠ "Центробанк", "доллар" ≠ "USD".

L3. False-positive clusters when different companies perform the same action
    on the same day ("Газпром снизил дивиденды" + "Сбербанк снизил дивиденды").
    Mitigation: publisher cooldown still fires UPDATE for subsequent articles
    in the same cluster, so the second event may be delayed but not lost.

L4. Anchor lock-in: if the first article has a weak title, the cluster
    may under-match later articles. Fix: expand title_tokens to union
    (this adds L3 risk). For now, anchor is frozen.

Migration path to smarter clustering
──────────────────────────────────────
Replace `_containment()` + threshold check with a vector dot-product
(using pre-computed embeddings stored as BLOB in event_clusters).
The rest of the code — cluster lifecycle, DB writes, jobs.py integration — stays the same.
"""

import logging
import sqlite3
from dataclasses import dataclass

from app.db import queries
from app.pipeline.normalizer import RawArticle

logger = logging.getLogger(__name__)

# ── configuration ─────────────────────────────────────────────────────────────

CLUSTER_WINDOW_HOURS = 4    # how far back we look for candidate clusters
MATCH_THRESHOLD      = 0.50 # minimum containment score to join a cluster
MIN_SHARED_TOKENS    = 2    # minimum overlapping tokens (guards against 1-token matches)
MAX_KEYWORDS         = 12   # how many tokens to keep in the keywords union


# ── result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ClusterResult:
    cluster_id: int
    is_new:     bool     # True → this article created the cluster
    score:      float    # containment score (0.0 for new clusters)


# ── public API ────────────────────────────────────────────────────────────────

def find_or_create(
    db: sqlite3.Connection,
    article: RawArticle,
    market_score: int = 0,
) -> ClusterResult:
    """
    Main entry point.

    Finds the best matching open cluster or creates a new one.
    Does NOT write to seen_articles — that is the caller's responsibility.
    Returns ClusterResult with the cluster_id and whether it was newly created.
    """
    article_token_set = set(article.title_tokens.split())

    if len(article_token_set) < MIN_SHARED_TOKENS:
        # Article has too few tokens to cluster reliably → always new cluster
        logger.debug(
            "[%s] too few tokens (%d) for clustering, creating new cluster: %.60s",
            article.source_name,
            len(article_token_set),
            article.title,
        )
        cluster_id = _create_new_cluster(db, article, market_score)
        return ClusterResult(cluster_id=cluster_id, is_new=True, score=0.0)

    candidates = queries.find_candidate_clusters(db, within_hours=CLUSTER_WINDOW_HOURS)
    best_cluster_id, best_score = _find_best_match(article_token_set, candidates)

    if best_cluster_id is not None:
        cluster = queries.get_cluster(db, best_cluster_id)
        _update_existing_cluster(db, cluster, article, article_token_set, market_score)
        logger.debug(
            "[%s] joined cluster #%d (containment=%.2f): %.60s",
            article.source_name,
            best_cluster_id,
            best_score,
            article.title,
        )
        return ClusterResult(cluster_id=best_cluster_id, is_new=False, score=best_score)

    cluster_id = _create_new_cluster(db, article, market_score)
    logger.info(
        "[%s] new cluster #%d: %.60s",
        article.source_name,
        cluster_id,
        article.title,
    )
    return ClusterResult(cluster_id=cluster_id, is_new=True, score=0.0)


# ── internals ─────────────────────────────────────────────────────────────────

def _find_best_match(
    article_tokens: set[str],
    candidates: list[sqlite3.Row],
) -> tuple[int | None, float]:
    """
    Scan candidate clusters, return (best_cluster_id, best_score) or (None, 0).
    Stops early once a perfect match (score=1.0) is found.
    """
    best_id    = None
    best_score = 0.0

    for cluster in candidates:
        cluster_tokens = set(cluster["title_tokens"].split())
        score, shared_count = _containment(article_tokens, cluster_tokens)

        if shared_count < MIN_SHARED_TOKENS:
            continue

        if score >= MATCH_THRESHOLD and score > best_score:
            best_score = score
            best_id    = cluster["id"]

            if best_score == 1.0:
                break   # can't do better

    return best_id, best_score


def _create_new_cluster(
    db: sqlite3.Connection,
    article: RawArticle,
    market_score: int,
) -> int:
    keywords = _top_keywords(article.title_tokens)
    return queries.create_cluster(
        db,
        canonical_title=article.title,
        title_tokens=article.title_tokens,
        keywords=keywords,
        score=market_score,
    )


def _update_existing_cluster(
    db: sqlite3.Connection,
    cluster: sqlite3.Row,
    article: RawArticle,
    article_token_set: set[str],
    market_score: int,
) -> None:
    existing_sources = queries.get_cluster_source_ids(db, cluster["id"])
    is_new_source    = article.source_id not in existing_sources

    # Merge keywords: union of existing + new article tokens, capped at MAX_KEYWORDS
    existing_kw_set  = set(cluster["keywords"].split()) if cluster["keywords"] else set()
    merged_keywords  = _top_keywords(
        " ".join(sorted(existing_kw_set | article_token_set))
    )

    queries.update_cluster(
        db,
        cluster_id=cluster["id"],
        score=market_score,
        new_source=is_new_source,
        merged_keywords=merged_keywords,
    )


def _containment(tokens_a: set[str], tokens_b: set[str]) -> tuple[float, int]:
    """
    Containment similarity: |A ∩ B| / min(|A|, |B|).

    Returns (score, shared_count).
    Measures how much of the SMALLER set is covered by the LARGER set.
    Robust when articles have very different token counts.
    """
    if not tokens_a or not tokens_b:
        return 0.0, 0
    shared        = tokens_a & tokens_b
    shared_count  = len(shared)
    min_size      = min(len(tokens_a), len(tokens_b))
    return shared_count / min_size, shared_count


def _top_keywords(tokens_str: str, max_count: int = MAX_KEYWORDS) -> str:
    """
    From a space-joined token string, pick the top N longest tokens.
    Length is a proxy for semantic importance (short tokens tend to be generic).
    Returns space-joined result.
    """
    tokens = sorted(set(tokens_str.split()))
    # Sort by descending length, then alphabetically for determinism
    top    = sorted(tokens, key=lambda t: (-len(t), t))[:max_count]
    return " ".join(sorted(top))   # store in sorted order for consistent hashing

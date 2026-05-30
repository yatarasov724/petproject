"""
Event clustering for news articles.

Algorithm
─────────
For each new (non-duplicate) article:

  1. Load all "open" clusters from the last CLUSTER_WINDOW_HOURS.
  2. Embed the article title with sentence-transformers (paraphrase-multilingual-MiniLM-L12-v2).
  3. For every candidate cluster:
     a. If both the article and the cluster have an embedding: cosine similarity.
     b. Otherwise (missing embedding — existing data or embedder failure): containment fallback.
  4. Accept a cluster if score >= COSINE_THRESHOLD (embedding) or MATCH_THRESHOLD (containment).
  5. Among accepting clusters pick the one with the highest score.
  6. If no cluster accepted → create a new cluster from this article (it becomes the anchor).

Embedding fallback
──────────────────
The embedder may return None when:
- sentence-transformers is not installed.
- The model fails to load (OOM, network error on first download).
- Runtime exception during encode().

In all such cases the code silently falls back to containment similarity so the
pipeline continues to work without embeddings.
"""

import logging
from dataclasses import dataclass
from typing import Any

from app.db.database import DBConnection

from app.ai import embedder
from app.ai.filter import extract_tickers
from app.db import queries
from app.pipeline.normalizer import RawArticle

logger = logging.getLogger(__name__)

# ── configuration ─────────────────────────────────────────────────────────────

CLUSTER_WINDOW_HOURS = 24   # how far back we look for candidate clusters
COSINE_THRESHOLD     = 0.80 # minimum cosine similarity to join a cluster (embedding path)
MATCH_THRESHOLD      = 0.50 # minimum containment score (fallback path, no embedding)
MIN_SHARED_TOKENS    = 2    # minimum overlapping tokens (containment fallback guard)
MAX_KEYWORDS         = 12   # how many tokens to keep in the keywords union


# ── result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ClusterResult:
    cluster_id: int
    is_new:     bool     # True → this article created the cluster
    score:      float    # cosine or containment score (0.0 for new clusters)


# ── public API ────────────────────────────────────────────────────────────────

def find_or_create(
    db: DBConnection,
    article: RawArticle,
    market_score: int = 0,
    *,
    commit: bool = True,
) -> ClusterResult:
    """
    Main entry point.

    Finds the best matching open cluster or creates a new one.
    Does NOT write to seen_articles — that is the caller's responsibility.
    Returns ClusterResult with the cluster_id and whether it was newly created.

    commit=False: skip individual db.commit() calls so the caller can batch
    this write with dedup.record() into one atomic transaction.
    """
    article_token_set = set(article.title_tokens.split())

    if len(article_token_set) < MIN_SHARED_TOKENS:
        logger.debug(
            "[%s] too few tokens (%d) for clustering, creating new cluster: %.60s",
            article.source_name,
            len(article_token_set),
            article.title,
        )
        cluster_id = _create_new_cluster(db, article, market_score, embedding=None, commit=commit)
        return ClusterResult(cluster_id=cluster_id, is_new=True, score=0.0)

    article_embedding = _safe_embed(article.title)

    candidates = queries.find_candidate_clusters(db, within_hours=CLUSTER_WINDOW_HOURS)
    best_cluster_id, best_score = _find_best_match(article_token_set, article_embedding, candidates)

    if best_cluster_id is not None:
        cluster = queries.get_cluster(db, best_cluster_id)
        if cluster is None:
            # Cluster was deleted between find and get (e.g. concurrent cleanup) — treat as new
            cluster_id = _create_new_cluster(db, article, market_score, embedding=article_embedding, commit=commit)
            return ClusterResult(cluster_id=cluster_id, is_new=True, score=0.0)
        _update_existing_cluster(db, cluster, article, article_token_set, market_score, commit=commit)
        logger.debug(
            "[%s] joined cluster #%d (score=%.2f): %.60s",
            article.source_name,
            best_cluster_id,
            best_score,
            article.title,
        )
        return ClusterResult(cluster_id=best_cluster_id, is_new=False, score=best_score)

    cluster_id = _create_new_cluster(db, article, market_score, embedding=article_embedding, commit=commit)
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
    article_embedding: bytes | None,
    candidates: list[Any],
) -> tuple[int | None, float]:
    """
    Scan candidate clusters, return (best_cluster_id, best_score) or (None, 0).

    Uses cosine similarity when both sides have an embedding; falls back to
    token containment otherwise. Stops early once a perfect match (score=1.0) is found.
    """
    best_id    = None
    best_score = 0.0

    for cluster in candidates:
        cluster_emb = cluster["embedding"]
        if article_embedding is not None and cluster_emb is not None:
            score     = embedder.cosine(article_embedding, cluster_emb)
            threshold = COSINE_THRESHOLD
        else:
            cluster_tokens = set(cluster["title_tokens"].split())
            score, shared_count = _containment(article_tokens, cluster_tokens)
            if shared_count < MIN_SHARED_TOKENS:
                continue
            threshold = MATCH_THRESHOLD

        if score >= threshold and score > best_score:
            best_score = score
            best_id    = cluster["id"]

            if best_score >= 1.0:
                break   # can't do better

    return best_id, best_score


def _create_new_cluster(
    db: DBConnection,
    article: RawArticle,
    market_score: int,
    embedding: bytes | None,
    *,
    commit: bool = True,
) -> int:
    keywords = _top_keywords(article.title_tokens)
    tickers = extract_tickers(article.title)
    return queries.create_cluster(
        db,
        canonical_title=article.title,
        title_tokens=article.title_tokens,
        keywords=keywords,
        tickers=",".join(tickers),
        score=market_score,
        embedding=embedding,
        commit=commit,
    )


def _update_existing_cluster(
    db: DBConnection,
    cluster: Any,
    article: RawArticle,
    article_token_set: set[str],
    market_score: int,
    *,
    commit: bool = True,
) -> None:
    existing_sources = queries.get_cluster_source_ids(db, cluster["id"])
    is_new_source    = article.source_id not in existing_sources

    existing_kw_set  = set(cluster["keywords"].split()) if cluster["keywords"] else set()
    merged_keywords  = _top_keywords(
        " ".join(sorted(existing_kw_set | article_token_set))
    )

    existing_tickers = {t for t in (cluster["tickers"] or "").split(",") if t}
    new_tickers      = set(extract_tickers(article.title))
    # Защита от загрязнения тикеров: если у кластера уже есть тикеры и новые
    # тикеры ПОЛНОСТЬЮ отличаются (нет пересечения) — это, вероятно, другая компания,
    # попавшая в кластер из-за общей dividend/earnings-лексики. Не добавляем.
    if existing_tickers and new_tickers and not (existing_tickers & new_tickers):
        logger.debug(
            "ticker contamination guard: cluster #%d has %s, article brings %s — skipping merge",
            cluster["id"],
            existing_tickers,
            new_tickers,
        )
        merged_tickers = ",".join(sorted(existing_tickers))
    else:
        merged_tickers = ",".join(sorted(existing_tickers | new_tickers))

    queries.update_cluster(
        db,
        cluster_id=cluster["id"],
        score=market_score,
        new_source=is_new_source,
        merged_keywords=merged_keywords,
        merged_tickers=merged_tickers,
        commit=commit,
    )


def _safe_embed(text: str) -> bytes | None:
    """Call embedder.embed(), return None on any failure (missing library, OOM, etc.)."""
    try:
        return embedder.embed(text)
    except Exception as exc:
        logger.warning("Embedding failed (%s): %.60s", exc, text)
        return None


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

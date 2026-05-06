"""
Pipeline orchestrator — processes one RawArticle through the full pipeline.

Responsibilities:
  - Own the per-article logic sequence
  - Isolate every article: an exception here never bubbles to the caller
  - Return a typed result so the job can aggregate stats

The orchestrator does NOT own the DB connection or the fetch loop.
Those belong to the scheduler job. One connection per poll cycle,
passed in from the job.

Sequence for each article
──────────────────────────
  1. dedup.check()          — exact hash, then Jaccard near-dedup
  2. scorer.compute_score() — noise floor filter (score < ARTICLE_MIN_SCORE → drop)
  3. clusterer.find_or_create() — join existing or open new cluster
  4. dedup.record()         — persist to seen_articles with cluster_id
  5. queries.get_cluster()  — reload cluster state (source_count updated)
  6. scorer.compute_score() — rescore with actual source_count
  7. publish_decision.decide() — NEW_EVENT / UPDATE / SILENCE
  8. tg.send()              — send to Telegram (if not SILENCE)
"""

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from enum import Enum

from datetime import datetime, timezone, timedelta

from app.ai import analyzer
from app.core import metrics
from app.db import queries
from app.pipeline import dedup, clusterer, scorer
from app.pipeline.normalizer import RawArticle
from app.pipeline.publish_decision import decide, Decision
from app.telegram import client as tg
from app.telegram.formatter import format_message

# Articles older than this are skipped before entering the pipeline.
# Prevents publishing stale RSS entries that appeared late in the feed.
ARTICLE_MAX_AGE_HOURS = 24

logger = logging.getLogger(__name__)


# ── result type ───────────────────────────────────────────────────────────────

class Outcome(str, Enum):
    EXACT_DUP  = "exact_dup"
    NEAR_DUP   = "near_dup"
    NOISE      = "noise"
    SILENCE    = "silence"
    SENT_NEW   = "sent_new"
    SENT_UPDATE= "sent_update"
    SEND_FAIL  = "send_fail"
    ERROR      = "error"


@dataclass(frozen=True)
class ArticleResult:
    outcome:    Outcome
    source:     str
    title:      str     # first 70 chars
    score:      int = 0
    cluster_id: int = 0


# ── public API ────────────────────────────────────────────────────────────────

async def process(db: sqlite3.Connection, article: RawArticle) -> ArticleResult:
    """
    Run one article through the full pipeline.
    Never raises — all exceptions are caught and returned as Outcome.ERROR.
    """
    short = article.title[:70]

    try:
        return await _run(db, article)
    except Exception:
        metrics.inc(metrics.PIPELINE_ERRORS)
        logger.exception(
            "pipeline error",
            extra={
                "event":  "pipeline_error",
                "source": article.source_name,
                "title":  short,
            },
        )
        return ArticleResult(
            outcome=Outcome.ERROR,
            source=article.source_name,
            title=short,
        )


# ── internals ─────────────────────────────────────────────────────────────────

async def _run(db: sqlite3.Connection, article: RawArticle) -> ArticleResult:
    short = article.title[:70]

    # ── step 1: dedup ─────────────────────────────────────────────────────
    dup = dedup.check(db, article)

    if dup.reason == dedup.DupReason.EXACT:
        metrics.inc(metrics.ARTICLES_EXACT_DUP)
        logger.debug(
            "duplicate skipped",
            extra={"event": "dup_skipped", "kind": "exact",
                   "source": article.source_name, "hash": article.raw_hash},
        )
        return ArticleResult(Outcome.EXACT_DUP, article.source_name, short)

    if dup.reason == dedup.DupReason.NEAR:
        metrics.inc(metrics.ARTICLES_NEAR_DUP)
        logger.debug(
            "duplicate skipped",
            extra={"event": "dup_skipped", "kind": "near", "jaccard": round(dup.score, 2),
                   "source": article.source_name},
        )
        return ArticleResult(Outcome.NEAR_DUP, article.source_name, short)

    # ── step 1b: freshness filter by article published_at ────────────────
    # Blocks stale RSS entries that appear late in the feed (2-day-old articles).
    # This check uses the article's own publish date, not when we first saw it.
    age = datetime.now(timezone.utc) - article.published_at
    if age > timedelta(hours=ARTICLE_MAX_AGE_HOURS):
        metrics.inc(metrics.ARTICLES_NOISE)
        logger.debug(
            "article too old",
            extra={"event": "article_stale", "age_hours": round(age.total_seconds() / 3600, 1),
                   "source": article.source_name},
        )
        return ArticleResult(Outcome.NOISE, article.source_name, short)

    # ── step 2: noise pre-filter (before any DB writes) ───────────────────
    pre = scorer.compute_score(article.title, source_count=1)
    if pre.score < scorer.ARTICLE_MIN_SCORE:
        metrics.inc(metrics.ARTICLES_NOISE)
        logger.debug(
            "article noise",
            extra={"event": "article_noise", "score": pre.score,
                   "source": article.source_name},
        )
        return ArticleResult(Outcome.NOISE, article.source_name, short, score=pre.score)

    # ── steps 3+4: cluster + record (atomic) ─────────────────────────────
    # Both writes go into one transaction so a crash between them can't
    # leave a cluster row without a matching seen_articles row.
    try:
        cluster_result = clusterer.find_or_create(db, article, market_score=pre.score, commit=False)
        dedup.record(db, article, cluster_id=cluster_result.cluster_id, commit=False)
        db.commit()
    except Exception:
        db.rollback()
        raise

    metrics.inc(
        metrics.CLUSTERS_CREATED if cluster_result.is_new else metrics.CLUSTERS_UPDATED
    )
    logger.debug(
        "cluster %s",
        "created" if cluster_result.is_new else "updated",
        extra={
            "event":      "cluster_created" if cluster_result.is_new else "cluster_updated",
            "cluster_id": cluster_result.cluster_id,
            "source":     article.source_name,
            "containment": round(cluster_result.score, 2),
        },
    )
    metrics.inc(metrics.ARTICLES_PROCESSED)
    logger.debug(
        "article seen",
        extra={
            "event":      "article_seen",
            "source":     article.source_name,
            "cluster_id": cluster_result.cluster_id,
            "hash":       article.raw_hash,
        },
    )

    # ── step 5 + 6: reload cluster, rescore with real source_count ────────
    cluster = queries.get_cluster(db, cluster_result.cluster_id)
    if cluster is None:
        # Should never happen — we just committed the cluster above.
        raise RuntimeError(f"cluster {cluster_result.cluster_id} vanished after commit")
    score_result = scorer.compute_score(
        article.title,
        source_count=cluster["source_count"],
    )

    # ── step 7: publish decision ──────────────────────────────────────────
    pub = decide(cluster, score_result)

    if pub.decision == Decision.SILENCE:
        metrics.inc(metrics.EVENTS_SILENCED)
        logger.debug(
            "event silenced",
            extra={
                "event":      "event_silenced",
                "cluster_id": cluster["id"],
                "score":      score_result.score,
                "reason":     pub.reason,
                "source":     article.source_name,
            },
        )
        return ArticleResult(
            Outcome.SILENCE, article.source_name, short,
            score=score_result.score, cluster_id=cluster["id"],
        )

    # ── step 8: fire AI analysis in the background (non-blocking) ───────────
    # Only create the task when an API key is configured; otherwise AI is a no-op
    # and there is no point scheduling a task that will return None immediately.
    from app.core.config import settings as _settings
    ai_task: "asyncio.Task | None" = (
        asyncio.create_task(analyzer.analyze(article.title, article.content))
        if _settings.openrouter_api_key else None
    )

    # ── step 9: send immediately — no AI yet ──────────────────────────────────
    msg_id = await tg.send(
        db=db,
        cluster=cluster,
        score_result=score_result,
        pub_decision=pub,
        ai_analysis=None,
    )
    ok = msg_id is not None

    if ok:
        counter = metrics.EVENTS_PUBLISHED if pub.decision == Decision.NEW_EVENT else metrics.EVENTS_UPDATED
        metrics.inc(counter)
        logger.info(
            "event published",
            extra={
                "event":      "event_published",
                "decision":   pub.decision.value,
                "cluster_id": cluster["id"],
                "score":      score_result.score,
                "event_type": score_result.event_type.value,
                "sources":    cluster["source_count"],
                "source":     article.source_name,
                "title":      short,
            },
        )
    # TG_SENT_OK / TG_SENT_FAIL are tracked inside tg.send() — not duplicated here

    # ── step 10: schedule AI enrichment edit when task completes ─────────────
    # msg_id is truthy only for real Telegram messages (not DRY_RUN, not failure).
    if ai_task and msg_id:
        asyncio.create_task(
            _enrich_with_ai(ai_task, msg_id, cluster, score_result, pub.decision)
        )
    elif ai_task:
        ai_task.cancel()

    outcome = (
        (Outcome.SENT_NEW if pub.decision == Decision.NEW_EVENT else Outcome.SENT_UPDATE)
        if ok else Outcome.SEND_FAIL
    )
    return ArticleResult(
        outcome, article.source_name, short,
        score=score_result.score, cluster_id=cluster["id"],
    )


async def _enrich_with_ai(
    ai_task: "asyncio.Task[analyzer.Optional[analyzer.AIAnalysis]]",
    msg_id: int,
    cluster: sqlite3.Row,
    score_result: scorer.ScoreResult,
    decision: Decision,
) -> None:
    """
    Background task: await the AI result and edit the already-sent Telegram message.
    Never raises — all failures are logged and swallowed.
    """
    try:
        ai = await ai_task
        if ai is None:
            return
        text = format_message(cluster, score_result, decision, ai_analysis=ai)
        await tg.edit_message(msg_id, text)
        logger.info(
            "ai enrichment applied",
            extra={"event": "ai_enriched", "msg_id": msg_id},
        )
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.warning("ai enrichment failed for msg_id=%d", msg_id, exc_info=True)

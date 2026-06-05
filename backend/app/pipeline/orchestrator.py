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
  6b. dup guard             — check telegram_sends to prevent same-cluster retries
                              and cross-cluster near-dup publishes
  7. publish_decision.decide() — NEW_EVENT / UPDATE / SILENCE
  8. analyzer.analyze()     — AI market relevance gate (awaited); если API ключ не задан
                              или AI недоступен — пропускаем гейт; если AI вернул
                              пустой affects (нет влияния на рынок) → SILENCE
  9. tg.send()              — send to Telegram with AI analysis included (if not SILENCE)
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.ai import analyzer, embedder
from app.bot.portfolio import notify as _notify_portfolio
from app.core import metrics
from app.core.config import settings
from app.db import queries
from app.db.database import DBConnection
from app.pipeline import dedup, clusterer, scorer
from app.pipeline.normalizer import RawArticle
from app.pipeline.price_history import capture_price_snapshot, get_correlations
from app.pipeline.publish_decision import decide, Decision, PublishDecision, COOLDOWN_HOURS
from app.pipeline.relevance import is_russia_relevant
from app.pipeline.ticker_validator import validate_tickers
from app.telegram import client as tg

# Articles older than this are skipped before entering the pipeline.
# Prevents publishing stale RSS entries that appeared late in the feed.
ARTICLE_MAX_AGE_HOURS = 24

# Cosine similarity threshold for cross-cluster dup guard when embeddings are available.
# Lower than clusterer.COSINE_THRESHOLD (0.80) to catch near-duplicate clusters that slipped
# through the dedup stage due to differently-worded headlines from different sources.
DUP_GUARD_COSINE_THRESHOLD = 0.75
DUP_GUARD_HOURS = 12  # window for cross-cluster duplicate guard

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

async def process(db: DBConnection, article: RawArticle) -> ArticleResult:
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

async def _run(db: DBConnection, article: RawArticle) -> ArticleResult:
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

    # ── step 6b: pre-publish dup guard (new clusters only) ───────────────
    # Two failure modes bypass the normal cooldown check and cause duplicates:
    #
    # (a) Same cluster resent: tg.send() timed out after Telegram delivered the
    #     message — mark_cluster_sent() was never called, so cluster stays 'new'.
    #     Next poll cycle sees status='new' and re-publishes.
    #
    # (b) Cross-cluster same event: two sources used different enough wording
    #     to escape near-dedup (Jaccard < 0.35) AND containment clustering
    #     (< 0.50), each creating its own 'new' cluster — both publish.
    #
    # Both are only possible when status='new'; published/updated clusters
    # are already guarded by the cooldown rule in decide().
    if cluster["status"] == "new":
        if queries.has_recent_send_attempt(db, cluster["id"], within_hours=COOLDOWN_HOURS):
            metrics.inc(metrics.EVENTS_SILENCED)
            logger.info(
                "dup guard: send already attempted for cluster #%d, silencing",
                cluster["id"],
                extra={"event": "dup_guard_same_cluster", "cluster_id": cluster["id"]},
            )
            return ArticleResult(
                Outcome.SILENCE, article.source_name, short,
                score=score_result.score, cluster_id=cluster["id"],
            )

        sent_clusters = queries.get_recently_sent_clusters(
            db, within_hours=DUP_GUARD_HOURS, exclude_cluster_id=cluster["id"]
        )
        cluster_tokens   = cluster["title_tokens"]
        cluster_emb      = cluster["embedding"]
        for sent_row in sent_clusters:
            sent_tokens = sent_row["title_tokens"]
            sent_emb    = sent_row["embedding"]

            if cluster_emb is not None and sent_emb is not None:
                cos = embedder.cosine(cluster_emb, sent_emb)
                is_near_dup = cos >= DUP_GUARD_COSINE_THRESHOLD
                if is_near_dup:
                    metrics.inc(metrics.EVENTS_SILENCED)
                    logger.info(
                        "dup guard: cross-cluster near-dup for cluster #%d "
                        "(cosine=%.2f), silencing",
                        cluster["id"],
                        cos,
                        extra={"event": "dup_guard_cross_cluster", "cluster_id": cluster["id"]},
                    )
                    return ArticleResult(
                        Outcome.SILENCE, article.source_name, short,
                        score=score_result.score, cluster_id=cluster["id"],
                    )
            else:
                j = dedup.jaccard(cluster_tokens, sent_tokens)
                c, shared = dedup.containment(cluster_tokens, sent_tokens)
                is_near_dup = j >= dedup.JACCARD_THRESHOLD or (
                    shared >= dedup.CONTAINMENT_MIN_SHARED and c >= dedup.CONTAINMENT_THRESHOLD
                )
                if is_near_dup:
                    metrics.inc(metrics.EVENTS_SILENCED)
                    logger.info(
                        "dup guard: cross-cluster near-dup for cluster #%d "
                        "(jaccard=%.2f containment=%.2f shared=%d), silencing",
                        cluster["id"],
                        j,
                        c,
                        shared,
                        extra={"event": "dup_guard_cross_cluster", "cluster_id": cluster["id"]},
                    )
                    return ArticleResult(
                        Outcome.SILENCE, article.source_name, short,
                        score=score_result.score, cluster_id=cluster["id"],
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

    # ── step 7.5: Russia/MOEX relevance gate ─────────────────────────────────
    # Hard filter: only events touching the Russian market reach Telegram.
    # Checked here (after clustering/scoring, before AI) so:
    #   - Clustering/dedup still learns from all articles.
    #   - AI calls are not wasted on irrelevant events.
    if not is_russia_relevant(cluster):
        metrics.inc(metrics.EVENTS_SILENCED)
        logger.info(
            "relevance gate: cluster #%d not RF/MOEX-relevant — silenced",
            cluster["id"],
            extra={
                "event":      "relevance_gate_silence",
                "cluster_id": cluster["id"],
                "source":     article.source_name,
                "title":      short,
            },
        )
        return ArticleResult(
            Outcome.SILENCE, article.source_name, short,
            score=score_result.score, cluster_id=cluster["id"],
        )

    # ── step 8: send immediately, enrich with AI in background ──────────────
    # Fire-and-forget: publish base message without waiting for AI (~0ms delay),
    # then _ai_enrich() edits the message in place once the LLM responds.
    # Filtering is handled by scorer + is_russia_relevant; AI is now decorative.
    # Валидация тикеров: убираем те, чьи ключевые слова отсутствуют в заголовке.
    # Это предотвращает публикацию тикеров, попавших в кластер через загрязнение.
    safe_tickers = validate_tickers(cluster["tickers"], cluster["canonical_title"])
    if safe_tickers != (cluster["tickers"] or ""):
        cluster = dict(cluster)
        cluster["tickers"] = safe_tickers or None

    correlations = get_correlations(db, score_result.event_type.value, cluster["tickers"] or "")
    if correlations:
        logger.info(
            "correlations attached cluster_id=%d event_type=%s count=%d",
            cluster["id"], score_result.event_type.value, len(correlations),
            extra={"event": "correlations_attached", "cluster_id": cluster["id"]},
        )
    msg_id = await tg.send(
        db=db,
        cluster=cluster,
        score_result=score_result,
        pub_decision=pub,
        ai_analysis=None,
        correlations=correlations,
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
        tickers_raw = cluster["tickers"] or ""
        if msg_id and settings.openrouter_api_key:
            asyncio.create_task(
                _ai_enrich(
                    article.title, article.content, cluster, score_result, pub, msg_id,
                    correlations, tickers_raw=tickers_raw,
                    canonical_title=cluster["canonical_title"],
                )
            )
        asyncio.create_task(
            _detect_unknown_company(cluster["canonical_title"], bool(cluster["tickers"]))
        )
        if cluster["tickers"]:
            asyncio.create_task(
                capture_price_snapshot(cluster["id"], cluster["tickers"], score_result.event_type.value)
            )

    outcome = (
        (Outcome.SENT_NEW if pub.decision == Decision.NEW_EVENT else Outcome.SENT_UPDATE)
        if ok else Outcome.SEND_FAIL
    )
    return ArticleResult(
        outcome, article.source_name, short,
        score=score_result.score, cluster_id=cluster["id"],
    )


import re as _re

# Паттерны, указывающие на упоминание конкретной компании в дивидендной/корпоративной новости.
# Если компания не распознана (нет тикера) — шлём alert в ops-чат.
_COMPANY_PATTERNS = [
    # "СД МГКЛ рекомендовал", "Совет директоров НМТП объявил"
    _re.compile(r'\bС[Дд]\.?\s+([А-ЯЁ]{2,6})\b'),
    # "МГКЛ: ДИВИДЕНДЫ =", "НМТП: ДИВИДЕНДЫ"
    _re.compile(r'\b([А-ЯЁ]{2,6})[:\s]+ДИВИДЕНДЫ'),
    # "[А-ЯЁ]{2,6} - ДИВИДЕНДЫ" (формат Smartlab)
    _re.compile(r'[-–]\s*([А-ЯЁ]{2,6})\s*[-–:]\s*ДИВИДЕНДЫ'),
]


async def _detect_unknown_company(title: str, has_tickers: bool) -> None:
    """
    If title matches a 'company dividend/earnings' pattern but no ticker was found,
    send an ops alert so the admin can add the ticker to the keyword list.
    Never raises.
    """
    if has_tickers:
        return  # тикер уже есть — всё хорошо
    try:
        for pattern in _COMPANY_PATTERNS:
            m = pattern.search(title)
            if m:
                abbr = m.group(1)
                from app.core.alerting import send_ops
                await send_ops(
                    f"⚠️ Незнакомая компания: «{abbr}»\n"
                    f"Добавьте тикер в filter.py:\n"
                    f"«{title[:100]}»"
                )
                logger.info(
                    "unknown_company_detected abbr=%s title=%.80s",
                    abbr, title,
                    extra={"event": "unknown_company", "abbr": abbr},
                )
                return  # один алерт за статью
    except Exception:
        logger.warning("unknown company detection failed", exc_info=True)


async def _ai_enrich(
    title: str,
    content: str,
    cluster: Any,
    score_result: scorer.ScoreResult,
    pub: PublishDecision,
    message_id: int,
    correlations: list | None = None,
    *,
    tickers_raw: str = "",
    canonical_title: str = "",
) -> None:
    """
    Background task: call AI, then edit the already-sent Telegram message.
    Runs concurrently with the next poll cycle — never blocks article processing.
    """
    try:
        recent_context: list[str] = []
        from app.db.database import get_db as _get_db
        from app.db import queries as _queries
        _db = _get_db()
        try:
            ticker_list = [t.strip() for t in tickers_raw.split(",") if t.strip()]
            ticker_ctx = _queries.get_recent_cluster_titles_for_tickers(_db, ticker_list) if tickers_raw else []
            embed_ctx = (
                _queries.get_similar_clusters_by_embedding(
                    _db, cluster["embedding"], exclude_id=cluster["id"]
                )
                if cluster.get("embedding")
                else []
            )
            seen = set(ticker_ctx)
            extra = [s for s in embed_ctx if s not in seen]
            recent_context = (ticker_ctx + extra)[:7]
        finally:
            _db.close()

        ai_analysis = await analyzer.analyze(title, content, recent_context=recent_context)
        if ai_analysis is None:
            # AI failed — delete the raw message so unedited posts don't appear in the channel.
            from app.telegram import client as _tg
            await _tg.delete_message(message_id)
            logger.warning(
                "AI enrich failed, deleting raw message: cluster_id=%d message_id=%d",
                cluster["id"], message_id,
                extra={"event": "ai_enrich_deleted", "cluster_id": cluster["id"]},
            )
            return

        # Edit the channel message with AI-enriched content.
        from app.telegram.formatter import format_message as _fmt
        from app.telegram import client as _tg
        enriched_text = _fmt(cluster, score_result, pub.decision, ai_analysis, correlations)
        await _tg.edit_message(message_id, enriched_text)

        if tickers_raw:
            from app.bot.portfolio import notify_with_ai
            await notify_with_ai(
                tickers_raw, ai_analysis, cluster["id"], canonical_title,
                correlations=correlations,
                event_type=score_result.event_type.value,
                score=score_result.score,
            )

        logger.info(
            "AI enrich ok: cluster_id=%d",
            cluster["id"],
            extra={"event": "ai_enrich_ok", "cluster_id": cluster["id"]},
        )
    except Exception:
        logger.warning(
            "AI enrich failed: cluster_id=%d",
            cluster["id"],
            exc_info=True,
        )

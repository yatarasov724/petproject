"""
Publish decision engine.

Three possible decisions:

  NEW_EVENT — cluster is new and score >= threshold.
              First time this event is published.

  UPDATE    — cluster was already published, cooldown has passed,
              AND the event has grown significantly (more sources or higher score).

  SILENCE   — everything else: noise, stale news, cooldown active, no meaningful growth.

Decision rules (in evaluation order)
──────────────────────────────────────
  1. score < PUBLISH_THRESHOLD                   → SILENCE  (noise floor)
  2. cluster is new AND first_seen_at > 24h      → SILENCE  (stale unnoticed event)
  3. cluster.status == 'new'                     → NEW_EVENT
  4. cooldown_until > now                        → SILENCE  (too soon)
  5. source_count >= UPDATE_SOURCE_FLOOR         → UPDATE   (cross-source confirmation)
  6. score >= published_score + UPDATE_SCORE_DELTA → UPDATE (material new info)
  7. else                                        → SILENCE  (event stalled)

Freshness policy
─────────────────
  Rule 2 only applies to clusters that have NOT been published yet (status='new').
  If a cluster was already published when fresh, it can still receive UPDATE decisions
  after the cooldown — even if the first_seen_at is old. This is intentional: a story
  that was published fresh can legitimately develop (e.g. sanctions expanded, deal
  value raised). The staleness guard only prevents us from publishing a forgotten
  event as if it were breaking news.

What NOT to do in MVP
──────────────────────
- No AI-generated "why this matters" text (needs LLM)
- No per-sector impact analysis
- No confidence interval or probability estimate
- No lookahead into future articles
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum

from app.pipeline.scorer import PUBLISH_THRESHOLD, ScoreResult

logger = logging.getLogger(__name__)

# ── configuration ─────────────────────────────────────────────────────────────

COOLDOWN_HOURS        = 2    # minimum gap between sends for the same cluster
UPDATE_SCORE_DELTA    = 15   # score must grow by this much since last send to trigger UPDATE
UPDATE_SOURCE_FLOOR   = 3    # OR: cluster reached this many sources (cross-confirmation)
FRESHNESS_HOURS       = 24   # unpublished cluster older than this → silenced as stale


# ── types ─────────────────────────────────────────────────────────────────────

class Decision(str, Enum):
    NEW_EVENT = "NEW_EVENT"
    UPDATE    = "UPDATE"
    SILENCE   = "SILENCE"


@dataclass(frozen=True)
class PublishDecision:
    decision:   Decision
    cluster_id: int
    score:      int
    reason:     str     # one-line explanation, used in logs and telegram_sends


# ── public API ────────────────────────────────────────────────────────────────

def decide(
    cluster: sqlite3.Row,
    score_result: ScoreResult,
) -> PublishDecision:
    """
    Given the current cluster state and the computed score, return a decision.
    Does NOT write to the DB — caller must call queries.mark_cluster_sent() after a send.
    """
    cluster_id      = cluster["id"]
    status          = cluster["status"]
    source_count    = cluster["source_count"]
    published_score = cluster["published_score"] or 0

    def _silence(reason: str) -> PublishDecision:
        return PublishDecision(
            decision=Decision.SILENCE,
            cluster_id=cluster_id,
            score=score_result.score,
            reason=reason,
        )

    # ── rule 1: noise floor ───────────────────────────────────────────────
    if score_result.score < PUBLISH_THRESHOLD:
        return _silence(
            f"score {score_result.score} < threshold {PUBLISH_THRESHOLD}"
        )

    # ── rule 2: freshness check for unpublished clusters ──────────────────
    # Only applies when the cluster has never been sent (status='new').
    # Prevents publishing stale stories as breaking news.
    if status == "new" and _is_stale(cluster):
        return _silence(
            f"stale: unpublished cluster older than {FRESHNESS_HOURS}h "
            f"(first_seen_at={cluster['first_seen_at']})"
        )

    # ── rule 3: brand new cluster ─────────────────────────────────────────
    if status == "new":
        return PublishDecision(
            decision=Decision.NEW_EVENT,
            cluster_id=cluster_id,
            score=score_result.score,
            reason=(
                f"new event · score={score_result.score} "
                f"type={score_result.event_type.value}"
            ),
        )

    # ── rule 4: cooldown ──────────────────────────────────────────────────
    if _in_cooldown(cluster):
        return _silence(
            f"cooldown active until {cluster['cooldown_until']}"
        )

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

    # ── rule 6: meaningful score growth ───────────────────────────────────
    delta = score_result.score - published_score
    if delta >= UPDATE_SCORE_DELTA:
        return PublishDecision(
            decision=Decision.UPDATE,
            cluster_id=cluster_id,
            score=score_result.score,
            reason=(
                f"score grew {published_score}→{score_result.score} "
                f"(+{delta})"
            ),
        )

    # ── rule 7: no meaningful change ──────────────────────────────────────
    return _silence(
        f"no significant change · score={score_result.score} "
        f"published_score={published_score} sources={source_count}"
    )


# ── helpers ───────────────────────────────────────────────────────────────────

def _in_cooldown(cluster: sqlite3.Row) -> bool:
    raw = cluster["cooldown_until"]
    if not raw:
        return False
    try:
        until = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < until
    except (ValueError, TypeError):
        return False


def _is_stale(cluster: sqlite3.Row) -> bool:
    """
    Returns True if the cluster was first seen more than FRESHNESS_HOURS ago.
    Used to silence unpublished clusters that were never promoted (low score clusters
    that only became publishable much later, long after the news was fresh).
    """
    raw = cluster["first_seen_at"]
    if not raw:
        return False
    try:
        first_seen = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - first_seen
        return age > timedelta(hours=FRESHNESS_HOURS)
    except (ValueError, TypeError):
        return False

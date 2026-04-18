"""
In-memory counters for MVP observability.

No Prometheus, no external deps — just a module-level dict
that lives for the duration of the process.

Usage:
  from app.core import metrics
  metrics.inc("articles_fetched", n=len(articles))
  metrics.inc("events_published")

Counters are accumulated across poll cycles and dumped to the log
at the end of each poll_job via log_snapshot(). They are NOT reset
between polls — they're process-lifetime totals. This makes it easy
to see growth trends in the log output.
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── counter store ─────────────────────────────────────────────────────────────

_counts: dict[str, int] = defaultdict(int)
_process_start = datetime.now(timezone.utc)


def inc(name: str, n: int = 1) -> None:
    """Increment a named counter. Thread-safe for single-threaded asyncio use."""
    _counts[name] += n


def get(name: str) -> int:
    return _counts[name]


def snapshot() -> dict[str, int]:
    """Return a copy of all counters. Useful for /health endpoint."""
    return dict(_counts)


def log_snapshot(poll_stats: dict[str, int] | None = None) -> None:
    """
    Emit current counter totals as a structured log line.
    Call at the end of each poll_job.
    poll_stats: per-poll stats dict (fetched, new, update, etc.)
    """
    uptime_s = int((datetime.now(timezone.utc) - _process_start).total_seconds())
    extra = {
        "event":    "metrics_snapshot",
        "uptime_s": uptime_s,
        **{f"total_{k}": v for k, v in _counts.items()},
    }
    if poll_stats:
        extra.update({f"poll_{k}": v for k, v in poll_stats.items()})

    logger.info("metrics", extra=extra)


# ── named counter keys (single source of truth) ───────────────────────────────
# Import these to avoid string typos across modules.

ARTICLES_FETCHED    = "articles_fetched"
ARTICLES_EXACT_DUP  = "articles_exact_dup"
ARTICLES_NEAR_DUP   = "articles_near_dup"
ARTICLES_NOISE      = "articles_noise"
ARTICLES_PROCESSED  = "articles_processed"

CLUSTERS_CREATED    = "clusters_created"
CLUSTERS_UPDATED    = "clusters_updated"

EVENTS_PUBLISHED    = "events_published"
EVENTS_UPDATED      = "events_updated"
EVENTS_SILENCED     = "events_silenced"

TG_SENT_OK          = "tg_sent_ok"
TG_SENT_FAIL        = "tg_sent_fail"
TG_RATE_LIMITED     = "tg_rate_limited"

SOURCES_BACKOFF     = "sources_backoff"
SOURCES_DEAD        = "sources_dead"

PIPELINE_ERRORS     = "pipeline_errors"
CLEANUP_RUNS        = "cleanup_runs"

"""
Logging configuration.

Two modes:
  text (default) — human-readable, good for local dev
  json           — single-line JSON per record, good for log aggregators

Activate JSON mode: set LOG_FORMAT=json in environment.

Key log events use extra={} fields for structured filtering:
  extra={"event": "event_published", "cluster_id": 42, "score": 65}

This makes it trivial to filter in any log aggregator:
  jq 'select(.event == "event_published")'
"""

import json
import logging
import sys
from datetime import datetime, timezone


# Fields that are part of LogRecord but not useful as structured data
_SKIP_FIELDS = frozenset({
    "args", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelno", "lineno", "module", "msecs", "msg",
    "name", "pathname", "process", "processName", "relativeCreated",
    "stack_info", "taskName", "thread", "threadName", "message",
})


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict = {
            "ts":     _now_iso(),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)

        # Attach any extra={} fields the caller passed
        for key, val in record.__dict__.items():
            if key not in _SKIP_FIELDS and not key.startswith("_"):
                out[key] = val

        return json.dumps(out, ensure_ascii=False, default=str)


class _TextFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def configure(json_logs: bool = False, level: str = "INFO") -> None:
    """
    Call once at startup, before any logger.* calls.
    Clears existing handlers so this is safe to call on reload.
    """
    formatter = _JsonFormatter() if json_logs else _TextFormatter()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Third-party noise reduction
    logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
    logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.client").setLevel(logging.WARNING)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

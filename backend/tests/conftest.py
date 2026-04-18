"""
Shared fixtures for the test suite.

Design decisions:
- In-memory SQLite (:memory:) — no file I/O, no cleanup, no concurrent-write issues.
- Schema applied via executescript from the real schema.sql — keeps tests in sync
  with production DDL automatically.
- make_article() factory — builds a RawArticle with sensible defaults so each test
  only overrides what it actually cares about.
"""

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.pipeline.normalizer import RawArticle

_SCHEMA_PATH = Path(__file__).parent.parent / "app" / "db" / "schema.sql"


@pytest.fixture()
def db() -> sqlite3.Connection:
    """In-memory SQLite connection with the production schema applied."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_PATH.read_text())
    # Seed a minimal rss_sources row so seen_articles FK is satisfiable
    conn.execute(
        "INSERT INTO rss_sources (id, name, url) VALUES (1, 'TestFeed', 'http://test.local/rss')"
    )
    conn.execute(
        "INSERT INTO rss_sources (id, name, url) VALUES (2, 'TestFeed2', 'http://test2.local/rss')"
    )
    conn.commit()
    yield conn
    conn.close()


def make_article(
    *,
    source_id: int = 1,
    source_name: str = "TestFeed",
    title: str = "ЦБ повысил ключевую ставку до 21 процента",
    url: str = "http://test.local/article/1",
    published_at: datetime | None = None,
    raw_hash: str | None = None,
    title_tokens: str | None = None,
) -> RawArticle:
    """
    Factory for RawArticle test instances.
    Computes raw_hash and title_tokens from the title if not provided explicitly.
    """
    from app.pipeline.normalizer import tokenize

    if published_at is None:
        published_at = datetime.now(timezone.utc)

    tokens = tokenize(title)
    computed_tokens = " ".join(tokens)

    if title_tokens is None:
        title_tokens = computed_tokens

    if raw_hash is None:
        date_hour = published_at.strftime("%Y%m%d%H")
        fingerprint = computed_tokens + date_hour
        raw_hash = hashlib.md5(fingerprint.encode()).hexdigest()

    return RawArticle(
        source_id=source_id,
        source_name=source_name,
        title=title,
        url=url,
        published_at=published_at,
        raw_hash=raw_hash,
        title_tokens=title_tokens,
    )

"""
Shared fixtures for the test suite.

Design decisions:
- Connects to a running PostgreSQL instance (start with: docker compose up -d postgres).
- TEST_DATABASE_URL env var overrides the default DSN.
- Schema applied once per session; tables truncated between tests for isolation.
- make_article() factory — builds a RawArticle with sensible defaults.
"""

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

from app.db.database import DBConnection, _split_sql
from app.pipeline.normalizer import RawArticle

_SCHEMA_PATH = Path(__file__).parent.parent / "app" / "db" / "schema.sql"

_TEST_DSN = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost/moex_assistant_test",
)


@pytest.fixture(scope="session")
def _init_test_db():
    """Create the test database and apply the schema once per session."""
    base, _, dbname = _TEST_DSN.rpartition("/")
    admin = psycopg2.connect(base + "/postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
        cur.execute(f"CREATE DATABASE {dbname}")
    admin.close()

    conn = psycopg2.connect(_TEST_DSN)
    conn.autocommit = False
    with conn.cursor() as cur:
        for stmt in _split_sql(_SCHEMA_PATH.read_text()):
            cur.execute(stmt)
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _use_test_db(_init_test_db, monkeypatch):
    """
    Point get_db() to the test database for every test.

    Depends on _init_test_db to ensure the test database exists before the
    patch takes effect. app.db.database.get_db reads settings.database_url at
    call time, so we patch that attribute to the test DSN before each test and
    restore it after (monkeypatch handles restoration automatically).
    """
    import app.db.database as _db_module
    monkeypatch.setattr(_db_module.settings, "database_url", _TEST_DSN)


@pytest.fixture(autouse=True)
def _no_real_ai():
    """
    Prevent background OpenRouter HTTP calls in every test.

    Returns None so _ai_enrich() exits early (no format_message, no editMessageText).
    Tests that need a real AIAnalysis object override this patch explicitly.
    """
    with patch("app.ai.analyzer.analyze", return_value=None):
        yield


@pytest.fixture(autouse=True)
def _no_real_signal_ai():
    """Prevent OpenRouter calls from _ai_long_term in every test."""
    from unittest.mock import AsyncMock
    with patch("app.ai.signal._ai_long_term", new=AsyncMock(return_value=("", ""))):
        yield


@pytest.fixture(autouse=True)
def _no_real_embedder():
    """
    Disable sentence-transformers inference in tests.

    Returns None so clusterer falls back to containment similarity.
    Tests that exercise the embedding path must override this patch explicitly.
    """
    with patch("app.ai.embedder.embed", return_value=None):
        yield


@pytest.fixture()
def db(_init_test_db) -> DBConnection:
    """
    Per-test connection with seed sources.
    All changes are rolled back via TRUNCATE after the test.
    """
    conn = psycopg2.connect(_TEST_DSN)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO rss_sources (id, name, url) OVERRIDING SYSTEM VALUE "
            "VALUES (1, 'TestFeed', 'http://test.local/rss')"
        )
        cur.execute(
            "INSERT INTO rss_sources (id, name, url) OVERRIDING SYSTEM VALUE "
            "VALUES (2, 'TestFeed2', 'http://test2.local/rss')"
        )
        cur.execute("SELECT setval('rss_sources_id_seq', 2)")
    conn.commit()

    yield DBConnection(conn)

    # Rollback any aborted transaction before cleanup (test may have failed mid-way)
    conn.rollback()
    # Truncate in FK-safe order (children before parents)
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE telegram_sends, seen_articles, portfolio_subscriptions, "
            "price_snapshots, event_clusters, rss_sources, subscriptions, "
            "user_settings, users, digest_sends, "
            "calendar_notifications_sent, corporate_events "
            "RESTART IDENTITY CASCADE"
        )
    conn.commit()
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

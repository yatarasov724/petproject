"""
Tests for the Admin API (MVP-7).

Uses FastAPI TestClient with a PostgreSQL test DB injected via dependency override.
No real network calls, no scheduler, no Telegram.
"""

import os
import psycopg2
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.api.routes.admin import _get_db
from app.db.database import DBConnection

_ADMIN_KEY = "test-secret-key"

_TEST_DSN = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost/moex_assistant_test",
)


@pytest.fixture()
def mem_db(_init_test_db) -> DBConnection:
    """PostgreSQL connection with one seed source; cleaned up after each test."""
    conn = psycopg2.connect(_TEST_DSN)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO rss_sources (id, name, url) OVERRIDING SYSTEM VALUE "
            "VALUES (1, 'TestFeed', 'https://test.local/rss')"
        )
        cur.execute("SELECT setval('rss_sources_id_seq', 1)")
    conn.commit()

    db = DBConnection(conn)
    yield db

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE telegram_sends, seen_articles, portfolio_subscriptions, "
            "price_snapshots, event_clusters, rss_sources RESTART IDENTITY"
        )
    conn.commit()
    conn.close()


@pytest.fixture()
def client(mem_db, monkeypatch):
    """TestClient with DB and secret_key overrides."""
    monkeypatch.setattr("app.core.config.settings.secret_key", _ADMIN_KEY)
    app.dependency_overrides[_get_db] = lambda: mem_db
    yield TestClient(app, raise_server_exceptions=True)
    app.dependency_overrides.clear()


def _auth() -> dict:
    return {"X-Admin-Key": _ADMIN_KEY}


def _wrong_key() -> dict:
    return {"X-Admin-Key": "wrong-key"}


# ── GET /admin/sources ────────────────────────────────────────────────────────

class TestListSources:
    def test_returns_sources(self, client):
        resp = client.get("/admin/sources", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["name"] == "TestFeed"

    def test_response_shape(self, client):
        resp = client.get("/admin/sources", headers=_auth())
        item = resp.json()[0]
        for field in ("id", "name", "url", "enabled", "status", "error_count"):
            assert field in item, f"Missing field: {field}"

    def test_unauthorized_without_key(self, client):
        resp = client.get("/admin/sources")
        assert resp.status_code == 422   # missing required header → Unprocessable

    def test_forbidden_with_wrong_key(self, client):
        resp = client.get("/admin/sources", headers=_wrong_key())
        assert resp.status_code == 403


# ── POST /admin/sources ───────────────────────────────────────────────────────

class TestCreateSource:
    def test_creates_source(self, client):
        resp = client.post(
            "/admin/sources",
            json={"name": "NewFeed", "url": "https://new.example.com/rss"},
            headers=_auth(),
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == "NewFeed"
        assert body["url"] == "https://new.example.com/rss"
        assert body["enabled"] is True
        assert body["status"] == "ok"

    def test_new_source_appears_in_list(self, client):
        client.post(
            "/admin/sources",
            json={"name": "NewFeed2", "url": "https://new2.example.com/rss"},
            headers=_auth(),
        )
        resp = client.get("/admin/sources", headers=_auth())
        names = {s["name"] for s in resp.json()}
        assert "NewFeed2" in names

    def test_duplicate_name_returns_409(self, client):
        resp = client.post(
            "/admin/sources",
            json={"name": "TestFeed", "url": "https://other.example.com/rss"},
            headers=_auth(),
        )
        assert resp.status_code == 409

    def test_duplicate_url_returns_409(self, client):
        resp = client.post(
            "/admin/sources",
            json={"name": "AnotherFeed", "url": "https://test.local/rss"},
            headers=_auth(),
        )
        assert resp.status_code == 409

    def test_invalid_url_returns_422(self, client):
        resp = client.post(
            "/admin/sources",
            json={"name": "BadUrl", "url": "not-a-url"},
            headers=_auth(),
        )
        assert resp.status_code == 422

    def test_forbidden_without_key(self, client):
        resp = client.post(
            "/admin/sources",
            json={"name": "X", "url": "https://x.com/rss"},
            headers=_wrong_key(),
        )
        assert resp.status_code == 403


# ── DELETE /admin/sources/{id} ────────────────────────────────────────────────

class TestDisableSource:
    def test_disable_sets_enabled_false(self, client, mem_db):
        resp = client.delete("/admin/sources/1", headers=_auth())
        assert resp.status_code == 204

        row = mem_db.execute("SELECT enabled FROM rss_sources WHERE id=%s", (1,)).fetchone()
        assert row["enabled"] == 0

    def test_row_still_exists_after_disable(self, client, mem_db):
        client.delete("/admin/sources/1", headers=_auth())
        row = mem_db.execute("SELECT id FROM rss_sources WHERE id=%s", (1,)).fetchone()
        assert row is not None, "Row must not be physically deleted"

    def test_nonexistent_returns_404(self, client):
        resp = client.delete("/admin/sources/999", headers=_auth())
        assert resp.status_code == 404

    def test_forbidden_without_key(self, client):
        resp = client.delete("/admin/sources/1", headers=_wrong_key())
        assert resp.status_code == 403


# ── PUT /admin/sources/{id} ───────────────────────────────────────────────────

class TestUpdateSource:
    def test_update_url(self, client):
        resp = client.put(
            "/admin/sources/1",
            json={"url": "https://updated.example.com/rss"},
            headers=_auth(),
        )
        assert resp.status_code == 200
        assert resp.json()["url"] == "https://updated.example.com/rss"

    def test_re_enable_disabled_source(self, client, mem_db):
        mem_db.execute("UPDATE rss_sources SET enabled=0 WHERE id=%s", (1,))
        mem_db.commit()

        resp = client.put("/admin/sources/1", json={"enabled": True}, headers=_auth())
        assert resp.status_code == 200
        assert resp.json()["enabled"] is True

    def test_empty_body_returns_422(self, client):
        resp = client.put("/admin/sources/1", json={}, headers=_auth())
        assert resp.status_code == 422

    def test_nonexistent_returns_404(self, client):
        resp = client.put(
            "/admin/sources/999",
            json={"url": "https://x.example.com/rss"},
            headers=_auth(),
        )
        assert resp.status_code == 404


# ── POST /admin/sources/{id}/reset ───────────────────────────────────────────

class TestResetBackoff:
    def test_reset_clears_backoff(self, client, mem_db):
        mem_db.execute(
            "UPDATE rss_sources SET status='backoff', error_count=5, "
            "next_retry_at='2030-01-01T00:00:00Z' WHERE id=%s",
            (1,),
        )
        mem_db.commit()

        resp = client.post("/admin/sources/1/reset", headers=_auth())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["error_count"] == 0

    def test_reset_clears_dead_status(self, client, mem_db):
        mem_db.execute("UPDATE rss_sources SET status='dead', error_count=10 WHERE id=%s", (1,))
        mem_db.commit()

        resp = client.post("/admin/sources/1/reset", headers=_auth())
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_nonexistent_returns_404(self, client):
        resp = client.post("/admin/sources/999/reset", headers=_auth())
        assert resp.status_code == 404

    def test_forbidden_without_key(self, client):
        resp = client.post("/admin/sources/1/reset", headers=_wrong_key())
        assert resp.status_code == 403


# ── get_recent_clusters ─────────────────────────────────────────────────────────────────────────────

def test_get_recent_clusters_empty(mem_db):
    from app.db.queries import get_recent_clusters
    result = get_recent_clusters(mem_db, limit=10)
    assert result == []


def test_get_recent_clusters_returns_rows(mem_db):
    """Insert a cluster directly and verify it shows up."""
    from app.db.queries import get_recent_clusters

    conn = mem_db._conn
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO event_clusters
                (canonical_title, source_count, status, tickers, first_seen_at,
                 keywords, title_tokens)
            VALUES
                (%s, 1, %s, %s,
                 NOW() AT TIME ZONE 'UTC',
                 %s, %s)
            RETURNING id
            """,
            (
                'Сбербанк объявил дивиденды',
                'new',
                'SBER',
                'сбербанк дивиденд',
                'сбербанк объявил дивиденды',
            ),
        )
        cluster_id = cur.fetchone()[0]
    conn.commit()

    result = get_recent_clusters(mem_db, limit=10)
    assert len(result) == 1
    assert result[0]["id"] == cluster_id
    assert result[0]["canonical_title"] == "Сбербанк объявил дивиденды"
    assert result[0]["tickers"] == "SBER"
    assert result[0]["sent_ok"] is None  # no telegram_sends row yet


# ── test_channel ──────────────────────────────────────────────────────────────

def test_test_channel_sends_message(client, monkeypatch):
    from unittest.mock import AsyncMock, patch
    mock_send = AsyncMock(return_value=12345)
    with patch("app.api.routes.admin.send_text", mock_send):
        resp = client.post(
            "/admin/test_channel",
            headers={"X-Admin-Key": _ADMIN_KEY},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["message_id"] == 12345
    mock_send.assert_called_once()


def test_test_channel_returns_502_on_failure(client):
    from unittest.mock import AsyncMock, patch
    mock_send = AsyncMock(return_value=None)
    with patch("app.api.routes.admin.send_text", mock_send):
        resp = client.post(
            "/admin/test_channel",
            headers={"X-Admin-Key": _ADMIN_KEY},
        )
    assert resp.status_code == 502


def test_test_channel_requires_admin_key(client):
    resp = client.post("/admin/test_channel")
    assert resp.status_code == 422


# ── pipeline/clusters ─────────────────────────────────────────────────────────

def test_pipeline_clusters_empty(client):
    resp = client.get(
        "/admin/pipeline/clusters",
        headers={"X-Admin-Key": _ADMIN_KEY},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["clusters"] == []


def test_pipeline_clusters_returns_data(client, mem_db):
    conn = mem_db._conn
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO event_clusters
                (canonical_title, source_count, status, tickers,
                 first_seen_at, keywords, title_tokens)
            VALUES
                ('Газпром сократил добычу', 2, 'published', 'GAZP',
                 NOW() AT TIME ZONE 'UTC',
                 'газпром добыча', 'газпром сократил добычу')
        """)
    conn.commit()

    resp = client.get(
        "/admin/pipeline/clusters?limit=5",
        headers={"X-Admin-Key": _ADMIN_KEY},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["clusters"][0]["canonical_title"] == "Газпром сократил добычу"
    assert body["clusters"][0]["tickers"] == "GAZP"


def test_pipeline_clusters_requires_admin(client):
    resp = client.get("/admin/pipeline/clusters")
    assert resp.status_code == 422

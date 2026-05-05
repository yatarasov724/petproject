"""
Tests for the Admin API (MVP-7).

Uses FastAPI TestClient with an in-memory SQLite DB injected via dependency override.
No real network calls, no scheduler, no Telegram.
"""

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.api.routes.admin import _get_db

_SCHEMA_PATH = Path(__file__).parent.parent / "app" / "db" / "schema.sql"
_ADMIN_KEY   = "test-secret-key"


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def mem_db() -> sqlite3.Connection:
    """In-memory DB with production schema and one seed source."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_PATH.read_text())
    conn.execute(
        "INSERT INTO rss_sources (id, name, url) VALUES (1, 'TestFeed', 'https://test.local/rss')"
    )
    conn.commit()
    yield conn
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

        row = mem_db.execute("SELECT enabled FROM rss_sources WHERE id=1").fetchone()
        assert row["enabled"] == 0

    def test_row_still_exists_after_disable(self, client, mem_db):
        client.delete("/admin/sources/1", headers=_auth())
        row = mem_db.execute("SELECT id FROM rss_sources WHERE id=1").fetchone()
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
        mem_db.execute("UPDATE rss_sources SET enabled=0 WHERE id=1")
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
            "next_retry_at='2030-01-01T00:00:00Z' WHERE id=1"
        )
        mem_db.commit()

        resp = client.post("/admin/sources/1/reset", headers=_auth())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["error_count"] == 0

    def test_reset_clears_dead_status(self, client, mem_db):
        mem_db.execute("UPDATE rss_sources SET status='dead', error_count=10 WHERE id=1")
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

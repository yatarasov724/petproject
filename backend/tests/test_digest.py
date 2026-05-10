"""
Tests for the daily digest feature.

Covers:
- queries.get_top_sent_clusters(): correct filtering and ordering
- telegram.formatter.format_digest(): MarkdownV2 output with and without AI
- ai.digest._validate(): DigestAnalysis coercion and validation
"""

import sqlite3
from datetime import datetime, timedelta, timezone

from app.db import queries
from app.ai.digest import _validate, DigestAnalysis
from app.telegram.formatter import format_digest


# ── helpers ───────────────────────────────────────────────────────────────────

def _insert_cluster(
    db: sqlite3.Connection,
    *,
    title: str,
    score: int = 50,
    source_count: int = 2,
    status: str = "published",
    sent_minutes_ago: int = 60,
) -> int:
    """Insert a cluster with last_sent_at set to `sent_minutes_ago` minutes ago."""
    sent_at = (datetime.now(timezone.utc) - timedelta(minutes=sent_minutes_ago)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    tokens = title.lower().replace(" ", "_")
    db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             status, last_sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (title, tokens, tokens, score, source_count, status, sent_at),
    )
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


# ── get_top_sent_clusters ─────────────────────────────────────────────────────

class TestGetTopSentClusters:
    def test_returns_published_clusters_within_window(self, db):
        _insert_cluster(db, title="ЦБ сохранил ставку", sent_minutes_ago=30)
        _insert_cluster(db, title="Газпром снизил дивиденды", sent_minutes_ago=120)

        result = queries.get_top_sent_clusters(db, within_hours=3, limit=10)
        titles = [r["canonical_title"] for r in result]
        assert "ЦБ сохранил ставку" in titles
        assert "Газпром снизил дивиденды" in titles

    def test_excludes_clusters_outside_window(self, db):
        _insert_cluster(db, title="Старое событие", sent_minutes_ago=25 * 60)  # 25h ago
        _insert_cluster(db, title="Свежее событие", sent_minutes_ago=30)

        result = queries.get_top_sent_clusters(db, within_hours=12, limit=10)
        titles = [r["canonical_title"] for r in result]
        assert "Свежее событие" in titles
        assert "Старое событие" not in titles

    def test_excludes_unsent_clusters(self, db):
        # Cluster with no last_sent_at (status='new')
        db.execute(
            """
            INSERT INTO event_clusters
                (canonical_title, title_tokens, keywords, best_score, source_count, status)
            VALUES ('Неопубликованное событие', 'tok', 'tok', 60, 3, 'new')
            """
        )
        db.commit()
        _insert_cluster(db, title="Опубликованное событие", sent_minutes_ago=30)

        result = queries.get_top_sent_clusters(db, within_hours=12, limit=10)
        titles = [r["canonical_title"] for r in result]
        assert "Опубликованное событие" in titles
        assert "Неопубликованное событие" not in titles

    def test_ordered_by_score_times_source_count(self, db):
        _insert_cluster(db, title="Среднее событие",    score=30, source_count=2, sent_minutes_ago=30)
        _insert_cluster(db, title="Важное событие",     score=50, source_count=4, sent_minutes_ago=30)
        _insert_cluster(db, title="Менее важное событие", score=50, source_count=1, sent_minutes_ago=30)

        result = queries.get_top_sent_clusters(db, within_hours=12, limit=10)
        titles = [r["canonical_title"] for r in result]
        # score * source_count: 200 > 50 > 60
        assert titles[0] == "Важное событие"

    def test_respects_limit(self, db):
        for i in range(5):
            _insert_cluster(db, title=f"Событие {i}", sent_minutes_ago=30)

        result = queries.get_top_sent_clusters(db, within_hours=12, limit=3)
        assert len(result) == 3

    def test_empty_when_no_sent_clusters(self, db):
        result = queries.get_top_sent_clusters(db, within_hours=12, limit=7)
        assert result == []


# ── _validate (DigestAnalysis) ────────────────────────────────────────────────

class TestDigestValidate:
    def test_valid_response(self):
        data = {
            "items": [
                {"emoji": "🟢", "effect": "поддержка рынка"},
                {"emoji": "🔴", "effect": "давление на акции"},
            ],
            "summary": "Сессия прошла с преобладанием негатива.",
        }
        result = _validate(data, expected_count=2)
        assert result is not None
        assert len(result.items) == 2
        assert result.items[0]["emoji"] == "🟢"
        assert result.summary == "Сессия прошла с преобладанием негатива."

    def test_wrong_item_count_returns_none(self):
        data = {"items": [{"emoji": "🟢", "effect": "x"}], "summary": "y"}
        assert _validate(data, expected_count=2) is None

    def test_invalid_emoji_coerced_to_red(self):
        data = {
            "items": [{"emoji": "❓", "effect": "неизвестно"}],
            "summary": "итог",
        }
        result = _validate(data, expected_count=1)
        assert result is not None
        assert result.items[0]["emoji"] == "🔴"

    def test_missing_summary_returns_empty_string(self):
        data = {"items": [{"emoji": "🟢", "effect": "x"}], "summary": ""}
        result = _validate(data, expected_count=1)
        assert result is not None
        assert result.summary == ""


# ── format_digest ─────────────────────────────────────────────────────────────

def _make_row(title: str, score: int = 50, source_count: int = 2) -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t (id INT, canonical_title TEXT, best_score INT, source_count INT, tickers TEXT, last_sent_at TEXT)"
    )
    conn.execute("INSERT INTO t VALUES (1, ?, ?, ?, NULL, NULL)", (title, score, source_count))
    return conn.execute("SELECT * FROM t").fetchone()


class TestFormatDigest:
    def test_header_contains_label(self):
        row = _make_row("ЦБ сохранил ставку")
        text = format_digest([row], ai_digest=None, label="18:30")
        assert "18:30" in text

    def test_header_is_bold(self):
        row = _make_row("ЦБ сохранил ставку")
        text = format_digest([row], ai_digest=None, label="18:30")
        assert text.startswith("📋 *ДАЙДЖЕСТ")

    def test_fallback_bullet_without_ai(self):
        row = _make_row("Газпром снизил дивиденды")
        text = format_digest([row], ai_digest=None, label="22:00")
        assert "•" in text
        assert "Газпром" in text

    def test_with_ai_shows_emoji_and_effect(self):
        row = _make_row("ЦБ повысил ставку")
        ai  = DigestAnalysis(
            items=[{"emoji": "🔴", "effect": "давление на ОФЗ"}],
            summary="Сессия закрылась снижением.",
        )
        text = format_digest([row], ai_digest=ai, label="18:30")
        assert "🔴" in text
        assert "давление на ОФЗ" in text

    def test_with_ai_shows_summary(self):
        row = _make_row("ЦБ повысил ставку")
        ai  = DigestAnalysis(
            items=[{"emoji": "🔴", "effect": "давление на ОФЗ"}],
            summary="Итоги: негативная сессия.",
        )
        text = format_digest([row], ai_digest=ai, label="18:30")
        assert "Итоги" in text

    def test_no_summary_when_ai_summary_empty(self):
        row = _make_row("ЦБ повысил ставку")
        ai  = DigestAnalysis(
            items=[{"emoji": "🟢", "effect": "поддержка"}],
            summary="",
        )
        text = format_digest([row], ai_digest=ai, label="18:30")
        # No trailing empty summary line
        assert text.count("\n\n") <= 1

    def test_multiple_clusters(self):
        rows = [_make_row(f"Событие {i}") for i in range(3)]
        text = format_digest(rows, ai_digest=None, label="22:00")
        assert text.count("•") == 3

"""
Tests for queries.get_escaped_duplicates_24h().

An "escaped duplicate" = two distinct published clusters, sent within 2 hours
of each other, with Jaccard title similarity >= JACCARD_THRESHOLD (0.35).
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.db import queries
from app.pipeline.dedup import JACCARD_THRESHOLD
from tests.conftest import db  # noqa: F401


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_sent_cluster(db, *, title: str, title_tokens: str, hours_ago: float = 0.0) -> int:
    sent_at = _iso(_now() - timedelta(hours=hours_ago))
    row = db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             status, last_sent_at, first_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (title, title_tokens, title_tokens, 65, 1, "published", sent_at, sent_at),
    ).fetchone()
    cluster_id = row["id"]
    db.execute(
        """
        INSERT INTO telegram_sends
            (cluster_id, decision, score, source_count, headline, ok, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (cluster_id, "NEW_EVENT", 65, 1, title, 1, sent_at),
    )
    db.commit()
    return cluster_id


class TestEscapedDuplicates:
    def test_no_sends_returns_zero(self, db):
        assert queries.get_escaped_duplicates_24h(db) == 0

    def test_single_send_returns_zero(self, db):
        _insert_sent_cluster(
            db,
            title="Сбербанк объявил дивиденды за 2025 год",
            title_tokens="сбербанк объявить дивиденды 2025 год",
        )
        assert queries.get_escaped_duplicates_24h(db) == 0

    def test_similar_titles_within_2h_counts_as_escaped(self, db):
        # Two nearly-identical headlines, sent 30 minutes apart → escaped dup
        _insert_sent_cluster(
            db,
            title="Сбербанк рекомендовал дивиденды за 2025 год",
            title_tokens="сбербанк рекомендовать дивиденды 2025 год",
            hours_ago=0.5,
        )
        _insert_sent_cluster(
            db,
            title="Сбербанк объявил дивиденды за 2025 год",
            title_tokens="сбербанк объявить дивиденды 2025 год",
            hours_ago=0.0,
        )
        assert queries.get_escaped_duplicates_24h(db) == 1

    def test_similar_titles_beyond_2h_not_counted(self, db):
        # Same similarity but sent 3 hours apart → different stories, not a dup
        _insert_sent_cluster(
            db,
            title="Сбербанк рекомендовал дивиденды за 2025 год",
            title_tokens="сбербанк рекомендовать дивиденды 2025 год",
            hours_ago=3.0,
        )
        _insert_sent_cluster(
            db,
            title="Сбербанк объявил дивиденды за 2025 год",
            title_tokens="сбербанк объявить дивиденды 2025 год",
            hours_ago=0.0,
        )
        assert queries.get_escaped_duplicates_24h(db) == 0

    def test_unrelated_titles_within_2h_not_counted(self, db):
        # Two unrelated stories sent minutes apart → not duplicates
        _insert_sent_cluster(
            db,
            title="Газпром снизил дивиденды",
            title_tokens="газпром снизить дивиденды",
            hours_ago=0.1,
        )
        _insert_sent_cluster(
            db,
            title="Сбербанк нарастил прибыль",
            title_tokens="сбербанк нарастить прибыль",
            hours_ago=0.0,
        )
        assert queries.get_escaped_duplicates_24h(db) == 0

    def test_send_older_than_24h_excluded(self, db):
        # One send is 25h ago — outside the window, shouldn't pair with the recent one
        _insert_sent_cluster(
            db,
            title="Сбербанк рекомендовал дивиденды за 2025 год",
            title_tokens="сбербанк рекомендовать дивиденды 2025 год",
            hours_ago=25.0,
        )
        _insert_sent_cluster(
            db,
            title="Сбербанк объявил дивиденды за 2025 год",
            title_tokens="сбербанк объявить дивиденды 2025 год",
            hours_ago=0.0,
        )
        assert queries.get_escaped_duplicates_24h(db) == 0

    def test_failed_send_not_counted(self, db):
        # ok=0 sends are Telegram failures, not actual publications
        sent_at = _iso(_now())
        row = db.execute(
            """
            INSERT INTO event_clusters
                (canonical_title, title_tokens, keywords, best_score, source_count,
                 status, last_sent_at, first_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            ("Сбербанк рекомендовал дивиденды", "сбербанк рекомендовать дивиденды",
             "сбербанк", 65, 1, "published", sent_at, sent_at),
        ).fetchone()
        db.execute(
            "INSERT INTO telegram_sends (cluster_id, decision, score, source_count, headline, ok, sent_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (row["id"], "NEW_EVENT", 65, 1, "Сбербанк рекомендовал дивиденды", 0, sent_at),
        )
        db.commit()
        _insert_sent_cluster(
            db,
            title="Сбербанк объявил дивиденды за 2025 год",
            title_tokens="сбербанк объявить дивиденды 2025 год",
        )
        assert queries.get_escaped_duplicates_24h(db) == 0

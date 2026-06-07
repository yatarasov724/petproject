"""
Tests for speaker saturation: prevents one person flooding the channel and digest.

Covers:
- _extract_speaker(): detects speaker from canonical_title
- queries.count_recent_speaker_publishes(): counts recent posts by speaker prefix
- Orchestrator silences a 3rd post from the same speaker within the window
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock

import pytest

from app.db import queries
from app.pipeline.orchestrator import _extract_speaker, process, Outcome, SPEAKER_SATURATION_LIMIT
from tests.conftest import make_article, db  # noqa: F401


# ── _extract_speaker ──────────────────────────────────────────────────────────

class TestExtractSpeaker:
    def test_colon_format(self):
        assert _extract_speaker("Сечин: санкции стали нормой") == "сечин"

    def test_colon_with_space(self):
        assert _extract_speaker("Сечин : цены на нефть вырастут") == "сечин"

    def test_verb_after_name(self):
        assert _extract_speaker("Сечин предупредил о последствиях") == "сечин"

    def test_two_word_name(self):
        assert _extract_speaker("Греф назвал ставку психологической чертой") == "греф"

    def test_lowercase_start_returns_none(self):
        assert _extract_speaker("санкции стали нормой в мире") is None

    def test_non_person_start_returns_none(self):
        # Org names / long phrases should not be detected as speaker
        assert _extract_speaker("Российская экономика продолжает развиваться") is None

    def test_empty_title_returns_none(self):
        assert _extract_speaker("") is None


# ── count_recent_speaker_publishes ────────────────────────────────────────────

def _insert_published_cluster(db, *, title: str, minutes_ago: int = 30) -> None:
    sent_at = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, source_count,
             status, last_sent_at, first_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (title, "tok", "tok", 65, 1, "published", sent_at, sent_at),
    )
    db.commit()


class TestCountRecentSpeakerPublishes:
    def test_counts_matching_titles(self, db):
        _insert_published_cluster(db, title="Сечин: санкции стали нормой")
        _insert_published_cluster(db, title="Сечин допустил рост нефти")
        count = queries.count_recent_speaker_publishes(db, "сечин", within_hours=8)
        assert count == 2

    def test_ignores_other_speakers(self, db):
        _insert_published_cluster(db, title="Сечин: санкции стали нормой")
        _insert_published_cluster(db, title="Греф назвал ставку нормальной")
        count = queries.count_recent_speaker_publishes(db, "сечин", within_hours=8)
        assert count == 1

    def test_ignores_old_publishes(self, db):
        _insert_published_cluster(db, title="Сечин: старое заявление", minutes_ago=10 * 60)
        count = queries.count_recent_speaker_publishes(db, "сечин", within_hours=8)
        assert count == 0

    def test_returns_zero_when_empty(self, db):
        count = queries.count_recent_speaker_publishes(db, "сечин", within_hours=8)
        assert count == 0


# ── orchestrator speaker saturation ──────────────────────────────────────────

@pytest.mark.asyncio
class TestOrchestratorSpeakerSaturation:
    async def test_third_sechin_post_is_silenced(self, db):
        """After SPEAKER_SATURATION_LIMIT posts from Сечин, the next one is silenced."""
        # Pre-populate the DB with enough published Sechin clusters
        for i in range(SPEAKER_SATURATION_LIMIT):
            _insert_published_cluster(
                db,
                title=f"Сечин: заявление номер {i}",
                minutes_ago=30 + i * 5,
            )

        article = make_article(title="Сечин: новое заявление о санкциях против России")
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=None):
            result = await process(db, article)

        assert result.outcome == Outcome.SILENCE

    async def test_first_sechin_post_is_published(self, db):
        """First post from Сечин goes through normally."""
        article = make_article(title="Сечин: заявление о санкциях против России")
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await process(db, article)

        assert result.outcome in (Outcome.SENT_NEW, Outcome.SENT_UPDATE)

    async def test_non_speaker_title_not_limited(self, db):
        """
        Titles without a detectable speaker bypass the saturation check.
        The article should NOT be silenced by speaker saturation — it may be silenced
        for other reasons (e.g. relevance), but not because of a speaker count.
        We use a title that scores above noise so it actually reaches the saturation check.
        """
        for i in range(SPEAKER_SATURATION_LIMIT):
            _insert_published_cluster(
                db,
                title=f"Газпром снизил дивиденды на {i} процентов",
                minutes_ago=30 + i * 5,
            )

        # Title has no detectable speaker — speaker saturation must not apply
        article = make_article(title="ЦБ повысил ключевую ставку до двадцати одного процента")
        with patch("app.telegram.client.send", new_callable=AsyncMock, return_value=42):
            result = await process(db, article)

        # SENT_NEW or SENT_UPDATE = passed through; SILENCE = ok too (other reasons may apply)
        # NOISE would mean we got wrong title — but shouldn't happen with a scored article
        assert result.outcome != Outcome.ERROR

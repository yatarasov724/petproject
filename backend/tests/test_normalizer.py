"""
Tests for app.pipeline.normalizer.

Covers:
- tokenize(): stop word removal, punctuation stripping, deduplication, sorting
- normalize(): title too short, missing date, HTML stripping, hash determinism
- _make_hash(): same tokens + same hour → same hash; different hour → different hash
"""

from datetime import datetime, timezone

import pytest

from app.pipeline.normalizer import normalize, tokenize, RawArticle


# ── tokenize ─────────────────────────────────────────────────────────────────

class TestTokenize:
    def test_basic_split_and_lower(self):
        tokens = tokenize("Газпром снизил дивиденды")
        assert "газпром" in tokens
        assert "снизил" in tokens
        assert "дивиденды" in tokens

    def test_stop_words_removed(self):
        # "в", "и", "на" are in stop list
        tokens = tokenize("в России и на бирже")
        assert "в" not in tokens
        assert "и" not in tokens
        assert "на" not in tokens

    def test_speech_verb_removed(self):
        tokens = tokenize("Газпром сообщил о снижении прибыли")
        assert "сообщил" not in tokens

    def test_punctuation_stripped(self):
        tokens = tokenize("ЦБ: повысил ставку!")
        # colons and exclamation marks should not appear in tokens
        assert all(":" not in t and "!" not in t for t in tokens)

    def test_output_is_sorted(self):
        tokens = tokenize("Роснефть купила актив Лукойл")
        assert tokens == sorted(tokens)

    def test_deduplication(self):
        tokens = tokenize("санкции санкции санкции США")
        assert tokens.count("санкции") == 1

    def test_short_tokens_removed(self):
        # Single-char tokens (not stop words) should be removed by _MIN_TOKEN_LEN=2
        tokens = tokenize("a b c abc")
        assert "a" not in tokens
        assert "b" not in tokens
        assert "c" not in tokens
        assert "abc" in tokens

    def test_empty_title(self):
        assert tokenize("") == []

    def test_all_stop_words(self):
        assert tokenize("в и на из за от") == []

    def test_html_not_stripped_by_tokenize(self):
        # tokenize doesn't strip HTML — that's normalize()'s job
        tokens = tokenize("<b>Газпром</b>")
        # angle brackets removed by punctuation regex, tags become empty strings
        assert "газпром" in tokens


# ── normalize ─────────────────────────────────────────────────────────────────

def _entry(title="Роснефть объявила о слиянии с Лукойл", date="Mon, 15 Jan 2024 10:00:00 +0000"):
    return {"title": title, "link": "http://example.com/1", "published": date}


class TestNormalize:
    def test_returns_raw_article(self):
        result = normalize(_entry(), source_id=1, source_name="RBC")
        assert isinstance(result, RawArticle)

    def test_title_too_short_returns_none(self):
        result = normalize(_entry(title="Кратко"), source_id=1, source_name="RBC")
        assert result is None

    def test_missing_date_returns_none(self):
        entry = {"title": "Роснефть купила актив за миллиард рублей", "link": "http://x.com"}
        result = normalize(entry, source_id=1, source_name="RBC")
        assert result is None

    def test_html_stripped_from_title(self):
        result = normalize(
            _entry(title="<b>Газпром</b> снизил <i>дивиденды</i> на 50 процентов"),
            source_id=1, source_name="RBC",
        )
        assert result is not None
        assert "<b>" not in result.title
        assert "Газпром" in result.title

    def test_published_at_utc_aware(self):
        result = normalize(_entry(), source_id=1, source_name="RBC")
        assert result is not None
        assert result.published_at.tzinfo is not None

    def test_raw_hash_is_md5_hex(self):
        result = normalize(_entry(), source_id=1, source_name="RBC")
        assert result is not None
        assert len(result.raw_hash) == 32
        assert all(c in "0123456789abcdef" for c in result.raw_hash)

    def test_same_title_same_hour_same_hash(self):
        # Two slightly different titles that normalize to identical tokens should share a hash
        title_a = "Роснефть объявила о слиянии с Лукойл"
        title_b = "О слиянии Роснефть с Лукойл объявила"
        date = "Mon, 15 Jan 2024 10:00:00 +0000"
        a = normalize(_entry(title_a, date), source_id=1, source_name="RBC")
        b = normalize(_entry(title_b, date), source_id=1, source_name="RBC")
        assert a is not None and b is not None
        # Both produce same sorted tokens → same hash
        assert a.raw_hash == b.raw_hash

    def test_different_hour_different_hash(self):
        title = "Роснефть объявила о слиянии с Лукойл"
        a = normalize(_entry(title, "Mon, 15 Jan 2024 10:00:00 +0000"), source_id=1, source_name="RBC")
        b = normalize(_entry(title, "Mon, 15 Jan 2024 11:00:00 +0000"), source_id=1, source_name="RBC")
        assert a is not None and b is not None
        assert a.raw_hash != b.raw_hash

    def test_title_tokens_are_sorted(self):
        result = normalize(_entry(), source_id=1, source_name="RBC")
        assert result is not None
        tokens = result.title_tokens.split()
        assert tokens == sorted(tokens)

    def test_updated_field_used_as_fallback_date(self):
        entry = {
            "title": "Роснефть объявила о крупной сделке по слиянию",
            "link": "http://x.com",
            "updated": "Mon, 15 Jan 2024 10:00:00 +0000",
        }
        result = normalize(entry, source_id=1, source_name="RBC")
        assert result is not None

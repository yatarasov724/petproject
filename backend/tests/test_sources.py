"""
Tests for RSS source configuration and feed parsing (MVP-6).

Covers:
- _RSS_SEEDS integrity: unique names, valid HTTPS URLs, no duplicates
- normalize() handles each new source's feed format correctly
  (parameterised over fixture XML files in tests/fixtures/rss/)
"""

from pathlib import Path

import feedparser
import pytest

from app.db.queries import _RSS_SEEDS
from app.pipeline.normalizer import normalize

_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "rss"

# Map fixture filename stem → source name as it appears in _RSS_SEEDS
_FIXTURE_SOURCES = {
    "bfm":      "BFM",
    "ria":      "RIA",
    "izvestia": "Izvestia",
    "gazeta":   "Gazeta",
    "lenta":    "Lenta",
    "rg":       "RG",
    "investing": "Investing",
    "moex":     "MOEX",
}


# ── seed integrity ─────────────────────────────────────────────────────────────

class TestSeedIntegrity:
    def test_all_names_are_unique(self):
        names = [name for name, _ in _RSS_SEEDS]
        assert len(names) == len(set(names)), f"Duplicate names in _RSS_SEEDS: {names}"

    def test_all_urls_are_unique(self):
        urls = [url for _, url in _RSS_SEEDS]
        assert len(urls) == len(set(urls)), f"Duplicate URLs in _RSS_SEEDS: {urls}"

    def test_all_urls_use_https(self):
        bad = [(name, url) for name, url in _RSS_SEEDS if not url.startswith("https://")]
        assert not bad, f"Non-HTTPS URLs in seeds: {bad}"

    def test_all_names_non_empty(self):
        bad = [name for name, _ in _RSS_SEEDS if not name.strip()]
        assert not bad, "Empty name in _RSS_SEEDS"

    def test_seed_count_at_least_13(self):
        """Regression guard: ensure we have at least 13 sources (5 original + 8 new)."""
        assert len(_RSS_SEEDS) >= 13, (
            f"Expected ≥13 seeds, got {len(_RSS_SEEDS)}"
        )

    def test_original_sources_present(self):
        """The 5 original sources must still be in the seeds."""
        names = {name for name, _ in _RSS_SEEDS}
        for expected in ("TASS", "Interfax", "Prime", "Vedomosti", "Kommersant"):
            assert expected in names, f"Original source '{expected}' missing from seeds"

    def test_new_sources_present(self):
        """New sources added in MVP-6 must be present."""
        names = {name for name, _ in _RSS_SEEDS}
        for expected in ("BFM", "RIA", "Izvestia", "Gazeta", "Lenta", "RG", "Investing", "MOEX"):
            assert expected in names, f"MVP-6 source '{expected}' missing from seeds"


# ── fixture parsing ────────────────────────────────────────────────────────────

def _load_fixture(name: str) -> list[dict]:
    """Parse a fixture RSS file and return feedparser entries."""
    path = _FIXTURES_DIR / f"{name}.xml"
    feed = feedparser.parse(path.read_text(encoding="utf-8"))
    assert feed.entries, f"Fixture {name}.xml has no entries"
    return feed.entries


@pytest.mark.parametrize("fixture_name,source_name", list(_FIXTURE_SOURCES.items()))
def test_normalize_returns_article_for_fixture(fixture_name, source_name):
    """
    normalize() must return a RawArticle (not None) for every entry
    in each new source's fixture file.
    """
    entries = _load_fixture(fixture_name)
    results = [normalize(entry, source_id=99, source_name=source_name) for entry in entries]

    nones = [i for i, r in enumerate(results) if r is None]
    assert not nones, (
        f"{source_name}: normalize() returned None for entries {nones} "
        f"in {fixture_name}.xml"
    )


@pytest.mark.parametrize("fixture_name,source_name", list(_FIXTURE_SOURCES.items()))
def test_normalize_produces_valid_hash_and_tokens(fixture_name, source_name):
    """raw_hash must be a 32-char hex string; title_tokens must be non-empty."""
    entries = _load_fixture(fixture_name)
    for entry in entries:
        article = normalize(entry, source_id=99, source_name=source_name)
        if article is None:
            continue  # already caught by the previous test
        assert len(article.raw_hash) == 32 and all(
            c in "0123456789abcdef" for c in article.raw_hash
        ), f"Invalid raw_hash: {article.raw_hash!r}"
        assert article.title_tokens.strip(), f"Empty title_tokens for: {article.title!r}"


@pytest.mark.parametrize("fixture_name,source_name", list(_FIXTURE_SOURCES.items()))
def test_normalize_preserves_source_metadata(fixture_name, source_name):
    """source_id and source_name must be passed through unchanged."""
    entries = _load_fixture(fixture_name)
    article = normalize(entries[0], source_id=42, source_name=source_name)
    assert article is not None
    assert article.source_id == 42
    assert article.source_name == source_name

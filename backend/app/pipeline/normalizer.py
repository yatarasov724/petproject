"""
Text normalization for Russian financial news headlines.

Pipeline for a single title:
  raw text
    → strip HTML
    → collapse whitespace
    → lowercase
    → remove punctuation (keep Cyrillic, Latin, digits)
    → split into tokens
    → remove stop words and short tokens
    → sort tokens (deterministic order for hashing and Jaccard)

Two outputs are produced per article:
  raw_hash    — MD5(sorted_tokens + date_hour)
                Exact-dedup key: same story, same calendar hour,
                regardless of minor wording differences between sources.
  title_tokens — space-joined sorted tokens
                Used for Jaccard near-dedup and stored in seen_articles.
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ── stop words ────────────────────────────────────────────────────────────────

# Russian grammatical particles, prepositions, conjunctions, common verbs.
# Kept intentionally narrow: we only remove words that carry zero
# discriminative value for news dedup.
_RU_STOP: frozenset[str] = frozenset({
    # prepositions
    "в", "на", "по", "из", "за", "от", "до", "при", "под", "над",
    "для", "без", "про", "вне", "меж", "ради",
    # conjunctions / particles
    "и", "а", "но", "или", "что", "как", "так", "же", "бы", "не",
    "ни", "то", "ли", "да", "нет", "уж", "ну",
    # demonstratives / pronouns
    "это", "этот", "эта", "эти", "тот", "та", "те", "все", "всё",
    "он", "она", "они", "его", "её", "их", "им", "мы", "вы",
    # common auxiliaries
    "был", "была", "было", "были", "есть", "быть", "будет", "будут",
    "стал", "стала", "стали",
    # filler adverbs frequent in news
    "уже", "еще", "ещё", "также", "только", "более", "менее",
    "около", "почти", "снова", "опять", "вновь", "через",
    "между", "вместе", "именно", "просто", "даже",
    # speech verbs (источник сообщил / заявил / …)
    "сообщил", "сообщила", "сообщили", "сообщает",
    "заявил", "заявила", "заявили", "заявляет",
    "рассказал", "рассказала", "рассказали",
    "отметил", "отметила", "отметили",
    "подчеркнул", "подчеркнула", "подчеркнули",
    "добавил", "добавила", "добавили",
    "уточнил", "уточнила", "уточнили",
    "напомнил", "напомнила", "напомнили",
    "пояснил", "пояснила", "пояснили",
    "объяснил", "объяснила", "объяснили",
    "предупредил", "предупредила", "предупредили",
    "признал", "признала", "признали",
    "считает", "считают", "полагает", "полагают",
})

# English stop words that appear in Russian news headlines
_EN_STOP: frozenset[str] = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "of", "for", "with", "by", "from", "is", "are", "was", "were",
    "says", "said", "new", "according", "reports",
})

_STOP_WORDS: frozenset[str] = _RU_STOP | _EN_STOP

# Minimum token length to keep (filters "рф" → keeps, "в" → filtered by stop list)
_MIN_TOKEN_LEN = 2

# Replace anything that is not Cyrillic, Latin, digit, or hyphen with space
_PUNCT = re.compile(r"[^\u0400-\u04FFa-zA-Z0-9\-]+")

# Collapse multiple spaces
_SPACES = re.compile(r"\s+")

# Strip HTML tags
_HTML_TAG = re.compile(r"<[^>]+>")


# ── data model ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RawArticle:
    source_id:    int
    source_name:  str
    title:        str           # human-readable, original casing
    url:          str
    published_at: datetime      # UTC-aware
    raw_hash:     str           # MD5(sorted_tokens + date_hour) — exact dedup key
    title_tokens: str           # space-joined sorted lowercase tokens for Jaccard


# ── public API ────────────────────────────────────────────────────────────────

def normalize(
    entry: dict,
    source_id: int,
    source_name: str,
) -> Optional[RawArticle]:
    """
    Convert a feedparser entry dict into a RawArticle.
    Returns None for unusable entries. Never raises.
    """
    raw_title = entry.get("title", "")
    display_title = _strip_html(raw_title).strip()
    if len(display_title) < 10:
        return None

    url = entry.get("link", "").strip()

    published_at = _parse_date(entry)
    if published_at is None:
        return None

    tokens = tokenize(display_title)
    if not tokens:
        return None

    title_tokens = " ".join(tokens)
    raw_hash = _make_hash(tokens, published_at)

    return RawArticle(
        source_id=source_id,
        source_name=source_name,
        title=display_title,
        url=url,
        published_at=published_at,
        raw_hash=raw_hash,
        title_tokens=title_tokens,
    )


def tokenize(title: str) -> list[str]:
    """
    Public function — used by both normalize() and dedup comparisons.

    Steps:
      1. Lowercase
      2. Remove punctuation (keep Cyrillic, Latin, digits, hyphens)
      3. Split on whitespace
      4. Drop stop words and short tokens
      5. Sort (deterministic order)
    """
    text = title.lower()
    text = _PUNCT.sub(" ", text)
    text = _SPACES.sub(" ", text).strip()

    tokens = [
        t for t in text.split()
        if len(t) >= _MIN_TOKEN_LEN and t not in _STOP_WORDS
    ]

    return sorted(set(tokens))


# ── internals ─────────────────────────────────────────────────────────────────

def _strip_html(raw: str) -> str:
    text = _HTML_TAG.sub("", raw)
    return _SPACES.sub(" ", text)


def _make_hash(sorted_tokens: list[str], published_at: datetime) -> str:
    """
    Exact-dedup hash.

    Key = sorted_tokens joined + calendar hour (YYYYMMDDHH).
    Two articles with the same core words published in the same hour
    (even from different sources) will produce the same hash.
    """
    date_hour = published_at.strftime("%Y%m%d%H")
    fingerprint = " ".join(sorted_tokens) + date_hour
    return hashlib.md5(fingerprint.encode()).hexdigest()


def _parse_date(entry: dict) -> Optional[datetime]:
    for field in ("published", "updated"):
        raw = entry.get(field, "")
        if not raw:
            continue
        try:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        except Exception:
            continue

    for field in ("published_parsed", "updated_parsed"):
        parsed = entry.get(field)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except Exception:
                continue

    logger.debug("No parseable date for: %.60s", entry.get("title", ""))
    return None

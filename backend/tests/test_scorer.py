"""
Tests for app.pipeline.scorer.

Covers:
- Noise fast-path: noise patterns short-circuit to score=0
- Tier classification: T1/T2/T3 base scores
- Event type classification: SANCTIONS, RATE_DECISION, DIVIDENDS, etc.
- Multi-source bonus: +10 for 2 sources, +20 for 3+
- Keyword bonus: capped at +15
- is_publishable(): threshold check
- ARTICLE_MIN_SCORE: pre-filter boundary
"""

import pytest

from app.pipeline.scorer import (
    compute_score,
    classify_event_type,
    is_publishable,
    EventType,
    PUBLISH_THRESHOLD,
    ARTICLE_MIN_SCORE,
    _TIER1_BASE,
    _TIER2_BASE,
    _TIER3_BASE,
)


# ── noise fast-path ───────────────────────────────────────────────────────────

class TestNoise:
    def test_noise_pattern_returns_zero(self):
        result = compute_score("Международный кинофестиваль открылся в Москве")
        assert result.score == 0
        assert result.tier == "noise"
        assert result.event_type == EventType.NOISE

    def test_sports_news_is_noise(self):
        result = compute_score("Россия выиграла чемпионат мира по хоккею")
        assert result.score == 0

    def test_unknown_topic_no_keywords(self):
        result = compute_score("Новый ресторан открылся в центре города")
        assert result.score == 0
        assert result.event_type == EventType.UNKNOWN


# ── tier 1 ───────────────────────────────────────────────────────────────────

class TestTier1:
    def test_sanctions_hits_tier1(self):
        result = compute_score("США ввели новые санкции против российских компаний")
        assert result.base_score == _TIER1_BASE
        assert result.tier == "tier1"

    def test_default_hits_tier1(self):
        result = compute_score("Россия объявила технический дефолт по внешнему долгу")
        assert result.base_score == _TIER1_BASE
        assert result.tier == "tier1"

    def test_delisting_hits_tier1(self):
        result = compute_score("MOEX объявила о принудительном делистинге акций Газпрома")
        assert result.base_score == _TIER1_BASE

    def test_devolution_hits_tier1(self):
        # keyword is "девальвация" (nominative) — title must use that exact form
        result = compute_score("Девальвация рубля объявлена официально")
        assert result.base_score == _TIER1_BASE


# ── tier 2 ───────────────────────────────────────────────────────────────────

class TestTier2:
    def test_key_rate_decision_hits_tier2(self):
        result = compute_score("ЦБ повысил ключевую ставку до 21 процента")
        assert result.base_score == _TIER2_BASE

    def test_dividends_hits_tier2(self):
        result = compute_score("Газпром объявил о дивидендах за 2023 год")
        assert result.base_score == _TIER2_BASE

    def test_net_profit_hits_tier2(self):
        result = compute_score("Сбербанк отчитался о рекордной чистой прибыли за квартал")
        assert result.base_score == _TIER2_BASE

    def test_moex_company_name_hits_tier2(self):
        result = compute_score("Лукойл готовится к крупной сделке поглощения")
        assert result.base_score == _TIER2_BASE


# ── tier 3 ───────────────────────────────────────────────────────────────────

class TestTier3:
    def test_inflation_data_hits_tier3(self):
        result = compute_score("Росстат опубликовал данные по инфляции за март")
        assert result.base_score == _TIER3_BASE
        assert result.tier == "tier3"

    def test_budget_deficit_hits_tier3(self):
        result = compute_score("Минфин сообщил о дефиците бюджета в первом квартале")
        assert result.base_score == _TIER3_BASE


# ── event type ────────────────────────────────────────────────────────────────

class TestEventType:
    def test_sanctions_classified(self):
        et = classify_event_type("США ввели новые санкции против Газпрома")
        assert et == EventType.SANCTIONS

    def test_rate_decision_classified(self):
        et = classify_event_type("цб принял решение по ставке на заседании")
        assert et == EventType.RATE_DECISION

    def test_dividends_classified(self):
        et = classify_event_type("газпром объявил дивиденды")
        assert et == EventType.DIVIDENDS

    def test_default_classified(self):
        et = classify_event_type("россия объявила дефолт")
        assert et == EventType.DEFAULT

    def test_ipo_classified(self):
        et = classify_event_type("компания объявила об ipo на московской бирже")
        assert et == EventType.IPO

    def test_opec_classified(self):
        et = classify_event_type("опек+ сократил добычу нефти на 1 млн баррелей")
        assert et == EventType.OPEC

    def test_spo_buyback_classified(self):
        et = classify_event_type("сбербанк объявил обратный выкуп акций на 50 млрд рублей")
        assert et == EventType.SPO_BUYBACK

    def test_regulation_classified(self):
        et = classify_event_type("фас выдала предписание яндексу по делу о монополии")
        assert et == EventType.REGULATION

    def test_unknown_fallback(self):
        et = classify_event_type("совершенно нейтральный текст без ключевых слов")
        assert et == EventType.UNKNOWN


# ── source bonus ──────────────────────────────────────────────────────────────

class TestSourceBonus:
    def test_single_source_no_bonus(self):
        result = compute_score("ЦБ повысил ключевую ставку до 21 процента", source_count=1)
        assert result.source_bonus == 0

    def test_two_sources_ten_bonus(self):
        result = compute_score("ЦБ повысил ключевую ставку до 21 процента", source_count=2)
        assert result.source_bonus == 10

    def test_three_sources_twenty_bonus(self):
        result = compute_score("ЦБ повысил ключевую ставку до 21 процента", source_count=3)
        assert result.source_bonus == 20

    def test_four_sources_same_as_three(self):
        result = compute_score("ЦБ повысил ключевую ставку до 21 процента", source_count=4)
        assert result.source_bonus == 20


# ── keyword bonus ─────────────────────────────────────────────────────────────

class TestKeywordBonus:
    def test_no_bonus_for_single_keyword(self):
        # "санкции" hits once → bonus = 0
        result = compute_score("введены санкции")
        assert result.keyword_bonus == 0

    def test_bonus_for_multiple_keywords(self):
        # "санкции" + "эмбарго" → 2 hits → +5
        result = compute_score("США ввели новые санкции и нефтяное эмбарго")
        assert result.keyword_bonus >= 5

    def test_keyword_bonus_capped_at_15(self):
        # Many matches in one title — bonus must not exceed 15
        result = compute_score(
            "санкции эмбарго дефолт делистинг девальвация мобилизация национализация"
        )
        assert result.keyword_bonus == 15


# ── publishability ────────────────────────────────────────────────────────────

class TestPublishability:
    def test_tier1_alone_is_publishable(self):
        # T1=50 ≥ threshold=30
        result = compute_score("введены санкции против России")
        assert is_publishable(result.score)

    def test_tier3_alone_not_publishable(self):
        # T3=10 < threshold=30
        result = compute_score("Росстат опубликовал данные по инфляции за март")
        # source_count=1, no type bonus for MACRO_DATA
        assert result.score < PUBLISH_THRESHOLD

    def test_tier3_three_sources_is_publishable(self):
        # T3=10 + source_bonus=20 = 30 — exactly at threshold
        result = compute_score("Росстат опубликовал данные по инфляции", source_count=3)
        assert result.score >= PUBLISH_THRESHOLD

    def test_article_min_score_boundary(self):
        # A T3 article alone scores 10 = ARTICLE_MIN_SCORE exactly → passes pre-filter
        result = compute_score("Росстат опубликовал данные по инфляции за март")
        assert result.score >= ARTICLE_MIN_SCORE

    def test_noise_below_min_score(self):
        result = compute_score("Кинофестиваль открылся в Москве")
        assert result.score < ARTICLE_MIN_SCORE

    def test_score_clamped_at_100(self):
        # Pile on as many hits as possible
        title = (
            "санкции эмбарго дефолт делистинг девальвация мобилизация "
            "национализация заморозка активов экстренное заседание цб"
        )
        result = compute_score(title, source_count=5)
        assert result.score <= 100

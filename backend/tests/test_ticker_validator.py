# tests/test_ticker_validator.py
"""
Tests for ticker_validator.validate_tickers().

The validator strips tickers whose company keywords are absent from
the cluster canonical_title. Commodity tickers are always kept.
"""
import pytest
from app.pipeline.ticker_validator import validate_tickers
from dataclasses import replace
from app.ai.analyzer import AIAnalysis


def test_valid_ticker_kept():
    result = validate_tickers("SBER", "Сбербанк объявил дивиденды")
    assert result == "SBER"


def test_mismatch_ticker_stripped():
    """MTSS не упомянут в новости о МГКЛ — должен быть убран."""
    result = validate_tickers("MTSS", "СД МГКЛ рекомендовал дивиденды за 2025 год")
    assert result == ""


def test_mgkl_ticker_kept():
    result = validate_tickers("MGKL", "СД МГКЛ рекомендовал дивиденды за 2025 год")
    assert result == "MGKL"


def test_commodity_tickers_always_kept():
    """LKOH, ROSN валидны даже без упоминания компании — commodity rule."""
    result = validate_tickers("LKOH,ROSN", "Цена нефти Brent выросла до $100")
    assert result == "LKOH,ROSN"


def test_mixed_valid_and_invalid():
    """Из двух тикеров убирается только несоответствующий."""
    result = validate_tickers("MGKL,MTSS", "СД МГКЛ рекомендовал дивиденды")
    assert result == "MGKL"


def test_empty_tickers_returns_empty():
    assert validate_tickers("", "любой заголовок") == ""


def test_none_tickers_returns_empty():
    assert validate_tickers(None, "любой заголовок") == ""


def test_all_commodity_tickers_kept():
    """GAZP, NVTK — тоже commodity, проходят без проверки."""
    result = validate_tickers("GAZP,NVTK", "Мировые цены на газ выросли на 15%")
    assert result == "GAZP,NVTK"


def test_plzl_kept_for_gold_news():
    result = validate_tickers("PLZL", "Золото выросло до $2500 за унцию")
    assert result == "PLZL"


def test_mtsb_kept_when_mts_bank_in_title():
    result = validate_tickers("MTSB", "СД МТС-Банка рекомендовал дивиденды")
    assert result == "MTSB"


# ── AI ticker cross-validation (regression for ai_tickers bypassing validator) ──

def test_ai_ticker_valid_when_company_in_title():
    """AI assigns SBER, company keyword present — keep it."""
    result = validate_tickers("SBER", "Сбербанк повысил ставку по депозитам")
    assert result == "SBER"


def test_ai_ticker_stripped_when_company_absent():
    """AI assigns SBER, but headline is about ЦБ — not about Sberbank itself."""
    result = validate_tickers("SBER", "ЦБ повысил ключевую ставку до 22%")
    assert result == ""


def test_ai_commodity_ticker_always_kept():
    """AI assigns LKOH — commodity rule exempts it from keyword check."""
    result = validate_tickers("LKOH", "ЦБ повысил ключевую ставку до 22%")
    assert result == "LKOH"


def test_ai_mixed_valid_and_invalid():
    """AI assigns GAZP (commodity, keep) + VTBR (no keyword, strip)."""
    result = validate_tickers("GAZP,VTBR", "Цена природного газа выросла на 10%")
    assert result == "GAZP"



def _apply_ai_ticker_validation(ai_analysis: AIAnalysis, canonical_title: str) -> AIAnalysis:
    """Mirrors the logic added to orchestrator._ai_enrich()."""
    if not ai_analysis.tickers:
        return ai_analysis
    validated_str = validate_tickers(",".join(ai_analysis.tickers), canonical_title)
    validated_list = [t for t in validated_str.split(",") if t] if validated_str else []
    if validated_list == ai_analysis.tickers:
        return ai_analysis
    return replace(ai_analysis, tickers=validated_list)


def test_ai_ticker_bypass_fix_strips_wrong_ticker():
    """After the fix, AI ticker SBER is stripped when headline has no Сбербанк."""
    ai = AIAnalysis(summary="ЦБ поднял ставку", what_behind="...", watch_for="...", tickers=["SBER"])
    cleaned = _apply_ai_ticker_validation(ai, "ЦБ повысил ключевую ставку до 22%")
    assert cleaned.tickers == []


def test_ai_ticker_bypass_fix_keeps_correct_ticker():
    """After the fix, AI ticker SBER is kept when Сбербанк is in headline."""
    ai = AIAnalysis(summary="Сбер объявил", what_behind="...", watch_for="...", tickers=["SBER"])
    cleaned = _apply_ai_ticker_validation(ai, "Сбербанк объявил о дивидендах")
    assert cleaned.tickers == ["SBER"]


def test_ai_ticker_bypass_fix_preserves_commodity():
    """Commodity tickers survive validation even without company keyword."""
    ai = AIAnalysis(summary="Нефть растёт", what_behind="...", watch_for="...", tickers=["LKOH", "ROSN"])
    cleaned = _apply_ai_ticker_validation(ai, "Цена нефти Brent выросла до $100")
    assert cleaned.tickers == ["LKOH", "ROSN"]

# tests/test_ticker_validator.py
"""
Tests for ticker_validator.validate_tickers().

The validator strips tickers whose company keywords are absent from
the cluster canonical_title. Commodity tickers are always kept.
"""
import pytest
from app.pipeline.ticker_validator import validate_tickers


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

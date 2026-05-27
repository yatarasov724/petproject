from app.ai.filter import extract_tickers


def test_no_tickers():
    assert extract_tickers("ЦБ повысил ключевую ставку") == []


def test_single_ticker_full_name():
    assert extract_tickers("Газпром снизил дивиденды за 2025 год") == ["GAZP"]


def test_single_ticker_short_name():
    assert extract_tickers("Сбер объявил о новых условиях ипотеки") == ["SBER"]


def test_person_name_as_keyword():
    assert extract_tickers("Греф рассказал о планах по ИИ") == ["SBER"]


def test_multiple_tickers():
    result = extract_tickers("Сбербанк и Газпром объявили о совместной сделке")
    assert "GAZP" in result
    assert "SBER" in result


def test_result_is_sorted():
    result = extract_tickers("Сбербанк и Газпром объявили о сделке")
    assert result == sorted(result)


def test_case_insensitive():
    assert extract_tickers("ГАЗПРОМ снизил дивиденды") == ["GAZP"]


def test_no_false_positive_on_market_news():
    assert extract_tickers("ЦБ поднял ставку на 100 базисных пунктов") == []


def test_lkoh():
    assert extract_tickers("Лукойл отчитался за третий квартал") == ["LKOH"]


def test_oil_price_news_maps_to_lkoh_rosn():
    result = extract_tickers("Нефть продолжает дорожать на мировых рынках")
    assert "LKOH" in result
    assert "ROSN" in result


def test_opec_news_maps_to_lkoh_rosn():
    result = extract_tickers("Генсек ОПЕК обсудил квоты на добычу")
    assert "LKOH" in result
    assert "ROSN" in result


def test_gold_price_news_maps_to_plzl():
    result = extract_tickers("Золото выросло до рекордного уровня")
    assert "PLZL" in result


def test_company_plus_oil_includes_commodity_tickers():
    result = extract_tickers("Лукойл нарастил добычу нефти в 2025 году")
    assert "LKOH" in result
    assert "ROSN" in result  # commodity rule adds ROSN even if not explicitly named


def test_multiple_articles_accumulate(db):
    from app.pipeline import clusterer
    from tests.conftest import make_article

    a1 = make_article(title="Газпром снизил дивиденды за 2025 год")
    a2 = make_article(
        title="Сбербанк одобрил сделку с Газпромом",
        url="http://test.local/article/2",
    )

    r1 = clusterer.find_or_create(db, a1, market_score=40)
    r2 = clusterer.find_or_create(db, a2, market_score=40)

    from app.db import queries
    c1 = queries.get_cluster(db, r1.cluster_id)
    c2 = queries.get_cluster(db, r2.cluster_id)

    assert c1 is not None
    assert c1["tickers"] == "GAZP"
    assert c2 is not None
    assert c2["tickers"] is not None
    assert "GAZP" in c2["tickers"]


# ── Регрессионные тесты (баги 2026-05-26) ───────────────────────────────────

def test_mgkl_returns_mgkl():
    """МГКЛ должен давать MGKL, а не MTSS/FEES/AFLT."""
    assert extract_tickers("СД МГКЛ рекомендовал дивиденды за 2025 год") == ["MGKL"]


def test_mts_bank_returns_mtsb_not_mtss():
    """МТС-Банк — это банк, не оператор. Тикер MTSB, не MTSS."""
    assert extract_tickers("СД МТС-Банк рекомендовал дивиденды") == ["MTSB"]
    assert extract_tickers("Совет директоров МТС банка объявил дивиденды") == ["MTSB"]


def test_mts_operator_still_works():
    """МТС (оператор связи) продолжает давать MTSS."""
    result = extract_tickers("МТС запустил новый тарифный план")
    assert result == ["MTSS"]


def test_ozon_pharma_returns_ozph_not_ozon():
    """Озон Фармацевтика — не Ozon-маркетплейс."""
    assert extract_tickers("Озон Фармацевтика выплатит дивиденды за 2025 год") == ["OZPH"]


def test_autozone_returns_empty():
    """AutoZone (американская компания) — не OZON."""
    assert extract_tickers("Evercore ISI подтверждает рейтинг акций AutoZone") == []


def test_rosseti_mr_returns_msrs_not_fees():
    """Россети Московский регион — дочка, не головная компания."""
    assert extract_tickers("Россети Московский регион рекомендовал дивиденды") == ["MSRS"]


def test_russneft_returns_rnft_not_oil():
    """Русснефть имеет собственный тикер, не LKOH/ROSN."""
    assert extract_tickers("СД Русснефти не выплатит дивиденды по обыкновенным акциям") == ["RNFT"]


def test_nmtp_returns_nmtp():
    assert extract_tickers("СД НМТП рекомендовал дивиденды за 2025 год") == ["NMTP"]


def test_ozon_cyrillic_still_works():
    """Озон (маркетплейс) без слова фармацевтика — OZON."""
    result = extract_tickers("Озон нарастил выручку в 2 раза")
    assert result == ["OZON"]


def test_oil_price_gives_commodity_tickers():
    result = extract_tickers("Цена нефти Brent выросла до 100 долларов")
    assert "LKOH" in result
    assert "ROSN" in result


def test_conflict_resolution_mtsb_wins():
    """Если в тексте мтс банк, MTSB должен вытеснить MTSS."""
    result = extract_tickers("Акции МТС банка выросли на 3%")
    assert "MTSB" in result
    assert "MTSS" not in result

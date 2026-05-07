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

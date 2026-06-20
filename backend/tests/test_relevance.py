"""
Tests for app.pipeline.relevance.is_russia_relevant.

Covers:
  - Clusters with known tickers always pass
  - Clusters matching Russia/MOEX tokens pass
  - Clusters with no Russian signal are rejected
  - IPO of unknown Russian company (no ticker yet) is correctly passed
"""

from app.pipeline.relevance import is_russia_relevant


def _cluster(tickers=None, keywords="", title_tokens=""):
    return {"tickers": tickers, "keywords": keywords, "title_tokens": title_tokens}


class TestKnownTickers:
    def test_cluster_with_tickers_always_relevant(self):
        cluster = _cluster(tickers="SBER GAZP", keywords="", title_tokens="")
        assert is_russia_relevant(cluster) is True

    def test_empty_tickers_string_not_treated_as_tickers(self):
        cluster = _cluster(tickers="", keywords="мечел дивиденды", title_tokens="мечел дивиденды")
        assert is_russia_relevant(cluster) is True


class TestRussiaTokens:
    def test_cluster_mentioning_sberbank_is_relevant(self):
        cluster = _cluster(keywords="сбербанк акции", title_tokens="сбербанк акции")
        assert is_russia_relevant(cluster) is True

    def test_cluster_mentioning_cb_rate_is_relevant(self):
        cluster = _cluster(keywords="цб ставка решение", title_tokens="цб ставка решение")
        assert is_russia_relevant(cluster) is True

    def test_cluster_mentioning_putin_is_relevant(self):
        cluster = _cluster(keywords="путин рубль", title_tokens="путин рубль")
        assert is_russia_relevant(cluster) is True


class TestIrrelevant:
    def test_foreign_company_no_russia_token_is_not_relevant(self):
        cluster = _cluster(keywords="apple revenue quarterly results", title_tokens="apple revenue quarterly results")
        assert is_russia_relevant(cluster) is False

    def test_empty_tokens_no_tickers_is_not_relevant(self):
        cluster = _cluster(keywords="", title_tokens="")
        assert is_russia_relevant(cluster) is False


class TestIPORelevance:
    def test_ipo_of_unknown_russian_company_is_relevant(self):
        # Инкаб — российская компания, тикера нет в системе, но IPO значимо для рынка.
        # Воспроизводит баг: cluster #7776 силенсился relevance gate несмотря на score=35.
        cluster = _cluster(
            tickers=None,
            keywords="ipo близкий граница допустить закрытие заявка инкаб книга нижний",
            title_tokens="ipo близкий граница допустить закрытие заявка инкаб книга нижний",
        )
        assert is_russia_relevant(cluster) is True

    def test_ipo_token_in_keywords_only_is_relevant(self):
        cluster = _cluster(keywords="ipo размещение акций", title_tokens="")
        assert is_russia_relevant(cluster) is True

    def test_ипо_cyrillic_token_is_relevant(self):
        cluster = _cluster(keywords="ипо первичное размещение", title_tokens="")
        assert is_russia_relevant(cluster) is True

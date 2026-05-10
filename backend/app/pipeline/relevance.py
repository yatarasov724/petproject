"""
Russia/MOEX relevance gate.

Applied in the main pipeline (orchestrator) before Telegram send,
and in digest_job to select digest candidates.

Checks lemmatized token sets stored in event_clusters.keywords and
event_clusters.title_tokens — so short tokens like "рф" are never
missed even when longer tokens fill the top-12 keywords slot.
"""

_RUSSIA_MARKET_TOKENS: frozenset[str] = frozenset({
    # Country / government
    "рф", "россия", "российский",
    "правительство", "минфин", "цб",
    # Key officials
    "путин", "мишустин", "набиуллина", "новак", "силуанов",
    "греф", "миллер", "сечин", "белоусов",
    # Top MOEX companies
    "газпром", "сбербанк", "лукойл", "роснефть", "новатэк",
    "норникель", "яндекс", "татнефть", "транснефть", "алроса",
    "северсталь", "нлмк", "ммк", "сургутнефтегаз", "аэрофлот",
    "магнит", "мтс", "ростелеком", "фосагро", "озон",
    "тинькофф", "мосбиржа", "втб", "полюс", "мечел", "селигдар",
    # Russian financial instruments / macro
    "рубль", "офз",
    # Energy — critical for Russian budget and MOEX heavyweights
    "нефть", "опек",
})


def is_russia_relevant(cluster) -> bool:
    """
    Return True if the cluster is relevant to the Russian market / MOEX.

    cluster — any object supporting ['tickers'], ['keywords'], ['title_tokens']
              (sqlite3.Row from event_clusters works directly).
    """
    if cluster["tickers"]:
        return True
    tokens = (
        set((cluster["keywords"]      or "").split())
        | set((cluster["title_tokens"] or "").split())
    )
    return bool(tokens & _RUSSIA_MARKET_TOKENS)

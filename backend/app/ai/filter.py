# Фильтр релевантности ДО отправки в AI (экономим rate limit Groq)

TICKER_KEYWORDS: dict[str, list[str]] = {
    "SBER": ["сбербанк", "сбер", "греф"],
    "GAZP": ["газпром", "миллер"],
    "LKOH": ["лукойл", "алекперов"],
    "YNDX": ["яндекс", "yandex"],
    "ROSN": ["роснефть", "сечин"],
    "GMKN": ["норникель", "потанин"],
    "NVTK": ["новатэк", "михельсон"],
    "TATN": ["татнефть"],
    "MTSS": ["мтс банк", "мтс-банк"],
    "VTBR": ["банк втб", " втб "],
    "AFLT": ["аэрофлот"],
    "MGNT": ["магнит"],
    "PHOR": ["фосагро"],
    "ALRS": ["алроса"],
    "PLZL": ["полюс золото", "полюс золот"],
    "OZON": ["озон", "ozon"],
    "PIKK": ["пик групп", "группа пик"],
    "CHMF": ["северсталь"],
    "NLMK": ["нлмк"],
    "MAGN": ["ммк", "магнитогорский металлургический"],
    "SNGS": ["сургутнефтегаз"],
    "FEES": ["фск еэс", "россети"],
    "RTKM": ["ростелеком"],
    "MTLR": ["мечел"],
}

# Whitelist допустимых тикеров для валидации ответа AI
VALID_TICKERS = set(TICKER_KEYWORDS.keys())

MARKET_KEYWORDS = [
    "акции", "фондовый рынок", "биржа", "moex", "мосбиржа",
    "дивиденды", "чистая прибыль", "выручка", "санкции",
    "ключевая ставка", "цб рф", "центробанк",
    "национализация", "приватизация", "пошлина на экспорт",
    "листинг", "ipo", "buyback", "обратный выкуп",
]


def is_relevant(title: str, content: str) -> bool:
    text = (title + " " + content).lower()
    for keywords in TICKER_KEYWORDS.values():
        if any(kw in text for kw in keywords):
            return True
    return any(kw in text for kw in MARKET_KEYWORDS)


def get_priority(source: str) -> int:
    if source.startswith("TG:"):
        return 1  # Telegram — высший приоритет
    if source in ("TASS", "Interfax", "RBC"):
        return 2
    return 3

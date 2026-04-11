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
    "MTSS": ["мтс"],
    "VTBR": ["втб"],
    "AFLT": ["аэрофлот"],
    "MGNT": ["магнит"],
    "PHOR": ["фосагро"],
    "ALRS": ["алроса"],
    "PLZL": ["полюс", "золото"],
}

MARKET_KEYWORDS = [
    "акции", "рынок", "биржа", "moex", "мосбиржа",
    "дивиденды", "прибыль", "выручка", "санкции",
    "ключевая ставка", "цб рф", "центробанк", "нефть",
    "рубль", "экспорт", "импорт", "бюджет", "ввп",
    "национализация", "приватизация", "налог", "пошлина",
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

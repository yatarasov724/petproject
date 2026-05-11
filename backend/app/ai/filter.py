import re

TICKER_KEYWORDS: dict[str, list[str]] = {
    # ── Банки / финансы ──────────────────────────────────────────────────────────
    "SBER": ["сбербанк", "сбер", "греф"],
    "VTBR": ["банк втб", "втб"],
    "TCSG": ["тинькофф", "т-банк", "тбанк"],
    "CBOM": ["мкб", "московский кредитный банк"],
    "BSPB": ["банк санкт-петербург"],
    "AFKS": ["афк система"],
    # ── Нефть / газ ──────────────────────────────────────────────────────────────
    "GAZP": ["газпром", "миллер"],
    "LKOH": ["лукойл", "алекперов"],
    "ROSN": ["роснефть", "сечин"],
    "NVTK": ["новатэк", "михельсон"],
    "TATN": ["татнефть"],
    "SNGS": ["сургутнефтегаз"],
    "ENPG": ["эн+", "en+"],
    # ── Металлы / горнодобыча ─────────────────────────────────────────────────────
    "GMKN": ["норникель", "потанин"],
    "CHMF": ["северсталь", "мордашов"],
    "NLMK": ["нлмк"],
    "MAGN": ["ммк", "магнитогорский металлургический"],
    "PLZL": ["полюс"],
    "ALRS": ["алроса"],
    "POLY": ["полиметалл", "polymetal"],
    "MTLR": ["мечел"],
    "SELG": ["селигдар"],
    # ── Телеком / IT ─────────────────────────────────────────────────────────────
    "YNDX": ["яндекс", "yandex"],
    "MTSS": ["мтс"],
    "RTKM": ["ростелеком"],
    "VKCO": ["вконтакте", "vk компани"],
    "POSI": ["позитив текнолоджис", "positive technologies"],
    "HHRU": ["хедхантер", "хэдхантер", "headhunter"],
    "OZON": ["озон", "ozon"],
    # ── Потребительский / ритейл ──────────────────────────────────────────────────
    "MGNT": ["магнит"],
    "FIVE": ["x5", "х5", "пятёрочка", "пятерочка", "перекрёсток", "чижик"],
    "FIXP": ["fix price", "фикс прайс"],
    "SMLT": ["самолёт", "самолет"],
    "PIKK": ["пик групп", "группа пик"],
    "LSRG": ["лср"],
    # ── Прочие ───────────────────────────────────────────────────────────────────
    "AFLT": ["аэрофлот"],
    "PHOR": ["фосагро"],
    "FEES": ["фск еэс", "россети"],
    "SGZH": ["сегежа"],
    "AGRO": ["русагро"],
    "MOEX": ["мосбиржа", "московская биржа"],
}

# Whitelist допустимых тикеров для валидации ответа AI
VALID_TICKERS = set(TICKER_KEYWORDS.keys())

# Прямой матч тикера в заголовке: "акции SBER упали", "котировки GAZP"
_TICKER_DIRECT_RE = re.compile(
    r'\b(' + '|'.join(sorted(TICKER_KEYWORDS, key=len, reverse=True)) + r')\b'
)

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


def extract_tickers(title: str) -> list[str]:
    """Return sorted list of MOEX tickers mentioned in the article title."""
    found: set[str] = set(_TICKER_DIRECT_RE.findall(title))
    text = title.lower()
    for ticker, keywords in TICKER_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            found.add(ticker)
    return sorted(found)


def get_priority(source: str) -> int:
    if source.startswith("TG:"):
        return 1  # Telegram — высший приоритет
    if source in ("TASS", "Interfax", "RBC"):
        return 2
    return 3

import re

TICKER_KEYWORDS: dict[str, list[str]] = {
    # ── Банки / финансы ──────────────────────────────────────────────────────────
    "SBER": ["сбербанк", "сбер", "греф"],
    "VTBR": ["банк втб", "втб"],
    "TCSG": ["тинькофф", "т-банк", "тбанк"],
    "CBOM": ["мкб", "московский кредитный банк"],
    "BSPB": ["банк санкт-петербург"],
    "AFKS": ["афк система"],
    "MTSB": ["мтс-банк", "мтс банк"],   # МТС-Банк — до MTSS, чтобы конфликт-резолюция сработала
    "SVCB": ["совкомбанк"],
    "RENI": ["ренессанс страхование"],
    "SPBE": ["спб биржа", "спб-биржа"],
    # ── Нефть / газ ──────────────────────────────────────────────────────────────
    "GAZP": ["газпром", "миллер"],
    "LKOH": ["лукойл", "алекперов"],
    "ROSN": ["роснефть", "сечин"],
    "NVTK": ["новатэк", "михельсон"],
    "TATN": ["татнефть"],
    "SNGS": ["сургутнефтегаз"],
    "ENPG": ["эн+", "en+"],
    "TRNFP": ["транснефть"],
    "BANEP": ["башнефть"],
    "RNFT": ["русснефт"],          # стем: охватывает русснефть/русснефти/русснефтью
    "NMTP": ["нмтп", "новороссийский морской торговый порт"],
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
    "RUAL": ["русал"],
    "RASP": ["распадская"],
    # ── Электроэнергетика ─────────────────────────────────────────────────────────
    "IRAO": ["интер рао"],
    "HYDR": ["русгидро"],
    # Россети-дочки — до FEES, чтобы конфликт-резолюция убрала общий тикер
    "MSRS": ["россети московский регион", "мрск москвы", "мосэнерго"],
    "MRKV": ["россети волга", "мрск волги"],
    "MRKU": ["россети урал", "мрск урала"],
    "MRKP": ["россети центр и приволжье", "мрск центра и приволжья"],
    "MRKC": ["россети центр", "мрск центра"],
    "FEES": ["фск еэс", "россети федеральная", "фск-еэс", "россети"],  # головная компания
    # ── Телеком / IT ─────────────────────────────────────────────────────────────
    "YNDX": ["яндекс", "yandex"],
    "MTSS": ["мтс"],          # МТС-оператор — ПОСЛЕ MTSB, конфликт-резолюция уберёт при нужде
    "RTKM": ["ростелеком"],
    "VKCO": ["вконтакте", "vk компани"],
    "POSI": ["позитив текнолоджис", "positive technologies"],
    "HHRU": ["хедхантер", "хэдхантер", "headhunter"],
    "OZON": ["озон", "ozon"],   # OZPH переопределит для «Озон Фармацевтика»
    "DIAS": ["диасофт"],
    # ── Потребительский / ритейл ──────────────────────────────────────────────────
    "MGNT": ["магнит"],
    "FIVE": ["x5", "х5", "пятёрочка", "пятерочка", "перекрёсток", "чижик"],
    "FIXP": ["fix price", "фикс прайс"],
    "SMLT": ["самолёт", "самолет"],
    "PIKK": ["пик групп", "группа пик"],
    "LSRG": ["лср"],
    # ── Прочие ───────────────────────────────────────────────────────────────────
    "AFLT": ["аэрофлот"],
    "FLOT": ["совкомфлот"],
    "PHOR": ["фосагро"],
    "SGZH": ["сегежа"],
    "AGRO": ["русагро"],
    "MOEX": ["мосбиржа", "московская биржа"],
    # Мосгорломбард — критично (именно из-за него был баг МГКЛ→MTSS)
    "MGKL": ["мгкл", "мосгорломбард"],
    # Озон Фармацевтика — отдельно от OZON-маркетплейс
    "OZPH": ["озон фармацевтика", "озон фарм"],
}

# Whitelist допустимых тикеров для валидации ответа AI
VALID_TICKERS = set(TICKER_KEYWORDS.keys())

# Прямой матч тикера в заголовке: "акции SBER упали", "котировки GAZP"
_TICKER_DIRECT_RE = re.compile(
    r'\b(' + '|'.join(sorted(TICKER_KEYWORDS, key=len, reverse=True)) + r')\b'
)

# Разрешение конфликтов: специфичный тикер вытесняет общий.
# Формат: {специфичный_тикер: [тикеры_которые_надо_убрать]}
_TICKER_OVERRIDES: dict[str, list[str]] = {
    "MTSB": ["MTSS"],              # МТС-Банк → убрать МТС
    "OZPH": ["OZON"],              # Озон Фармацевтика → убрать Ozon-маркетплейс
    "RNFT": ["LKOH", "ROSN"],     # Русснефть → убрать generic нефтяные тикеры
    "MSRS": ["FEES"],              # Россети МР → убрать общие Россети
    "MRKV": ["FEES"],              # Россети Волга → убрать общие Россети
    "MRKU": ["FEES"],              # Россети Урал → убрать общие Россети
    "MRKP": ["FEES"],              # Россети Центр и Приволжье → убрать общие Россети
    "MRKC": ["FEES"],              # Россети Центр → убрать общие Россети
}

# Commodity-level keywords: when oil/OPEC/gold is mentioned without a specific company.
# Mirrors the rule in the AI prompt: "нефтяные котировки / ОПЕК → LKOH, ROSN"
_COMMODITY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("нефт",  ["LKOH", "ROSN"]),  # нефть, нефти, нефтяной, нефтедобыча…
    ("опек",  ["LKOH", "ROSN"]),  # ОПЕК, ОПЕК+
    ("opec",  ["LKOH", "ROSN"]),
    ("золот", ["PLZL"]),           # золото, золотой, золотых
]

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
    """Return sorted list of MOEX tickers mentioned in the article title.

    Matching strategy:
    - Requires left word boundary (no letter/digit before keyword).
    - Allows Cyrillic case endings on the right (е, и, у, ю, я, а, ь, ой, ем…).
    - Blocks Latin letters and hyphens on the right (prevents substring matches
      like 'ozon' inside 'autozone', or 'мтс' inside 'мтс-банк').
    - Conflict resolution removes a generic ticker when a more specific one is found.
    """
    found: set[str] = set(_TICKER_DIRECT_RE.findall(title))
    padded = " " + title.lower() + " "
    for ticker, keywords in TICKER_KEYWORDS.items():
        for kw in keywords:
            if _word_match(kw, padded):
                found.add(ticker)
                break

    for stem, tickers in _COMMODITY_KEYWORDS:
        if _word_match(stem, padded):
            found.update(tickers)

    # Конфликт-резолюция: если найден специфичный тикер, убираем общий
    for specific, generics in _TICKER_OVERRIDES.items():
        if specific in found:
            for generic in generics:
                found.discard(generic)

    return sorted(found)


def _word_match(keyword: str, padded_text: str) -> bool:
    """Match `keyword` in lowercased, space-padded text with word-boundary semantics.

    Left side: no alpha/digit/hyphen may precede the keyword.
    Right side: Cyrillic letters (case endings) are ALLOWED; Latin letters,
                digits, and hyphens are BLOCKED (they'd make keyword a prefix of
                a longer word, e.g. 'ozon' inside 'autozone').
    Exception: adjective-forming suffix '-ов-' (Cyrillic 'ов' + alpha) is
               BLOCKED to prevent 'озон' matching 'озонового слоя'.
    """
    idx = padded_text.find(keyword)
    while idx != -1:
        before = padded_text[idx - 1] if idx > 0 else " "
        after_pos = idx + len(keyword)
        after = padded_text[after_pos] if after_pos < len(padded_text) else " "

        left_ok  = not (before.isalpha() or before.isdigit() or before == "-")
        # Right: block Latin letters, digits, hyphens; allow space, punct, Cyrillic
        right_ok = not (after.isdigit() or after == "-" or
                        (after.isalpha() and _is_latin(after)))
        # Block adjective suffix 'ов+alpha' (e.g. 'озонового', 'яндексовый')
        if right_ok and after.lower() == "о":
            next_ch   = padded_text[after_pos + 1] if after_pos + 1 < len(padded_text) else " "
            after_two = padded_text[after_pos + 2] if after_pos + 2 < len(padded_text) else " "
            if next_ch.lower() == "в" and after_two.isalpha():
                right_ok = False
        if left_ok and right_ok:
            return True
        idx = padded_text.find(keyword, idx + 1)
    return False


def word_match(keyword: str, title: str) -> bool:
    """Public wrapper: check if keyword matches title with word-boundary semantics."""
    return _word_match(keyword, " " + title.lower() + " ")


def _is_latin(ch: str) -> bool:
    return "a" <= ch <= "z" or "A" <= ch <= "Z"


def get_priority(source: str) -> int:
    if source.startswith("TG:"):
        return 1  # Telegram — высший приоритет
    if source in ("TASS", "Interfax", "RBC"):
        return 2
    return 3

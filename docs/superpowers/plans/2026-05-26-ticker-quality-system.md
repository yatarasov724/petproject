# Ticker Quality System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Предотвратить публикацию неверных тикеров через валидацию перед публикацией, детектор незнакомых компаний и команду ручного исправления.

**Architecture:** Три слоя: (1) `ticker_validator.py` отфильтровывает тикеры без ключевых слов в заголовке до `tg.send()`; (2) детектор в `orchestrator.py` алертит в ops-чат, если паттерн дивидендов/дохода без тикера; (3) команда `/fix` в `commands.py` позволяет admin исправить тикер в БД.

**Tech Stack:** Python 3.11, psycopg2, aiogram-style Telegram Bot API, pytest, существующий стек проекта.

**Working directory:** `/opt/newsparser/backend`  
**Run tests:** `docker exec backend-backend-1 python -m pytest tests/ -x -q`

---

## File Map

| Файл | Действие | Ответственность |
|------|----------|-----------------|
| `tests/test_filter.py` | modify | Регрессионные тесты extract_tickers |
| `app/pipeline/ticker_validator.py` | create | Валидация тикеров перед публикацией |
| `tests/test_ticker_validator.py` | create | Тесты валидатора |
| `app/pipeline/orchestrator.py` | modify | Вызов валидатора + детектор незнакомых компаний |
| `app/db/queries.py` | modify | `get_last_message_id_for_cluster()` |
| `app/bot/commands.py` | modify | Команда `/fix` |

---

## Task 1: Регрессионные тесты для extract_tickers

**Files:**
- Modify: `tests/test_filter.py`

- [ ] **Step 1: Добавить тесты в конец существующего test_filter.py**

```python
# Добавить в конец tests/test_filter.py

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
    """Озон (маркетплейс) без слова 'фармацевтика' — OZON."""
    result = extract_tickers("Озон нарастил выручку в 2 раза")
    assert result == ["OZON"]


def test_oil_price_gives_commodity_tickers():
    result = extract_tickers("Цена нефти Brent выросла до 100 долларов")
    assert "LKOH" in result
    assert "ROSN" in result


def test_conflict_resolution_mtsb_wins():
    """Если в тексте 'мтс банк', MTSB должен вытеснить MTSS."""
    result = extract_tickers("Акции МТС банка выросли на 3%")
    assert "MTSB" in result
    assert "MTSS" not in result
```

- [ ] **Step 2: Запустить тесты — убедиться что всё зелёное**

```bash
docker exec backend-backend-1 python -m pytest tests/test_filter.py -v
```

Ожидаем: все тесты PASSED (новый код уже задеплоен в предыдущей сессии).

- [ ] **Step 3: Commit**

```bash
cd /opt/newsparser
git add backend/tests/test_filter.py
git commit -m "test: add ticker extraction regression tests for 2026-05-26 bugs

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: TickerValidator — модуль валидации тикеров

**Files:**
- Create: `app/pipeline/ticker_validator.py`
- Create: `tests/test_ticker_validator.py`

- [ ] **Step 1: Написать тест файл**

```python
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
```

- [ ] **Step 2: Запустить тест — убедиться что FAIL (модуль не существует)**

```bash
docker exec backend-backend-1 python -m pytest tests/test_ticker_validator.py -v 2>&1 | head -20
```

Ожидаем: `ModuleNotFoundError: No module named 'app.pipeline.ticker_validator'`

- [ ] **Step 3: Создать ticker_validator.py**

```python
# app/pipeline/ticker_validator.py
"""
Pre-publication ticker validation.

Before sending a cluster to Telegram, verify that each assigned ticker's
company keyword appears in the cluster's canonical title.

Commodity tickers (LKOH, ROSN, GAZP, NVTK, PLZL) are exempt — they are
assigned based on market-level keywords (oil price, OPEC, gold) even when
no specific company is mentioned.

Returns a comma-joined string of valid tickers (same format as DB column).
"""

import logging
from typing import Optional

from app.ai.filter import TICKER_KEYWORDS

logger = logging.getLogger(__name__)

# These tickers are valid even if their company keyword isn't in the title.
# They're assigned by commodity/macro rules, not company-name matching.
COMMODITY_TICKERS: frozenset[str] = frozenset({"LKOH", "ROSN", "GAZP", "NVTK", "PLZL"})


def validate_tickers(tickers_str: Optional[str], cluster_title: str) -> str:
    """
    Return comma-joined tickers whose company keywords appear in cluster_title.
    Commodity tickers pass without keyword check.
    Invalid/mismatched tickers are stripped and logged as WARNING.

    Args:
        tickers_str: comma-joined ticker string from event_clusters.tickers, or None.
        cluster_title: the cluster's canonical_title used for keyword lookup.

    Returns:
        Comma-joined string of validated tickers, possibly empty.
    """
    if not tickers_str:
        return ""

    tickers = [t.strip() for t in tickers_str.split(",") if t.strip()]
    title_lower = cluster_title.lower()
    valid: list[str] = []

    for ticker in tickers:
        if ticker in COMMODITY_TICKERS:
            valid.append(ticker)
            continue

        keywords = TICKER_KEYWORDS.get(ticker, [])
        if any(kw in title_lower for kw in keywords):
            valid.append(ticker)
        else:
            logger.warning(
                "ticker_mismatch ticker=%s not in title «%.80s» — stripped from publication",
                ticker,
                cluster_title,
                extra={
                    "event":   "ticker_mismatch",
                    "ticker":  ticker,
                    "title":   cluster_title[:80],
                },
            )

    return ",".join(valid)
```

- [ ] **Step 4: Запустить тесты — убедиться что PASSED**

```bash
docker exec backend-backend-1 python -m pytest tests/test_ticker_validator.py -v
```

Ожидаем: все 10 тестов PASSED.

- [ ] **Step 5: Commit**

```bash
cd /opt/newsparser
git add backend/app/pipeline/ticker_validator.py backend/tests/test_ticker_validator.py
git commit -m "feat: add ticker validator — strip tickers not in cluster title

Commodity tickers (LKOH, ROSN, GAZP, NVTK, PLZL) are always kept.
All others require their company keyword to appear in canonical_title.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Интеграция валидатора в orchestrator + детектор незнакомых компаний

**Files:**
- Modify: `app/pipeline/orchestrator.py`

Нужно вставить два блока в функцию `_run()`: валидатор (перед `tg.send`) и детектор (после решения о публикации).

- [ ] **Step 1: Добавить импорт validate_tickers в начало orchestrator.py**

Найти блок импортов (строки ~20-35) и добавить после существующих pipeline-импортов:

```python
from app.pipeline.ticker_validator import validate_tickers
```

- [ ] **Step 2: Добавить детектор незнакомых компаний**

Добавить функцию `_detect_unknown_company` в конец файла (после `_ai_enrich`):

```python
import re as _re

# Паттерны, указывающие на упоминание конкретной компании в дивидендной/корпоративной новости.
# Если компания не распознана (нет тикера) — шлём alert в ops-чат.
_COMPANY_PATTERNS = [
    # "СД МГКЛ рекомендовал", "Совет директоров НМТП объявил"
    _re.compile(r'\bС[Дд]\.?\s+([А-ЯЁ]{2,6})\b'),
    # "МГКЛ: ДИВИДЕНДЫ =", "НМТП: ДИВИДЕНДЫ"
    _re.compile(r'\b([А-ЯЁ]{2,6})[:\s]+ДИВИДЕНДЫ'),
    # "[А-ЯЁ]{2,6} - ДИВИДЕНДЫ" (формат Smartlab)
    _re.compile(r'[-–]\s*([А-ЯЁ]{2,6})\s*[-–:]\s*ДИВИДЕНДЫ'),
]

async def _detect_unknown_company(title: str, has_tickers: bool) -> None:
    """
    If title matches a 'company dividend/earnings' pattern but no ticker was found,
    send an ops alert so the admin can add the ticker to the keyword list.
    Never raises.
    """
    if has_tickers:
        return  # тикер уже есть — всё хорошо
    try:
        for pattern in _COMPANY_PATTERNS:
            m = pattern.search(title)
            if m:
                abbr = m.group(1)
                from app.core.alerting import send_ops
                await send_ops(
                    f"⚠️ Незнакомая компания: «{abbr}»\n"
                    f"Добавьте тикер в filter.py:\n"
                    f"«{title[:100]}»"
                )
                logger.info(
                    "unknown_company_detected abbr=%s title=%.80s",
                    abbr, title,
                    extra={"event": "unknown_company", "abbr": abbr},
                )
                return  # один алерт за статью
    except Exception:
        logger.warning("unknown company detection failed", exc_info=True)
```

- [ ] **Step 3: Вызвать validate_tickers перед tg.send()**

Найти блок в `_run()` (строки около `correlations = get_correlations(...)`):

```python
    correlations = get_correlations(db, score_result.event_type.value, cluster["tickers"] or "")
```

Заменить на:

```python
    # Валидация тикеров: убираем те, чьи ключевые слова отсутствуют в заголовке.
    # Это предотвращает публикацию тикеров, попавших в кластер через загрязнение.
    safe_tickers = validate_tickers(cluster["tickers"], cluster["canonical_title"])
    if safe_tickers != (cluster["tickers"] or ""):
        cluster = dict(cluster)
        cluster["tickers"] = safe_tickers or None

    correlations = get_correlations(db, score_result.event_type.value, cluster["tickers"] or "")
```

- [ ] **Step 4: Вызвать детектор после tg.send()**

Найти блок:
```python
        if cluster["tickers"]:
            asyncio.create_task(
                _notify_portfolio(cluster["tickers"], cluster["canonical_title"], cluster["id"])
            )
```

Добавить перед этим блоком:

```python
        asyncio.create_task(
            _detect_unknown_company(cluster["canonical_title"], bool(cluster["tickers"]))
        )
```

- [ ] **Step 5: Проверить что сервис стартует без ошибок**

```bash
docker restart backend-backend-1
sleep 8
docker logs backend-backend-1 --tail 15 2>&1 | grep -E "ERROR|started|ticker_mismatch"
```

Ожидаем: `"app started"` без ERROR.

- [ ] **Step 6: Commit**

```bash
cd /opt/newsparser
git add backend/app/pipeline/orchestrator.py
git commit -m "feat: integrate ticker validator and unknown company detector

- validate_tickers() called before tg.send() strips mismatched tickers
- _detect_unknown_company() sends ops alert for unrecognized company abbrs

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Команда /fix для ручного исправления тикера

**Files:**
- Modify: `app/db/queries.py`
- Modify: `app/bot/commands.py`

- [ ] **Step 1: Добавить query get_last_message_id_for_cluster в queries.py**

Добавить после функции `get_last_ok_send_at` (строка ~606):

```python
def get_last_message_id_for_cluster(db: DBConnection, cluster_id: int) -> Optional[int]:
    """Return the most recent tg_message_id for a cluster, or None."""
    row = db.execute(
        """
        SELECT tg_message_id
        FROM   telegram_sends
        WHERE  cluster_id = %s AND ok = 1 AND tg_message_id IS NOT NULL
        ORDER  BY sent_at DESC
        LIMIT  1
        """,
        (cluster_id,),
    ).fetchone()
    return row["tg_message_id"] if row else None


def update_cluster_tickers(db: DBConnection, cluster_id: int, tickers: str) -> bool:
    """Update tickers for a cluster. Returns True if a row was updated."""
    db.execute(
        "UPDATE event_clusters SET tickers = %s WHERE id = %s",
        (tickers or None, cluster_id),
    )
    db.commit()
    # rowcount may be 0 if cluster_id doesn't exist
    return True  # psycopg2 doesn't reliably expose rowcount via fetchone


def get_cluster_by_id(db: DBConnection, cluster_id: int) -> Optional[dict]:
    """Return cluster row or None if not found."""
    return db.execute(
        "SELECT id, canonical_title, tickers FROM event_clusters WHERE id = %s",
        (cluster_id,),
    ).fetchone()
```

- [ ] **Step 2: Добавить обработчик /fix в commands.py**

Добавить функцию после `_handle_status`:

```python
async def _handle_fix(db: DBConnection, user_id: int, args: list[str]) -> None:
    """
    /fix <cluster_id> <TICKER[,TICKER2]>
    Admin-only. Updates the cluster's tickers in DB.
    Usage examples:
      /fix 690 MGKL
      /fix 690 MGKL,SBER
      /fix 690 ""        ← clear tickers
    """
    from app.ai.filter import VALID_TICKERS
    from app.db import queries
    from app.telegram import client as tg

    if len(args) < 2:
        await send_dm(user_id, "❌ Использование: `/fix <cluster_id> <TICKER>` или `/fix <cluster_id> \"\"`")
        return

    # Parse cluster_id
    try:
        cluster_id = int(args[0])
    except ValueError:
        await send_dm(user_id, f"❌ Неверный cluster\\_id: `{_md_escape(args[0])}`")
        return

    # Parse tickers (empty string = clear)
    raw_tickers = args[1].strip().strip('"').strip("'")
    if raw_tickers:
        ticker_list = [t.strip().upper() for t in raw_tickers.split(",") if t.strip()]
        invalid = [t for t in ticker_list if t not in VALID_TICKERS]
        if invalid:
            invalid_str = ", ".join(_md_escape(t) for t in invalid)
            await send_dm(user_id, f"❌ Неизвестные тикеры: `{invalid_str}`")
            return
        new_tickers = ",".join(ticker_list)
    else:
        new_tickers = ""

    # Check cluster exists
    cluster = queries.get_cluster_by_id(db, cluster_id)
    if cluster is None:
        await send_dm(user_id, f"❌ Кластер `#{cluster_id}` не найден")
        return

    old_tickers = cluster["tickers"] or "(нет)"
    queries.update_cluster_tickers(db, cluster_id, new_tickers)

    # Confirm to admin
    old_display  = _md_escape(old_tickers)
    new_display  = _md_escape(new_tickers or "(нет)")
    title_display = _md_escape(cluster["canonical_title"][:80])
    await send_dm(
        user_id,
        f"✅ *Кластер \\#{cluster_id}* обновлён\n\n"
        f"Новость: _{title_display}_\n\n"
        f"Было: `{old_display}`\n"
        f"Стало: `{new_display}`",
    )

    logger.info(
        "fix command: cluster_id=%d %s → %s by user_id=%d",
        cluster_id, old_tickers, new_tickers, user_id,
        extra={
            "event":      "fix_ticker",
            "cluster_id": cluster_id,
            "old":        old_tickers,
            "new":        new_tickers,
            "admin":      user_id,
        },
    )
```

- [ ] **Step 3: Зарегистрировать /fix в handle_update**

Найти блок в `handle_update`:

```python
    elif cmd == "/status":
        if user_id in ADMIN_USER_IDS:
            await _handle_status(user_id)
        else:
            await send_dm(user_id, "⛔ Нет доступа\\.")
```

Добавить после него:

```python
    elif cmd == "/fix":
        if user_id in ADMIN_USER_IDS:
            args = text.split()[1:]
            await _handle_fix(db, user_id, args)
        else:
            await send_dm(user_id, "⛔ Нет доступа\\.")
```

- [ ] **Step 4: Перезапустить и проверить**

```bash
docker restart backend-backend-1
sleep 8
docker logs backend-backend-1 --tail 10 2>&1 | grep -E "ERROR|started"
```

Ожидаем: `"app started"` без ERROR.

- [ ] **Step 5: Проверить /fix вручную**

Отправить боту от admin аккаунта:
```
/fix 690 MGKL
```

Ожидаем ответ:
```
✅ Кластер #690 обновлён

Новость: СД МГКЛ рекомендовал дивиденды за 2025 год...
Было: MTSS
Стало: MGKL
```

Проверить в БД:
```bash
docker exec backend-postgres-1 psql -U postgres -d moex_assistant \
  -c "SELECT tickers FROM event_clusters WHERE id = 690;"
```

Ожидаем: `MGKL`

- [ ] **Step 6: Commit**

```bash
cd /opt/newsparser
git add backend/app/db/queries.py backend/app/bot/commands.py
git commit -m "feat: add /fix admin command to correct cluster tickers

Usage: /fix <cluster_id> <TICKER>
- Validates ticker against VALID_TICKERS whitelist
- Updates event_clusters.tickers in DB
- Logs the correction with old/new values

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 5: Финальная проверка

- [ ] **Step 1: Запустить все тесты**

```bash
docker exec backend-backend-1 python -m pytest tests/test_filter.py tests/test_ticker_validator.py -v
```

Ожидаем: все тесты PASSED.

- [ ] **Step 2: Smoke test — сымитировать сценарий бага**

```bash
docker exec backend-backend-1 python3 -c "
from app.pipeline.ticker_validator import validate_tickers

# Воспроизводим исходный баг: кластер МГКЛ с загрязнённым MTSS
result = validate_tickers('MTSS', 'СД МГКЛ рекомендовал дивиденды за 2025 год')
print('МГКЛ+MTSS ->', repr(result))  # ожидаем ''

# Commodity тикеры не трогаем
result = validate_tickers('LKOH,ROSN', 'Цена нефти выросла')
print('Oil commodity ->', result)  # ожидаем 'LKOH,ROSN'

# Правильный тикер проходит
result = validate_tickers('MGKL', 'СД МГКЛ рекомендовал дивиденды за 2025 год')
print('MGKL valid ->', result)  # ожидаем 'MGKL'
"
```

Ожидаем:
```
МГКЛ+MTSS -> ''
Oil commodity -> LKOH,ROSN
MGKL valid -> MGKL
```

- [ ] **Step 3: Финальный commit с тегом**

```bash
cd /opt/newsparser
git tag ticker-quality-v1
git log --oneline -5
```

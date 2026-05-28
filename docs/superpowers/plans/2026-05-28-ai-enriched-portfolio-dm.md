# AI-Enriched Portfolio DM Alerts — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Заменить простые портфельные DM (title + tickers) на AI-обогащённые алерты с сентиментом, summary, RAG-контекстом и market_effect; убрать AI-редактирование канальных постов.

**Architecture:** Один AI-вызов на событие — результат используется только для DM. Канальный пост остаётся простым и финальным (без `edit_message`). RAG-контекст: последние 5 кластеров по тикерам из event_clusters за 14 дней — передаётся в AI-промпт до вызова.

**Tech Stack:** Python 3.11, psycopg2, OpenRouter API (meta-llama/llama-3.3-70b-instruct), python-telegram-bot (aiohttp-based), pytest + pytest-asyncio

---

## Новый формат DM

```
🔴 *Газпром снижает дивиденды*

СД рекомендовал 15 руб. — вдвое ниже ожиданий рынка

📌 На фоне двух недель давления из-за падения цен на газ

⚡ Давление на акции, возможна коррекция

$GAZP
```

Строка `📌 context` включается только если AI вернул непустой `context`.

---

## Изменяемые файлы

| Файл | Что меняется |
|------|-------------|
| `app/db/queries.py` | +`get_recent_cluster_titles_for_tickers()` |
| `app/ai/analyzer.py` | +`context` в `AIAnalysis`; +`recent_context` param; обновлённый промпт |
| `app/pipeline/orchestrator.py` | убрать `edit_message`; убрать немедленный `_notify_portfolio`; RAG + portfolio в `_ai_enrich` |
| `app/bot/portfolio.py` | +`notify_with_ai()` |
| `tests/test_portfolio.py` | +тесты для `notify_with_ai` и нового query |

---

## Task 1: RAG-запрос в queries.py

**Files:**
- Modify: `app/db/queries.py` (добавить в секцию portfolio, после `get_subscribed_users`)
- Test: `tests/test_portfolio.py`

- [ ] **Step 1: Написать failing test**

Добавить в `tests/test_portfolio.py` после секции `# ── subscription CRUD`:

```python
# ── RAG context query ─────────────────────────────────────────────────────────

def _insert_cluster(db, title: str, tickers: str, status: str = "published") -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db.execute(
        """
        INSERT INTO event_clusters
            (canonical_title, title_tokens, keywords, best_score, tickers, status,
             first_seen_at, last_updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (title, title.lower(), title.lower(), 50, tickers, status, now, now),
    )
    db.commit()


def test_get_recent_cluster_titles_returns_matching(db):
    _insert_cluster(db, "Газпром снизил поставки", "GAZP")
    from app.db.queries import get_recent_cluster_titles_for_tickers
    result = get_recent_cluster_titles_for_tickers(db, ["GAZP"])
    assert "Газпром снизил поставки" in result


def test_get_recent_cluster_titles_no_match(db):
    _insert_cluster(db, "Сбер отчитался", "SBER")
    from app.db.queries import get_recent_cluster_titles_for_tickers
    result = get_recent_cluster_titles_for_tickers(db, ["GAZP"])
    assert result == []


def test_get_recent_cluster_titles_empty_tickers(db):
    from app.db.queries import get_recent_cluster_titles_for_tickers
    result = get_recent_cluster_titles_for_tickers(db, [])
    assert result == []


def test_get_recent_cluster_titles_ignores_new_status(db):
    _insert_cluster(db, "Кластер без публикации", "SBER", status="new")
    from app.db.queries import get_recent_cluster_titles_for_tickers
    result = get_recent_cluster_titles_for_tickers(db, ["SBER"])
    assert result == []


def test_get_recent_cluster_titles_multi_ticker(db):
    _insert_cluster(db, "Газпром и Новатэк под давлением", "GAZP,NVTK")
    from app.db.queries import get_recent_cluster_titles_for_tickers
    result = get_recent_cluster_titles_for_tickers(db, ["NVTK"])
    assert "Газпром и Новатэк под давлением" in result
```

- [ ] **Step 2: Запустить тест — убедиться что FAIL**

```bash
cd /opt/newsparser/backend
docker exec backend-backend-1 python -m pytest tests/test_portfolio.py::test_get_recent_cluster_titles_returns_matching -v
```

Ожидаем: `ImportError: cannot import name get_recent_cluster_titles_for_tickers`

- [ ] **Step 3: Реализовать функцию в queries.py**

Добавить после функции `get_subscribed_users` (строка ~718):

```python
def get_recent_cluster_titles_for_tickers(
    db: DBConnection, tickers: list[str], limit: int = 5, days: int = 14
) -> list[str]:
    """Return recent published cluster titles matching any of the tickers (RAG context)."""
    if not tickers:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ilike_clauses = " OR ".join("tickers ILIKE %s" for _ in tickers)
    params = tuple(f"%{t}%" for t in tickers) + (cutoff, limit)
    rows = db.execute(
        f"""
        SELECT canonical_title FROM event_clusters
        WHERE ({ilike_clauses})
          AND last_updated_at > %s
          AND status IN ('published', 'updated')
        ORDER BY last_updated_at DESC
        LIMIT %s
        """,
        params,
    ).fetchall()
    return [row["canonical_title"] for row in rows]
```

- [ ] **Step 4: Запустить все новые тесты — убедиться что PASS**

```bash
docker exec backend-backend-1 python -m pytest tests/test_portfolio.py::test_get_recent_cluster_titles_returns_matching tests/test_portfolio.py::test_get_recent_cluster_titles_no_match tests/test_portfolio.py::test_get_recent_cluster_titles_empty_tickers tests/test_portfolio.py::test_get_recent_cluster_titles_ignores_new_status tests/test_portfolio.py::test_get_recent_cluster_titles_multi_ticker -v
```

Ожидаем: 5 PASSED

- [ ] **Step 5: Commit**

```bash
cd /opt/newsparser
git add backend/app/db/queries.py backend/tests/test_portfolio.py
git commit -m "feat: add get_recent_cluster_titles_for_tickers for RAG context"
```

---

## Task 2: Добавить поле context в AIAnalysis

**Files:**
- Modify: `app/ai/analyzer.py`

- [ ] **Step 1: Добавить `context` в dataclass и `_validate()`**

В `AIAnalysis` dataclass добавить поле (после `tickers`):
```python
context: str = ""        # why this matters now (from RAG context), may be empty
```

В `_validate()` добавить после `affects`:
```python
context = str(data.get("context", "")).strip()
```

И добавить `context=context` в `return AIAnalysis(...)`.

- [ ] **Step 2: Обновить подпись `analyze()` — добавить `recent_context`**

```python
async def analyze(title: str, text: str = "", recent_context: list[str] = []) -> Optional[AIAnalysis]:
```

- [ ] **Step 3: Добавить секцию `context` в системный промпт**

В `_SYSTEM_PROMPT` после секции `5. ВЛИЯЕТ НА` добавить новую секцию `6. КОНТЕКСТ`:

```
---

6. КОНТЕКСТ (context)
Если переданы последние события по тикерам — напиши 1 предложение:
почему данное событие важно именно сейчас, в контексте этих событий.
Стиль: «На фоне ...», «После ...», «Вопреки ...».
Если последних событий нет или они не релевантны — верни пустую строку "".
Максимум 15 слов.

---
```

И сдвинуть нумерацию: `6. ТИКЕРЫ` → `7. ТИКЕРЫ`, `7. ПРАВИЛА` → `8. ПРАВИЛА`.

Добавить `"context"` в JSON-схему ответа:
```json
"context": "На фоне падения цен на газ в Европе"
```

- [ ] **Step 4: Передавать recent_context в user-сообщение**

Обновить формирование user-сообщения в `analyze()`:

```python
user_content = _USER_TEMPLATE.format(title=title, text=text or title)
if recent_context:
    events_block = "\n".join(f"• {t}" for t in recent_context[:5])
    user_content += f"\n\nПоследние события по этим тикерам:\n{events_block}"
```

- [ ] **Step 5: Запустить тесты analyzer**

```bash
docker exec backend-backend-1 python -m pytest tests/test_filter.py -v
```

Ожидаем: все PASSED (filter tests не затронуты)

- [ ] **Step 6: Commit**

```bash
cd /opt/newsparser
git add backend/app/ai/analyzer.py
git commit -m "feat: add context field to AIAnalysis with RAG-based prompt enrichment"
```

---

## Task 3: notify_with_ai() в portfolio.py

> ⚠️ Требует Task 2 — поле `context` должно быть добавлено в `AIAnalysis` до запуска этих тестов.

**Files:**
- Modify: `app/bot/portfolio.py`
- Test: `tests/test_portfolio.py`

- [ ] **Step 1: Написать failing тест**

Добавить в `tests/test_portfolio.py` в секцию `# ── notification dispatch`:

```python
@pytest.mark.asyncio
async def test_notify_with_ai_sends_enriched_dm(db):
    queries.set_user_tickers(db, 111, ["SBER"])

    from app.ai.analyzer import AIAnalysis
    ai = AIAnalysis(
        title="ЦБ сохранил ставку",
        impact="positive",
        emoji="🟢",
        summary="Банк России оставил ставку на уровне 21%",
        market_effect="поддержка для ОФЗ и банков",
        affects="ОФЗ · акции",
        tickers=["SBER"],
        context="На фоне замедления инфляции",
    )

    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 42

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
        patch.object(db, "close"),
    ):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("SBER", ai, cluster_id=1)

    assert len(sent_texts) == 1
    msg = sent_texts[0]
    assert "🟢" in msg
    assert "ЦБ сохранил ставку" in msg
    assert "Банк России оставил ставку" in msg
    assert "поддержка для ОФЗ" in msg
    assert "SBER" in msg


@pytest.mark.asyncio
async def test_notify_with_ai_omits_empty_context(db):
    queries.set_user_tickers(db, 111, ["SBER"])

    from app.ai.analyzer import AIAnalysis
    ai = AIAnalysis(
        title="ЦБ сохранил ставку",
        impact="positive",
        emoji="🟢",
        summary="Банк России оставил ставку на уровне 21%",
        market_effect="поддержка для ОФЗ",
        affects="ОФЗ · акции",
        tickers=["SBER"],
        context="",  # пустой
    )

    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 42

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
        patch.object(db, "close"),
    ):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("SBER", ai, cluster_id=1)

    assert "📌" not in sent_texts[0]


@pytest.mark.asyncio
async def test_notify_with_ai_uses_ai_tickers(db):
    queries.set_user_tickers(db, 111, ["SBER"])

    from app.ai.analyzer import AIAnalysis
    ai = AIAnalysis(
        title="Банки под давлением",
        impact="negative",
        emoji="🔴",
        summary="Санкции затронули банковский сектор",
        market_effect="давление на банки",
        affects="акции",
        tickers=["SBER", "VTBR"],  # AI-тикеры
        context="",
    )

    sent_texts: list[str] = []

    async def capture_dm(user_id, text, **kwargs):
        sent_texts.append(text)
        return 42

    with (
        patch("app.bot.portfolio.get_db", return_value=db),
        patch("app.bot.portfolio.send_dm", side_effect=capture_dm),
        patch.object(db, "close"),
    ):
        from app.bot.portfolio import notify_with_ai
        await notify_with_ai("SBER", ai, cluster_id=1)  # keyword ticker только SBER

    # DM должен содержать оба AI-тикера, не только keyword-тикер
    assert "VTBR" in sent_texts[0]
```

- [ ] **Step 2: Запустить тесты — убедиться что FAIL**

```bash
docker exec backend-backend-1 python -m pytest tests/test_portfolio.py::test_notify_with_ai_sends_enriched_dm -v
```

Ожидаем: `ImportError: cannot import name 'notify_with_ai'`

- [ ] **Step 3: Реализовать notify_with_ai()**

Добавить в `app/bot/portfolio.py` после функции `notify()`:

```python
async def notify_with_ai(tickers_raw: str, ai_analysis: "AIAnalysis", cluster_id: int) -> None:
    """Send AI-enriched DM to all users subscribed to any ticker in this cluster."""
    from app.ai.analyzer import AIAnalysis as _AIAnalysis  # avoid circular import at module level
    from app.telegram.formatter import _esc

    tickers = [t.strip() for t in tickers_raw.split(",") if t.strip()]
    if not tickers:
        return

    db = get_db()
    try:
        user_ids = queries.get_subscribed_users(db, tickers)
    finally:
        db.close()

    if not user_ids:
        metrics.inc(metrics.PORTFOLIO_NO_SUBS)
        return

    # AI тикеры приоритетнее keyword-тикеров из кластера
    dm_tickers = ai_analysis.tickers if ai_analysis.tickers else tickers
    tickers_line = " · ".join(f"\\${t}" for t in dm_tickers)

    parts = [
        f"{ai_analysis.emoji} *{_esc(ai_analysis.title)}*",
        "",
        _esc(ai_analysis.summary),
    ]
    if ai_analysis.context:
        parts += ["", f"📌 {_esc(ai_analysis.context)}"]
    parts += [
        "",
        f"⚡ {_esc(ai_analysis.market_effect)}",
    ]
    if tickers_line:
        parts += ["", tickers_line]

    text = "\n".join(parts)

    for user_id in user_ids:
        msg_id = await send_dm(user_id, text)
        ok = msg_id is not None
        metrics.inc(metrics.PORTFOLIO_DM_SENT if ok else metrics.PORTFOLIO_DM_FAILED)
        logger.info(
            "portfolio notify_with_ai %s: user_id=%d cluster_id=%d",
            "ok" if ok else "failed",
            user_id,
            cluster_id,
            extra={
                "event":      "portfolio_notify_ai_ok" if ok else "portfolio_notify_ai_failed",
                "user_id":    user_id,
                "cluster_id": cluster_id,
            },
        )
```

Также добавить в начало файла `from __future__ import annotations` если его нет (для type hint строки).

- [ ] **Step 4: Запустить все тесты portfolio**

```bash
docker exec backend-backend-1 python -m pytest tests/test_portfolio.py -v
```

Ожидаем: все PASSED

- [ ] **Step 5: Commit**

```bash
cd /opt/newsparser
git add backend/app/bot/portfolio.py backend/tests/test_portfolio.py
git commit -m "feat: add notify_with_ai() for AI-enriched portfolio DMs"
```

---

## Task 4: Обновить orchestrator.py

**Files:**
- Modify: `app/pipeline/orchestrator.py`

- [ ] **Step 1: Убрать немедленный _notify_portfolio из _run()**

Найти блок (примерно строки 180-185):
```python
if cluster["tickers"]:
    asyncio.create_task(
        _notify_portfolio(cluster["tickers"], cluster["canonical_title"], cluster["id"])
    )
    asyncio.create_task(
        capture_price_snapshot(cluster["id"], cluster["tickers"], score_result.event_type.value)
    )
```

Заменить на:
```python
if cluster["tickers"]:
    asyncio.create_task(
        capture_price_snapshot(cluster["id"], cluster["tickers"], score_result.event_type.value)
    )
```

- [ ] **Step 2: Обновить вызов _ai_enrich — добавить tickers_raw и canonical_title**

Найти:
```python
if msg_id and settings.openrouter_api_key:
    asyncio.create_task(
        _ai_enrich(article.title, article.content, cluster, score_result, pub, msg_id, correlations)
    )
```

Заменить на:
```python
tickers_raw = cluster["tickers"] or ""
if msg_id and settings.openrouter_api_key:
    asyncio.create_task(
        _ai_enrich(
            article.title, article.content, cluster, score_result, pub, msg_id,
            correlations, tickers_raw=tickers_raw,
            canonical_title=cluster["canonical_title"],
        )
    )
elif tickers_raw:
    # нет API ключа — отправить простой DM сразу
    asyncio.create_task(
        _notify_portfolio(tickers_raw, cluster["canonical_title"], cluster["id"])
    )
```

- [ ] **Step 3: Обновить сигнатуру _ai_enrich**

Найти:
```python
async def _ai_enrich(
    title: str,
    content: str,
    cluster: Any,
    score_result: scorer.ScoreResult,
    pub: PublishDecision,
    message_id: int,
    correlations: list | None = None,
) -> None:
```

Заменить на:
```python
async def _ai_enrich(
    title: str,
    content: str,
    cluster: Any,
    score_result: scorer.ScoreResult,
    pub: PublishDecision,
    message_id: int,
    correlations: list | None = None,
    *,
    tickers_raw: str = "",
    canonical_title: str = "",
) -> None:
```

- [ ] **Step 4: Обновить тело _ai_enrich**

Найти текущее тело `_ai_enrich`:
```python
    try:
        ai_analysis = await analyzer.analyze(title, content)
        if ai_analysis is None:
            return
        enriched = format_message(cluster, score_result, pub.decision, ai_analysis, correlations=correlations or [])
        await tg.edit_message(message_id, enriched)
        logger.info(
            "AI enrich ok: edited message_id=%d cluster_id=%d",
            message_id,
            cluster["id"],
            extra={"event": "ai_enrich_ok", "cluster_id": cluster["id"]},
        )
    except Exception:
        logger.warning(
            "AI enrich failed: message_id=%d cluster_id=%d",
            message_id,
            cluster["id"],
            exc_info=True,
        )
```

Заменить на:
```python
    try:
        # RAG-контекст: последние события по тикерам
        recent_context: list[str] = []
        if tickers_raw:
            from app.db.database import get_db as _get_db
            from app.db import queries as _queries
            _db = _get_db()
            try:
                ticker_list = [t.strip() for t in tickers_raw.split(",") if t.strip()]
                recent_context = _queries.get_recent_cluster_titles_for_tickers(_db, ticker_list)
            finally:
                _db.close()

        ai_analysis = await analyzer.analyze(title, content, recent_context=recent_context)
        if ai_analysis is None:
            # AI недоступен — fallback на простой DM
            if tickers_raw:
                await _notify_portfolio(tickers_raw, canonical_title, cluster["id"])
            return

        # Отправить обогащённый DM подписчикам
        if tickers_raw:
            from app.bot.portfolio import notify_with_ai
            await notify_with_ai(tickers_raw, ai_analysis, cluster["id"])

        logger.info(
            "AI enrich ok: cluster_id=%d",
            cluster["id"],
            extra={"event": "ai_enrich_ok", "cluster_id": cluster["id"]},
        )
    except Exception:
        logger.warning(
            "AI enrich failed: cluster_id=%d",
            cluster["id"],
            exc_info=True,
        )
        # Fallback: простой DM даже при необработанном исключении
        if tickers_raw:
            try:
                await _notify_portfolio(tickers_raw, canonical_title, cluster["id"])
            except Exception:
                logger.warning("portfolio fallback DM also failed", exc_info=True)
```

- [ ] **Step 5: Запустить тесты pipeline**

```bash
docker exec backend-backend-1 python -m pytest tests/test_pipeline.py tests/test_portfolio.py -v
```

Ожидаем: все PASSED

- [ ] **Step 6: Commit**

```bash
cd /opt/newsparser
git add backend/app/pipeline/orchestrator.py
git commit -m "feat: move portfolio DM to after AI enrichment, remove channel edit_message"
```

---

## Task 5: Финальная проверка и деплой

- [ ] **Step 1: Запустить полный тест-сьют**

```bash
docker exec backend-backend-1 python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Ожидаем: все PASSED (или только pre-existing fails)

- [ ] **Step 2: Перезапустить сервис**

```bash
cd /opt/newsparser/backend && docker compose restart backend
```

- [ ] **Step 3: Проверить логи**

```bash
docker logs backend-backend-1 --tail=50
```

Убедиться что нет ImportError или TypeError.

- [ ] **Step 4: Проверить что канальный пост НЕ редактируется**

Дождаться публикации любой новости в канале. Убедиться что сообщение не меняется через 15 сек после публикации.

- [ ] **Step 5: Commit финальный**

```bash
cd /opt/newsparser
git add -A
git commit -m "feat: AI-enriched portfolio DM alerts complete"
```

---

## Граничные случаи (уже обработаны в коде)

| Случай | Обработка |
|--------|-----------|
| AI упал / timeout | Fallback: простой DM через `_notify_portfolio` |
| `openrouter_api_key` не задан | Простой DM сразу (elif ветка в `_run`) |
| `context` пуст | Строка 📌 не включается в DM |
| AI вернул тикеры | Используем AI-тикеры вместо keyword |
| Нет подписчиков | DM не отправляется (текущее поведение в `notify_with_ai`) |
| Необработанное исключение в `_ai_enrich` | Catch + fallback DM |


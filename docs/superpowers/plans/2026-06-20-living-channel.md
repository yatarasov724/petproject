# Sprint "Живой канал" — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сделать посты в канале визуально разнообразными: разные шаблоны под тип события, глубина поста зависит от важности, AI пишет живым языком.

**Architecture:** Изменения только в `formatter.py` (новые шаблоны + score-depth) и `analyzer.py` (промпт). `score_result` уже передаётся в `format_message()` с нужными полями — оркестратор не трогаем.

**Tech Stack:** Python 3.11, pytest. Все тесты — чистые unit-тесты форматтера, БД не нужна.

## Global Constraints

- Запуск тестов: `docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q`
- Все существующие тесты должны проходить после каждого коммита
- MarkdownV2 Telegram: все raw-строки прогоняются через `_esc()` перед вставкой в шаблон
- Не трогать: `format_digest()`, `format_ticker_dm()`, логику pipeline, БД

---

## File Map

| Файл | Что меняем |
|------|-----------|
| `backend/app/telegram/formatter.py` | Новые шаблоны под EventType + score-depth в `format_message()` |
| `backend/app/ai/analyzer.py` | Переписываем `_SYSTEM_PROMPT` для живого языка |
| `backend/tests/test_formatter.py` | Новый тест-файл для `format_message()` |

---

## Task 1: Score-based depth в format_message()

**Files:**
- Modify: `backend/app/telegram/formatter.py`
- Create: `backend/tests/test_formatter.py`

**Interfaces:**
- Consumes: `format_message(cluster, score_result, decision, ai_analysis, correlations)` — существующая сигнатура, не меняется
- Produces: `_depth(score: int) -> str` — возвращает `"full"` / `"medium"` / `"compact"`

---

- [ ] **Шаг 1: Написать падающий тест**

Создать файл `backend/tests/test_formatter.py`:

```python
import pytest
from unittest.mock import MagicMock
from app.telegram.formatter import format_message, _depth
from app.pipeline.scorer import ScoreResult, EventType
from app.pipeline.publish_decision import Decision
from app.ai.analyzer import AIAnalysis


def _make_score(score: int, event_type: EventType = EventType.CORPORATE) -> ScoreResult:
    return ScoreResult(
        score=score,
        tier="tier1",
        event_type=event_type,
        base_score=score,
        keyword_bonus=0,
        source_bonus=0,
        type_bonus=0,
    )


def _make_ai() -> AIAnalysis:
    return AIAnalysis(
        summary="Сбербанк повысил прогноз прибыли",
        what_behind="Рост кредитного портфеля — акции под давлением покупок",
        watch_for="Отчёт за Q2 — конец июля",
        tickers=["SBER"],
        sentiment="positive",
    )


def _make_cluster() -> dict:
    return {"canonical_title": "Сбербанк повысил прогноз прибыли", "tickers": "SBER"}


class TestDepth:
    def test_score_50_returns_full(self):
        assert _depth(50) == "full"

    def test_score_80_returns_full(self):
        assert _depth(80) == "full"

    def test_score_49_returns_medium(self):
        assert _depth(49) == "medium"

    def test_score_30_returns_medium(self):
        assert _depth(30) == "medium"

    def test_score_29_returns_compact(self):
        assert _depth(29) == "compact"

    def test_score_0_returns_compact(self):
        assert _depth(0) == "compact"
```

- [ ] **Шаг 2: Запустить — убедиться что падает**

```bash
ssh root@213.108.1.38 'docker exec backend-backend-1 pytest tests/test_formatter.py::TestDepth -v 2>&1 | tail -10'
```

Ожидаем: `ImportError: cannot import name '_depth'`

- [ ] **Шаг 3: Добавить `_depth()` в formatter.py**

В `backend/app/telegram/formatter.py` добавить после импортов:

```python
def _depth(score: int) -> str:
    """Return format depth based on importance score."""
    if score >= 50:
        return "full"
    if score >= 30:
        return "medium"
    return "compact"
```

- [ ] **Шаг 4: Запустить — убедиться что проходит**

```bash
ssh root@213.108.1.38 'docker exec backend-backend-1 pytest tests/test_formatter.py::TestDepth -v 2>&1 | tail -10'
```

Ожидаем: `6 passed`

- [ ] **Шаг 5: Коммит**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/telegram/formatter.py backend/tests/test_formatter.py && git commit -m "feat(formatter): add score-based depth helper"'
```

---

## Task 2: Event-typed шаблоны в format_message()

**Files:**
- Modify: `backend/app/telegram/formatter.py`
- Modify: `backend/tests/test_formatter.py`

**Interfaces:**
- Consumes: `_depth(score) -> str` из Task 1
- Produces: обновлённый `format_message()` с event-typed ветками

Текущий вызов в orchestrator (строка 669):
```python
enriched_text = _fmt(cluster, score_result, pub.decision, ai_analysis, correlations)
```
Сигнатура не меняется.

---

- [ ] **Шаг 1: Написать падающие тесты — добавить в test_formatter.py**

```python
class TestFormatMessageEventTypes:
    """format_message produces distinct output per EventType."""

    def test_dividends_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.DIVIDENDS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "💰" in result

    def test_earnings_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.EARNINGS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "📊" in result

    def test_rate_decision_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.RATE_DECISION)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "🏦" in result

    def test_sanctions_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.SANCTIONS)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "⚠️" in result

    def test_war_escalation_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.WAR_ESCALATION)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "⚠️" in result

    def test_ipo_emoji_in_output(self):
        cluster = _make_cluster()
        sr = _make_score(40, EventType.IPO)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "🏢" in result


class TestFormatMessageScoreDepth:
    """High-score posts include more fields than low-score posts."""

    def test_full_score_includes_watch_for(self):
        cluster = _make_cluster()
        sr = _make_score(55, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "конец июля" in result  # watch_for content

    def test_medium_score_excludes_watch_for(self):
        cluster = _make_score(35, EventType.CORPORATE)
        ai = _make_ai()
        cluster_dict = _make_cluster()
        result = format_message(cluster_dict, _make_score(35), Decision.NEW, ai)
        assert "конец июля" not in result  # watch_for omitted

    def test_compact_score_excludes_what_behind(self):
        cluster = _make_cluster()
        sr = _make_score(20, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "кредитного портфеля" not in result  # what_behind omitted

    def test_compact_score_includes_summary(self):
        cluster = _make_cluster()
        sr = _make_score(20, EventType.CORPORATE)
        ai = _make_ai()
        result = format_message(cluster, sr, Decision.NEW, ai)
        assert "Сбербанк повысил прогноз" in result
```

- [ ] **Шаг 2: Запустить — убедиться что падают**

```bash
ssh root@213.108.1.38 'docker exec backend-backend-1 pytest tests/test_formatter.py::TestFormatMessageEventTypes tests/test_formatter.py::TestFormatMessageScoreDepth -v 2>&1 | tail -20'
```

Ожидаем: большинство тестов fail (EventType-эмодзи не совпадают со старыми).

- [ ] **Шаг 3: Переписать format_message() в formatter.py**

Заменить существующую функцию `format_message()`:

```python
def format_message(
    cluster: Any,
    score_result: ScoreResult,
    decision: Decision,
    ai_analysis: Optional[AIAnalysis] = None,
    correlations: Optional[list] = None,
) -> str:
    """Telegram channel message in MarkdownV2."""
    is_update = decision == Decision.UPDATE
    depth = _depth(score_result.score)
    etype = score_result.event_type

    if ai_analysis and ai_analysis.tickers:
        ticker_line = " ".join(f"\\${t}" for t in ai_analysis.tickers)
    else:
        ticker_line = _format_tickers_compact(cluster["tickers"])

    if ai_analysis:
        return _format_with_ai(ai_analysis, etype, is_update, depth, ticker_line, correlations)
    else:
        return _format_fallback(cluster, is_update, ticker_line)


_EVENT_EMOJI: dict = {
    EventType.DIVIDENDS:      "💰",
    EventType.EARNINGS:       "📊",
    EventType.RATE_DECISION:  "🏦",
    EventType.SANCTIONS:      "⚠️",
    EventType.WAR_ESCALATION: "⚠️",
    EventType.IPO:            "🏢",
    EventType.M_AND_A:        "🏢",
    EventType.SPO_BUYBACK:    "🏢",
}


def _format_with_ai(
    ai: AIAnalysis,
    etype: EventType,
    is_update: bool,
    depth: str,
    ticker_line: str,
    correlations: Optional[list],
) -> str:
    if is_update:
        prefix = "🔄 "
    else:
        base_emoji = _EVENT_EMOJI.get(etype)
        if base_emoji:
            prefix = f"{base_emoji} "
        elif ai.sentiment == "positive":
            prefix = "🟢 "
        elif ai.sentiment == "negative":
            prefix = "🔴 "
        else:
            prefix = ""

    parts = [f"{prefix}*{_esc(ai.summary)}*"]

    if depth in ("full", "medium") and ai.what_behind:
        parts += ["", f"_{_esc(ai.what_behind)}_"]

    if depth == "full" and ai.watch_for:
        parts += ["", f"_{_esc(ai.watch_for)}_"]

    if ticker_line:
        parts += ["", ticker_line]

    return "\n".join(parts)


def _format_fallback(cluster: Any, is_update: bool, ticker_line: str) -> str:
    emoji = "🔄" if is_update else "📰"
    parts = [f"{emoji} *{_esc(cluster['canonical_title'])}*"]
    if ticker_line:
        parts += ["", ticker_line]
    return "\n".join(parts)
```

Также убрать из импортов `ScoreResult` и `EventType` если не импортированы — добавить:

```python
from app.pipeline.scorer import EventType, ScoreResult
```

(уже есть в текущем файле — проверить)

- [ ] **Шаг 4: Запустить все тесты**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q 2>&1 | tail -20'
```

Ожидаем: все проходят. Если что-то сломалось — скорее всего тест сравнивает старый формат (ищем `"Контекст:"` — его теперь нет, убрали явный лейбл).

- [ ] **Шаг 5: Коммит**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/telegram/formatter.py backend/tests/test_formatter.py && git commit -m "feat(formatter): event-typed templates + score-based post depth"'
```

---

## Task 3: Живой язык в AI-промпте

**Files:**
- Modify: `backend/app/ai/analyzer.py` — только константа `_SYSTEM_PROMPT`

**Interfaces:**
- Consumes: ничего нового
- Produces: тот же `AIAnalysis` dataclass — API не меняется

Что меняем в промпте:
- Убрать жирные заголовки-секции `1. СУТЬ`, `2. ЧТО ЗА ЭТИМ СТОИТ`, `3. НА ЧТО СМОТРЕТЬ` — они задают казённый стиль
- `summary`: одна сильная фраза — не пересказ заголовка, а осмысление события
- `what_behind`: почему это важно КОНКРЕТНО для инвестора — с позицией, не нейтрально
- `watch_for`: конкретный ориентир (дата/цифра/событие), без "следить за"
- Голос: умный знакомый, разбирается в рынке, говорит по-человечески

---

- [ ] **Шаг 1: Написать тест на промпт-регрессии**

Добавить в `backend/tests/test_formatter.py`:

```python
class TestAnalyzerPrompt:
    """Smoke tests that _SYSTEM_PROMPT contains required guardrails."""

    def test_noise_filter_present(self):
        from app.ai.analyzer import _SYSTEM_PROMPT
        assert "ФИЛЬТР ШУМА" in _SYSTEM_PROMPT

    def test_forbidden_tickers_warning_present(self):
        from app.ai.analyzer import _SYSTEM_PROMPT
        assert "ЗАПРЕЩЁННЫЕ ОШИБКИ" in _SYSTEM_PROMPT

    def test_json_format_instruction_present(self):
        from app.ai.analyzer import _SYSTEM_PROMPT
        assert '"summary"' in _SYSTEM_PROMPT
        assert '"watch_for"' in _SYSTEM_PROMPT
```

- [ ] **Шаг 2: Запустить — убедиться что проходят (они должны)**

```bash
ssh root@213.108.1.38 'docker exec backend-backend-1 pytest tests/test_formatter.py::TestAnalyzerPrompt -v 2>&1 | tail -10'
```

Ожидаем: `3 passed` — это baseline, они должны остаться зелёными после правки.

- [ ] **Шаг 3: Переписать вводную часть _SYSTEM_PROMPT в analyzer.py**

Заменить блок от `_SYSTEM_PROMPT = """\` до первой строки `ФИЛЬТР ШУМА`:

```python
_SYSTEM_PROMPT = """\
Ты — финансовый редактор Telegram-канала для российских частных инвесторов. Пишешь коротко, по делу, живым языком — как умный знакомый, который следит за рынком.

На вход — заголовок и текст новости. Твоя задача: превратить её в три коротких строки для поста.

ФИЛЬТР ШУМА — проверь ПЕРВЫМ, до заполнения полей
"""
```

Затем заменить секции 1-3 (оставив 4-5 и всё что ниже без изменений):

```
Поля для заполнения:

summary — одна фраза, ~15 слов
  • Не пересказывай заголовок — осмысли событие
  • Пиши активно: "Газпром режет дивиденды второй год подряд", а не "Газпром сообщил об отказе от дивидендов"
  • Без оценочных слов ("важно", "значительно", "существенно")

what_behind — одна строка, почему это важно инвестору
  • Конкретная причина → конкретное следствие для акций или сектора
  • Можно иметь позицию: "Долг растёт быстрее прибыли — акционеры платят за это"
  • Запрещены пустые фразы: "ситуация сохраняется", "давление продолжается", "риски остаются"

watch_for — одна строка, конкретный ориентир
  • Называй событие, дату или метрику: "Отчёт за Q2 — конец июля", "Заседание ЦБ 25 июля"
  • Без "Следить за" в начале — сразу к делу
  • Если даты нет — метрику: "Объём экспорта нефти в июне", "Квартальная отчётность"
  • Запрещено: "следить за официальными заявлениями"
```

- [ ] **Шаг 4: Запустить тесты-регрессии**

```bash
ssh root@213.108.1.38 'docker exec backend-backend-1 pytest tests/test_formatter.py::TestAnalyzerPrompt -v 2>&1 | tail -10'
```

Ожидаем: `3 passed`

- [ ] **Шаг 5: Запустить все тесты**

```bash
ssh root@213.108.1.38 'docker exec -e TEST_DATABASE_URL=postgresql://postgres:postgres@postgres/moex_assistant_test backend-backend-1 pytest tests/ -q 2>&1 | tail -10'
```

Ожидаем: все проходят.

- [ ] **Шаг 6: Коммит**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && git add backend/app/ai/analyzer.py && git commit -m "feat(ai): rewrite system prompt for livelier, more direct language"'
```

---

## Task 4: Deploy и наблюдение

**Files:**
- Нет изменений кода

---

- [ ] **Шаг 1: Перезапустить сервис**

```bash
ssh root@213.108.1.38 'cd /opt/newsparser && docker compose restart backend'
```

- [ ] **Шаг 2: Проверить что поднялся**

```bash
ssh root@213.108.1.38 'curl -s http://localhost:8000/health | python3 -m json.tool'
```

Ожидаем: `"status": "ok"`

- [ ] **Шаг 3: Посмотреть логи первых публикаций**

```bash
ssh root@213.108.1.38 'docker logs backend-backend-1 --since 10m 2>&1 | grep -E "publish|format|event_type" | head -20'
```

- [ ] **Шаг 4: Проверить первые 3–5 постов в канале**

Открыть канал и убедиться:
- Дивидендная новость → `💰`
- Отчётность → `📊`
- Мелкая новость (score < 30) → только summary + тикеры, без контекстного блока
- Текст звучит живее чем раньше

- [ ] **Шаг 5: Коммит если нужны правки после наблюдения**

Если что-то не так — правим и коммитим с описанием что именно подправили.

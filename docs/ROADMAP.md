# Roadmap — Следующий релиз: Quality Sprint v2

## Context

Проект — Telegram-бот для российских розничных инвесторов (`/opt/newsparser` на сервере 213.108.1.38). Агрегирует финансовые новости из 14+ RSS-источников и 5 Telegram-каналов, фильтрует через AI (Llama 3.3 70B / OpenRouter), отправляет торговые сигналы и портфельные DM.

**Цель релиза:** улучшение качества данных — два главных болевых места:
1. **Ложные тикеры** — AI назначает неверные тикеры к сообщениям
2. **Дубликаты и шум** — одно событие проходит несколько раз или шумовые статьи проскальзывают мимо фильтра

---

## Направление 1: Валидация тикеров

**Проблема:** Llama 3.3 70B иногда галлюцинирует тикеры (неоднозначные аббревиатуры, несуществующие инструменты).

**Текущий стек:** `backend/app/ai/` — analyzer + signal, `backend/app/pipeline/` — orchestrator, ticker_quality с embedding similarity.

### Задачи

1. **Реестр MOEX-инструментов** (`backend/app/ai/ticker_registry.py`)
   - Загружать список активных инструментов с MOEX ISS API: `https://iss.moex.com/iss/engines/stock/markets/shares/securities.json`
   - Кэшировать в PostgreSQL таблицу `moex_instruments` (ticker, short_name, full_name)
   - Обновлять раз в сутки через APScheduler
   - Строить нормализованный словарь `{company_alias: ticker}`

2. **Post-validation слой** (`backend/app/ai/ticker_validator.py`)
   - После AI-назначения тикеров — проверять каждый по реестру
   - Если тикер не найден → strip или заменить на `None`
   - Стоп-лист паттернов для частых ошибок: «ЦБ», «Рубль», «ФРС» → не тикер
   - Если embedding similarity < threshold (сейчас в ticker_quality) → не выставлять

3. **Бенчмарк** — выборка из 100 последних сообщений канала, ручная разметка, измерить baseline accuracy до и после

### Файлы для изменения
- `backend/app/ai/analyzer.py` — добавить вызов validator после LLM ответа
- `backend/app/db/` — миграция для таблицы `moex_instruments`
- `backend/app/scheduler/` — добавить daily job для обновления реестра

---

## Направление 2: Шум и дубликаты

**Проблема:** 90% статей фильтруется (47K/52K) — но часть дублей всё ещё проходит; также иногда шумовые новости проникают в канал.

**Текущий стек:** `backend/app/pipeline/orchestrator.py`, дедупликация через cosine similarity (sentence-transformers).

### Задачи

1. **Авторитет источника** (`backend/app/pipeline/source_authority.py`)
   - Ранжировать источники: ТАСС/Интерфакс = tier-1, РБК/Ведомости = tier-2, остальные = tier-3
   - Если tier-2/3 статья появляется в течение 60 мин после tier-1 с similarity > 0.85 → suppress
   - Конфиг в БД или settings file (изменяемый без деплоя)

2. **Time-window clustering** (`backend/app/pipeline/time_cluster.py`)
   - Группировать статьи об одном событии в 1-часовое окно
   - В канал проходит только одна — от наиболее авторитетного источника
   - Остальные логировать как `suppressed_duplicate`

3. **Тюнинг порогов дедупликации**
   - Проанализировать логи за последнюю неделю: найти случаи когда дубли прошли
   - Скорректировать cosine threshold (вероятно, поднять с текущего значения)
   - Добавить метрику `duplicates_escaped` в `/health` endpoint

4. **Улучшение noise filter**
   - Проверить prompt в `analyzer.py` — добавить явные примеры шума (пресс-релизы без событий, технические уведомления биржи)
   - Рассмотреть two-pass: первый pass быстрый (rule-based), второй (LLM) только для неоднозначных

### Файлы для изменения
- `backend/app/pipeline/orchestrator.py` — интегрировать source authority + time-window clustering
- `backend/app/ai/analyzer.py` — улучшить noise prompt
- `backend/app/core/` — конфиг авторитета источников

---

## Направление 3: MOEX реестр → актуализация тикеров

**Контекст:** В Quality Sprint v1 реализован реестр `moex_instruments` (262 инструмента с TQBR, sync ежедневно). Следующий шаг — использовать его для поддержания `TICKER_KEYWORDS` в актуальном состоянии.

### Задачи

1. **Скрипт расхождений** (`backend/scripts/ticker_gap_report.py`)
   - Сравнить `set(TICKER_KEYWORDS.keys())` с `get_moex_tickers()` из реестра
   - Вывести три списка:
     - **Новые на бирже, нет в боте** — кандидаты на добавление в `TICKER_KEYWORDS`
     - **Есть в боте, нет на бирже** — вероятно делистированные (POLY и др.)
     - **Пересечение** — всё нормально
   - Запуск: `docker exec backend-backend-1 python scripts/ticker_gap_report.py`

2. **Ops-алерт при делистинге** (`backend/app/scheduler/jobs.py`)
   - После ежедневного `moex_instruments_sync_job` — сравнить результат с `TICKER_KEYWORDS`
   - Если тикер из `TICKER_KEYWORDS` исчез из реестра → отправить алерт в ops-чат через `send_ops()`
   - Пример сообщения: `⚠️ Тикер POLY больше не торгуется на TQBR — проверь filter.py`

### Файлы для изменения
- Create: `backend/scripts/ticker_gap_report.py`
- Modify: `backend/app/scheduler/jobs.py` — добавить проверку в `moex_instruments_sync_job`
- Modify: `backend/tests/test_calendar_moex_client.py` или новый test файл — тест на алерт при делистинге

---

## Метрики успеха

| Метрика | Цель |
|---------|------|
| Ложные тикеры | < 5% от выборки (нужен baseline замер) |
| Дубли в канале | 0 за неделю наблюдения |
| Noise pass-through | -30% к текущему уровню |
| Regression: важные события | 0 потерь tier-1 новостей |

---

## Порядок реализации

1. Бенчмарк текущего состояния (baseline тикеры + дубли за неделю)
2. Реестр MOEX + ticker validator
3. Source authority + time-window clustering
4. Тюнинг порогов + noise prompt improvement
5. Измерение метрик → итерация если нужно
6. Опционально: feedback кнопка

---

## Верификация

- Запустить `pytest backend/tests/` — регрессия
- Поднять staging (второй docker-compose на другом порту) с новым кодом
- Прогнать 24-часовой трафик новостей, вручную проверить канал на дубли и тикеры
- Сравнить метрики в `/health` до и после

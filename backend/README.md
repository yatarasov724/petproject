# MOEX News Assistant — Backend

Мониторинг геополитических и финансовых новостей из российских RSS-лент.  
Pipeline: fetch → normalize → dedup → cluster → score → publish → Telegram.

---

## Быстрый старт

```bash
cd backend
cp .env.example .env           # заполнить BOT_TOKEN и CHANNEL_ID

pip install -r requirements.txt
python scripts/check_setup.py  # pre-flight проверка
uvicorn app.main:app --reload
```

---

## Обязательные переменные окружения

| Переменная | Где взять |
|---|---|
| `TELEGRAM_BOT_TOKEN` | [@BotFather](https://t.me/BotFather) → /newbot |
| `TELEGRAM_CHANNEL_ID` | Переслать любое сообщение из канала в [@userinfobot](https://t.me/userinfobot) |

**Важно:** бот должен быть добавлен в канал с правом «Публикация сообщений».

---

## Все переменные окружения

| Переменная | Дефолт | Описание |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | — | **Обязательна** |
| `TELEGRAM_CHANNEL_ID` | — | **Обязательна** |
| `DATABASE_URL` | `sqlite:///./moex_assistant.db` | Путь к SQLite |
| `FRONTEND_URL` | `http://localhost:3000` | CORS origin |
| `LOG_FORMAT` | `text` | `text` или `json` |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |
| `DRY_RUN` | `false` | `true` = pipeline без отправки в Telegram |

---

## Startup flow

```
uvicorn app.main:app
  │
  ├─ logging_setup.configure()     JSON или text, level из env
  ├─ Settings()                    читает .env, валидирует обязательные поля
  │
  ├─ @startup
  │    ├─ init_db()                CREATE TABLE IF NOT EXISTS (idемпотентно)
  │    ├─ seed_sources()           INSERT OR IGNORE для 5 RSS-источников
  │    └─ runner.start()           APScheduler: poll каждые 60s, cleanup каждые 24h
  │
  └─ @shutdown
       └─ runner.stop()            graceful shutdown
```

При первом запуске на пустой базе:
- Schema создаётся из `app/db/schema.sql`
- 5 RSS-источников вставляются автоматически
- Первый poll запускается через 60 секунд

---

## Локальный запуск

### Без Docker

```bash
# 1. Создать virtualenv
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Установить зависимости
pip install -r requirements.txt

# 3. Настроить env
cp .env.example .env
# Открыть .env и вписать TELEGRAM_BOT_TOKEN и TELEGRAM_CHANNEL_ID

# 4. Проверить конфиг
python scripts/check_setup.py

# 5. Запустить
uvicorn app.main:app --reload      # dev режим с авторелоадом
uvicorn app.main:app               # production-like (без --reload)
```

### Через Docker Compose

```bash
# 1. Настроить env
cp .env.example .env
# Вписать BOT_TOKEN и CHANNEL_ID

# 2. Запустить
docker compose up --build          # foreground с логами
docker compose up -d --build       # фоновый режим

# 3. Логи
docker compose logs -f

# 4. Остановить
docker compose down
```

SQLite файл сохраняется в `backend/data/moex_assistant.db`.

---

## Режим DRY_RUN

Pipeline отрабатывает полностью (fetch → cluster → score → decide), но Telegram HTTP-запрос не выполняется. Вместо этого в лог пишется текст сообщения.

```bash
DRY_RUN=true uvicorn app.main:app --reload
```

или в `.env`:
```
DRY_RUN=true
```

Кластеры помечаются как отправленные — повторной отправки на следующем цикле не будет.

---

## Добавление RSS-источников

**Автоматически (рекомендуется)** — в `app/db/queries.py`, список `_RSS_SEEDS`:

```python
_RSS_SEEDS = [
    ("RBC",        "https://rss.rbc.ru/finances/news.rss"),
    ("TASS",       "https://tass.ru/rss/v2.xml"),
    ("Interfax",   "https://www.interfax.ru/rss.asp"),
    ("Vedomosti",  "https://www.vedomosti.ru/rss/news"),
    ("Kommersant", "https://www.kommersant.ru/RSS/news.xml"),
    # Добавить сюда:
    ("Ведомости",  "https://example.com/rss"),
]
```

`seed_sources()` использует `INSERT OR IGNORE` — существующие источники не трогаются, новые добавляются при перезапуске.

**Напрямую в БД** (без перезапуска):
```sql
INSERT OR IGNORE INTO rss_sources (name, url) VALUES ('МойИсточник', 'https://example.com/rss');
```

---

## Smoke Test

### 1. Первый запуск — проверить schema и seed

```bash
DRY_RUN=true uvicorn app.main:app --reload
```

Ожидаемые логи (первые 5 секунд):
```
[INFO] app.db.database: Database initialized
[INFO] app.db.queries: RSS sources seeded (5 entries)
[INFO] app.scheduler.runner: Scheduler started (poll=60s, cleanup=24h)
[INFO] app.main: app started
```

### 2. Первый poll — проверить ingestion

Через 60 секунд:
```
[INFO] app.pipeline.fetcher: [RBC] fetched 25 articles
[INFO] app.pipeline.fetcher: [TASS] fetched 40 articles
[INFO] app.pipeline.fetcher: Poll complete: 5 sources, 130 raw articles
```

### 3. Проверить dedup и clustering

В логах DRY_RUN:
```
[DEBUG] app.pipeline.dedup: [RBC] exact dup: ...
[DEBUG] app.pipeline.clusterer: [TASS] joined cluster #3 (containment=0.67)
[INFO] app.pipeline.clusterer: [Interfax] new cluster #12: ЦБ повысил ставку...
```

### 4. Проверить publish decision

```
[INFO] app.telegram.client: [DRY RUN] would send NEW_EVENT cluster=#12 score=55
  *СТАВКА ЦБ*
  ЦБ повысил ключевую ставку до 21 процента
  ...
```

### 5. Проверить повторную отправку (не должна происходить)

На следующем poll (через 60s) для того же кластера:
```
[DEBUG] app.pipeline.dedup: exact dup: ... (skipped)
```
или
```
[DEBUG] app.pipeline.publish_decision: event_silenced ... cooldown active
```

### 6. Проверить health endpoint

```bash
curl http://localhost:8000/health | python3 -m json.tool
```

Ожидаемый ответ:
```json
{
  "status": "ok",
  "rss_sources": {"ok": 5},
  "clusters_24h": 12,
  "sends_24h": 3,
  "sends_ok_24h": 3,
  "counters": {
    "articles_fetched": 130,
    "articles_exact_dup": 45,
    "articles_near_dup": 12,
    "articles_noise": 60,
    "articles_processed": 13,
    "clusters_created": 10,
    "clusters_updated": 3,
    "events_published": 3,
    "tg_sent_ok": 3
  }
}
```

### 7. Проверить cleanup

Retention job запускается раз в 24 часа. Запустить вручную через SQLite:
```bash
sqlite3 moex_assistant.db "SELECT COUNT(*) FROM seen_articles;"
sqlite3 moex_assistant.db "SELECT COUNT(*) FROM event_clusters;"
```

### 8. Переключить DRY_RUN → реальная отправка

```bash
# Убрать DRY_RUN из .env, перезапустить
uvicorn app.main:app --reload
```

В логах должно появиться:
```
[INFO] app.telegram.client: sending NEW_EVENT cluster=#1 score=55
[INFO] app.telegram.client: SEND OK cluster=#1 tg_message_id=42
```

---

## Pipeline — справочник

```
RSS fetch (aiohttp, ETag/If-Modified-Since)
    │  60s interval, 5 sources concurrently
    ↓
normalize (stop words, MD5 hash, title_tokens)
    │  RawArticle dataclass
    ↓
dedup (exact hash → Jaccard near-dup, 4h window)
    │  EXACT_DUP / NEAR_DUP → stop
    ↓
score pre-filter (T1=50, T2=25, T3=10, noise=0)
    │  score < 10 → stop (NOISE)
    ↓
cluster (containment similarity ≥ 0.50, 4h window)
    │  join existing or create new
    ↓
rescore (with real source_count: +10 for 2, +20 for 3+)
    ↓
publish_decision (NEW_EVENT / UPDATE / SILENCE)
    │  NEW_EVENT: status='new', score ≥ 30
    │  UPDATE: cooldown expired + (3+ sources OR Δscore ≥ 15)
    │  SILENCE: everything else
    ↓
Telegram Bot API (MarkdownV2, retry 3×)
```

---

## Тесты

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

100 тестов, без внешних зависимостей.

---

## Структура проекта

```
backend/
├── app/
│   ├── core/
│   │   ├── config.py          Settings (pydantic-settings)
│   │   ├── logging_setup.py   JSON / text logging
│   │   └── metrics.py         In-memory counters
│   ├── db/
│   │   ├── schema.sql         DDL (4 таблицы)
│   │   ├── database.py        sqlite3 connection + init_db()
│   │   └── queries.py         Все SQL операции
│   ├── pipeline/
│   │   ├── fetcher.py         RSS ingestion (aiohttp + ETag)
│   │   ├── normalizer.py      Tokenization, RawArticle
│   │   ├── dedup.py           Exact + near-dup (Jaccard)
│   │   ├── clusterer.py       Event clustering (containment)
│   │   ├── scorer.py          Keyword scoring (T1/T2/T3)
│   │   ├── publish_decision.py Decision engine
│   │   └── orchestrator.py    Per-article pipeline
│   ├── scheduler/
│   │   ├── jobs.py            poll_job + cleanup_job
│   │   └── runner.py          APScheduler lifecycle
│   ├── telegram/
│   │   ├── client.py          Bot API client + retry
│   │   └── formatter.py       MarkdownV2 formatter
│   └── main.py                FastAPI app + /health
├── scripts/
│   └── check_setup.py         Pre-flight check
├── tests/                     100 pytest tests
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── requirements-dev.txt
```

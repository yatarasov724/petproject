# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
python3 -m pytest tests/ -q

# Run a single test file
python3 -m pytest tests/test_scorer.py -v

# Run a single test by name
python3 -m pytest tests/test_scorer.py::TestSanctions::test_sanctions_tier1 -v

# Start the service (production)
uvicorn app.main:app

# Start with auto-reload (development)
uvicorn app.main:app --reload

# Start with debug logging
LOG_LEVEL=DEBUG uvicorn app.main:app --reload

# Dry-run mode (pipeline runs fully, no Telegram calls)
DRY_RUN=true uvicorn app.main:app --reload

# Check setup (validates env, DB, Telegram connectivity)
python3 scripts/check_setup.py

# Docker
docker compose up --build
```

## Architecture

The service is a FastAPI app with an APScheduler background loop. There is no HTTP API for end-users — the only output is Telegram messages. The FastAPI app exists to expose `/health` and to host the scheduler lifecycle.

### Data flow (per poll cycle, every 60 s)

```
RSS feeds → fetcher → normalizer → orchestrator → Telegram channel
                                       │
                          dedup → age filter → scorer → clusterer → rescore → publish_decision → tg.send
```

1. **`fetcher.py`** — async HTTP fetch of all active `rss_sources` using `feedparser`. Supports ETag/304 conditional GET. Returns `list[RawArticle]`.
2. **`normalizer.py`** — converts a feedparser entry to `RawArticle`. Produces `title_tokens` (sorted lowercase Cyrillic tokens, stop words removed) and `raw_hash` (MD5 of tokens + calendar hour). Both are used for dedup.
3. **`orchestrator.py`** — runs each `RawArticle` through the full pipeline in sequence. Never raises — all exceptions are caught and returned as `Outcome.ERROR`.
4. **`dedup.py`** — two-stage: exact hash lookup (O(1) DB), then Jaccard near-dedup against last 4 h of `seen_articles` (in-memory, threshold 0.35). Near-dedup catches paraphrases across sources.
5. **`scorer.py`** — keyword-based scoring against the article title only. Three tiers (base 50/25/10) + type modifier + source modifier. Threshold to enter pipeline: `ARTICLE_MIN_SCORE=10`. Threshold to publish: `PUBLISH_THRESHOLD=30`.
6. **`clusterer.py`** — groups articles about the same event. Uses containment similarity (not Jaccard) against `event_clusters` from the last 4 h. Threshold: 0.50. New article → new cluster. Same event from another source → increments `source_count`.
7. **`publish_decision.py`** — decides NEW_EVENT / UPDATE / SILENCE based on cluster state and score. Seven ordered rules (see docstring). Key constants: `COOLDOWN_HOURS=2`, `FRESHNESS_HOURS=24`, `UPDATE_SOURCE_FLOOR=3`.
8. **`tg/client.py`** — formats and sends via Telegram Bot API. Handles 429 rate-limits (Retry-After) and 5xx with exponential backoff. Always writes to `telegram_sends` table including in `DRY_RUN`.

### Database (SQLite, single file)

Schema lives in `app/db/schema.sql` — applied at startup via `init_db()`. All queries are in `app/db/queries.py` — no ORM, raw `sqlite3`. Four tables:

- **`rss_sources`** — feed registry with backoff state (`status`: ok/backoff/dead)
- **`seen_articles`** — dedup store; rows are purged after 48 h by `cleanup_job`
- **`event_clusters`** — one row per event; holds `canonical_title`, `source_count`, publish state
- **`telegram_sends`** — audit log of every send attempt

### Scheduler (`app/scheduler/`)

- `runner.py` — wraps APScheduler; registers two jobs on startup
- `jobs.py` — `poll_job` (60 s interval) and `cleanup_job` (24 h interval)
- Each job opens its own DB connection and closes it in `finally`

### Scoring keywords (Russian inflection note)

Keywords in `scorer.py` use **stems** (truncated forms), not full nominative words, because Russian inflection changes word endings across cases. For example, `"санкци"` matches `"санкции"`, `"санкций"`, `"санкциям"`, etc. When adding new keywords, always verify they match the genitive/accusative forms that appear in real headlines, not just the nominative dictionary form.

### Key constants to tune

| Constant | Location | Default | Effect |
|---|---|---|---|
| `ARTICLE_MIN_SCORE` | scorer.py | 10 | Articles below this skip the pipeline |
| `PUBLISH_THRESHOLD` | scorer.py | 30 | Cluster must reach this to be sent |
| `JACCARD_THRESHOLD` | dedup.py | 0.35 | Near-dedup sensitivity |
| `MATCH_THRESHOLD` | clusterer.py | 0.50 | Clustering sensitivity |
| `ARTICLE_MAX_AGE_HOURS` | orchestrator.py | 24 | Articles older than this → NOISE |
| `FRESHNESS_HOURS` | publish_decision.py | 24 | Unpublished clusters older than this → SILENCE |
| `COOLDOWN_HOURS` | publish_decision.py | 2 | Min gap between sends for same cluster |

### Tests

Tests use an in-memory SQLite DB seeded from the real `schema.sql` (so schema changes are immediately caught). The `make_article()` factory in `conftest.py` always sets `published_at=datetime.now(timezone.utc)` so articles are never blocked by `ARTICLE_MAX_AGE_HOURS`.

Tests that exercise the full orchestrator pipeline mock `tg.send` — they do not make real HTTP calls. Tests that exercise `publish_decision.decide()` directly use `sqlite3.Row`-like structures.

### Environment variables

Required: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`  
Optional: `DATABASE_URL` (default: `sqlite:///./moex_assistant.db`), `LOG_FORMAT` (text/json), `LOG_LEVEL`, `DRY_RUN`

See `app/core/config.py` for the full list. The `.env` file is loaded automatically via `pydantic-settings`.

### Backup and restore

The DB is backed up automatically every 6 h by `backup_job` (APScheduler).
Backups are stored in `backend/backups/` as `moex_assistant_YYYYMMDD_HHMMSS.db`.
The last 10 backups are kept; older ones are rotated out automatically.

```bash
# One-shot manual backup
python scripts/backup_db.py

# List available backups
python scripts/backup_db.py --list

# Restore (interactive — stop the service first)
python scripts/restore_db.py

# Restore the latest backup non-interactively
python scripts/restore_db.py --latest

# Restore a specific file
python scripts/restore_db.py --file backups/moex_assistant_20260505_120000.db
```

Before a risky operation (migration, bulk SQL, schema change):
1. `python scripts/backup_db.py`
2. Do the operation
3. If something broke: `python scripts/restore_db.py --latest`

The current DB is always renamed to `moex_assistant.db.pre_restore` before
overwriting, so you can undo the restore manually if needed.

### Resetting state (when pipeline gets stuck on stale data)

If `seen_articles` is full of old articles and nothing publishes:

```sql
DELETE FROM seen_articles;
DELETE FROM event_clusters;
DELETE FROM telegram_sends;
UPDATE rss_sources SET etag=NULL, last_modified=NULL, status='ok', error_count=0, next_retry_at=NULL;
```

"""
Application settings loaded from environment variables / .env file.

Required variables (app won't start without them):
  TELEGRAM_BOT_TOKEN    — bot token from @BotFather
  TELEGRAM_CHANNEL_ID   — channel/chat ID, e.g. -1001234567890

Optional variables (have safe defaults):
  DATABASE_URL          — SQLite path (default: sqlite:///./moex_assistant.db)
  FRONTEND_URL          — CORS origin (default: http://localhost:3000)
  LOG_FORMAT            — "text" or "json" (default: text)
  LOG_LEVEL             — DEBUG/INFO/WARNING/ERROR (default: INFO)
  DRY_RUN               — if true, Telegram sends are skipped and logged only

Ignored / legacy (kept so existing .env doesn't break):
  GROQ_API_KEY, TELEGRAM_API_ID/HASH/SESSION_STRING — not used in MVP
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── required ──────────────────────────────────────────────────────────────
    telegram_bot_token:  str
    telegram_channel_id: str

    # ── optional ──────────────────────────────────────────────────────────────
    database_url:  str  = "sqlite:///./moex_assistant.db"
    frontend_url:  str  = "http://localhost:3000"
    secret_key:    str  = "changeme"

    # Logging
    log_format:    str  = "text"   # "text" | "json"
    log_level:     str  = "INFO"

    # DRY_RUN=true → pipeline runs fully but Telegram HTTP call is skipped.
    # Useful to verify ingestion/clustering/scoring without hitting the channel.
    dry_run:       bool = False

    # ── ops / monitoring ──────────────────────────────────────────────────────
    # If set, heartbeat and dead-source alerts are sent to this chat/channel.
    # Leave empty to disable all ops alerting.
    telegram_ops_chat_id: str = ""

    # ── AI analysis ───────────────────────────────────────────────────────────
    openrouter_api_key: str = ""

    # ── legacy / unused ───────────────────────────────────────────────────────
    groq_api_key:            str = ""
    telegram_api_id:         int = 0
    telegram_api_hash:       str = ""
    telegram_session_string: str = ""

    model_config = SettingsConfigDict(
        env_file=("../.env", ".env"),
        env_file_encoding="utf-8",
    )


settings = Settings()

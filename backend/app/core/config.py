"""
Application settings loaded from environment variables / .env file.

Required:
  TELEGRAM_BOT_TOKEN    — bot token from @BotFather
  TELEGRAM_CHANNEL_ID   — channel/chat ID, e.g. -1001234567890

Optional:
  DATABASE_URL          — SQLite path (default: sqlite:///./moex_assistant.db)
  LOG_FORMAT            — "text" or "json" (default: text)
  LOG_LEVEL             — DEBUG/INFO/WARNING/ERROR (default: INFO)
  DRY_RUN               — pipeline runs fully but Telegram sends are skipped
  TELEGRAM_OPS_CHAT_ID  — ops/heartbeat alerts destination

Telegram channel polling (Telethon):
  TELEGRAM_API_ID       — from my.telegram.org
  TELEGRAM_API_HASH     — from my.telegram.org
  TELEGRAM_SESSION_STRING — used once to bootstrap telegram_session.session file;
                            after the file exists this variable is no longer read
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── required ──────────────────────────────────────────────────────────────
    telegram_bot_token:  str
    telegram_channel_id: str

    # ── optional ──────────────────────────────────────────────────────────────
    database_url:  str  = "postgresql://postgres:postgres@localhost/moex_assistant"
    frontend_url:  str  = "http://localhost:3000"
    secret_key:    str  = "changeme"

    # Logging
    log_format:    str  = "text"   # "text" | "json"
    log_level:     str  = "INFO"

    # DRY_RUN=true → pipeline runs fully but Telegram HTTP call is skipped.
    # Useful to verify ingestion/clustering/scoring without hitting the channel.
    dry_run:       bool = False

    # "development" → prepends [DEV] to every channel message
    # "production"  → messages sent as-is
    environment:   str  = "production"

    # ── ops / monitoring ──────────────────────────────────────────────────────
    # If set, heartbeat and dead-source alerts are sent to this chat/channel.
    # Leave empty to disable all ops alerting.
    telegram_ops_chat_id: str = ""

    # ── AI analysis ───────────────────────────────────────────────────────────
    openrouter_api_key: str = ""

    # ── Telegram channel polling (Telethon) ───────────────────────────────────
    telegram_api_id:         int = 0
    telegram_api_hash:       str = ""
    # Bootstrap-only: used once to seed telegram_session.session, then ignored.
    telegram_session_string: str = ""

    # ── subscriber early access ──────────────────────────────────────────────
    # How many seconds DMs go out before the channel post is AI-enriched.
    # Set to 0 to disable the lead window.
    subscriber_lead_seconds: int = 60

    # ── legacy / unused ───────────────────────────────────────────────────────
    groq_api_key: str = ""

    model_config = SettingsConfigDict(
        env_file=("../.env", ".env"),
        env_file_encoding="utf-8",
    )


settings = Settings()

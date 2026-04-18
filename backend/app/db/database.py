import sqlite3
import logging
from pathlib import Path

from app.core.config import settings

logger = logging.getLogger(__name__)

_SCHEMA = Path(__file__).parent / "schema.sql"


def _db_path() -> str:
    # settings.database_url looks like "sqlite:///./moex_assistant.db"
    return settings.database_url.replace("sqlite:///", "")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(
        _db_path(),
        check_same_thread=False,
        timeout=10,          # wait up to 10s on SQLITE_BUSY before raising
    )
    conn.row_factory = sqlite3.Row
    # WAL: allows concurrent reads while a write is in progress
    conn.execute("PRAGMA journal_mode=WAL")
    # Enforce FK constraints (OFF by default in SQLite)
    conn.execute("PRAGMA foreign_keys=ON")
    # Synchronisation: NORMAL is safe with WAL and much faster than FULL
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    schema = _SCHEMA.read_text()
    conn = get_db()
    try:
        conn.executescript(schema)
        logger.info("Database initialized")
    finally:
        conn.close()

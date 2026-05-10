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
        _migrate(conn)
        logger.info("Database initialized")
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE event_clusters ADD COLUMN tickers TEXT DEFAULT NULL")
        conn.commit()
        logger.info("Migration applied: event_clusters.tickers")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE rss_sources ADD COLUMN source_type TEXT NOT NULL DEFAULT 'rss'")
        conn.commit()
        logger.info("Migration applied: rss_sources.source_type")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE rss_sources ADD COLUMN tg_last_msg_id INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        logger.info("Migration applied: rss_sources.tg_last_msg_id")
    except sqlite3.OperationalError:
        pass

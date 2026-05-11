import logging
from pathlib import Path

import psycopg2
import psycopg2.extras

from app.core.config import settings

logger = logging.getLogger(__name__)

_SCHEMA = Path(__file__).parent / "schema.sql"


class DBConnection:
    """
    Thin psycopg2 wrapper providing a sqlite3-like interface:
    - .execute(sql, params) → cursor (rows as RealDictRow, i.e. dict-like)
    - .executemany(sql, params_seq)
    - .commit() / .rollback() / .close()
    - with conn: → transaction (commit on success, rollback on exception)
    """

    def __init__(self, conn) -> None:
        self._conn = conn

    def execute(self, sql: str, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, params_seq) -> None:
        cur = self._conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, list(params_seq))

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        return False


def get_db() -> DBConnection:
    conn = psycopg2.connect(settings.database_url)
    conn.autocommit = False
    return DBConnection(conn)


def init_db() -> None:
    schema = _SCHEMA.read_text()
    conn = get_db()
    try:
        for stmt in _split_sql(schema):
            conn.execute(stmt)
        conn.commit()
        logger.info("Database initialized")
    finally:
        conn.close()


def _split_sql(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]

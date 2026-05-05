"""
Hot backup of the SQLite database.

Usage
─────
  python scripts/backup_db.py              # one-shot backup with defaults
  python scripts/backup_db.py --keep 20   # keep last 20 backups
  python scripts/backup_db.py --list      # list existing backups

Also importable for use by the scheduler job:
  from scripts.backup_db import backup
  dest = backup(db_path)

SQLite hot backup
─────────────────
sqlite3.Connection.backup() streams a consistent snapshot even while
the database is being written to. Safe to run alongside the live service.
WAL mode (already configured) makes this even safer — readers and the
backup do not block writers.
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Paths relative to this script's location (backend/)
_BACKEND = Path(__file__).parent.parent
_DEFAULT_DB   = _BACKEND / "moex_assistant.db"
_BACKUP_DIR   = _BACKEND / "backups"
_DEFAULT_KEEP = 10
_FILENAME_GLOB = "moex_assistant_????????_??????.db"


def backup(
    db_path: Path = _DEFAULT_DB,
    backup_dir: Path = _BACKUP_DIR,
    max_keep: int = _DEFAULT_KEEP,
) -> Path:
    """
    Create a timestamped backup of db_path in backup_dir.
    Rotates old backups, keeping the most recent max_keep files.
    Returns the path of the new backup file.
    """
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = backup_dir / f"moex_assistant_{ts}.db"

    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(dest))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    size_kb = dest.stat().st_size // 1024
    print(f"[backup] Created: {dest.name} ({size_kb} KB)")

    _rotate(backup_dir, max_keep)
    return dest


def list_backups(backup_dir: Path = _BACKUP_DIR) -> list[Path]:
    """Return existing backups sorted newest-first."""
    return sorted(backup_dir.glob(_FILENAME_GLOB), reverse=True)


# ── rotation ──────────────────────────────────────────────────────────────────

def _rotate(backup_dir: Path, max_keep: int) -> None:
    backups = sorted(backup_dir.glob(_FILENAME_GLOB))
    to_delete = backups[:-max_keep] if len(backups) > max_keep else []
    for old in to_delete:
        old.unlink()
        print(f"[backup] Rotated out: {old.name}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _main() -> None:
    parser = argparse.ArgumentParser(description="Backup moex_assistant.db")
    parser.add_argument("--db",   type=Path, default=_DEFAULT_DB,  help="Path to SQLite DB")
    parser.add_argument("--dir",  type=Path, default=_BACKUP_DIR,  help="Backup directory")
    parser.add_argument("--keep", type=int,  default=_DEFAULT_KEEP, help="Max backups to keep")
    parser.add_argument("--list", action="store_true",              help="List existing backups")
    args = parser.parse_args()

    if args.list:
        backups = list_backups(args.dir)
        if not backups:
            print("No backups found.")
        for p in backups:
            size_kb = p.stat().st_size // 1024
            print(f"  {p.name}  ({size_kb} KB)")
        return

    if not args.db.exists():
        print(f"[backup] ERROR: DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    backup(db_path=args.db, backup_dir=args.dir, max_keep=args.keep)


if __name__ == "__main__":
    _main()

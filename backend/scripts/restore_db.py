"""
Restore moex_assistant.db from a backup.

Usage
─────
  python scripts/restore_db.py              # interactive: pick from list
  python scripts/restore_db.py --latest     # restore most recent backup
  python scripts/restore_db.py --file backups/moex_assistant_20260505_120000.db

Safety
──────
Before overwriting, the current DB is renamed to moex_assistant.db.pre_restore
so you can undo the restore manually if needed.

IMPORTANT: Stop the service before restoring.
  1. Kill uvicorn (Ctrl+C or kill <pid>)
  2. Run this script
  3. Restart uvicorn
"""

import argparse
import sqlite3
import sys
from pathlib import Path

_BACKEND    = Path(__file__).parent.parent
_DEFAULT_DB = _BACKEND / "moex_assistant.db"
_BACKUP_DIR = _BACKEND / "backups"
_GLOB       = "moex_assistant_????????_??????.db"


def restore(backup_path: Path, db_path: Path = _DEFAULT_DB) -> None:
    """
    Restore db_path from backup_path.
    Renames the current DB to .pre_restore before overwriting.
    """
    if not backup_path.exists():
        print(f"[restore] ERROR: Backup not found: {backup_path}", file=sys.stderr)
        sys.exit(1)

    # Safety rename
    if db_path.exists():
        safety = db_path.with_suffix(".db.pre_restore")
        db_path.rename(safety)
        print(f"[restore] Current DB saved to: {safety.name}")

    src = sqlite3.connect(str(backup_path))
    dst = sqlite3.connect(str(db_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    size_kb = db_path.stat().st_size // 1024
    print(f"[restore] Restored: {backup_path.name} → {db_path.name} ({size_kb} KB)")
    print("[restore] Done. Start the service when ready.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _pick_interactive(backup_dir: Path) -> Path:
    backups = sorted(backup_dir.glob(_GLOB), reverse=True)
    if not backups:
        print("[restore] No backups found in:", backup_dir, file=sys.stderr)
        sys.exit(1)

    print("Available backups (newest first):\n")
    for i, p in enumerate(backups):
        size_kb = p.stat().st_size // 1024
        print(f"  [{i}] {p.name}  ({size_kb} KB)")

    print()
    raw = input("Enter number to restore (Ctrl+C to cancel): ").strip()
    try:
        idx = int(raw)
        return backups[idx]
    except (ValueError, IndexError):
        print("[restore] Invalid selection.", file=sys.stderr)
        sys.exit(1)


def _main() -> None:
    parser = argparse.ArgumentParser(description="Restore moex_assistant.db from backup")
    parser.add_argument("--db",     type=Path, default=_DEFAULT_DB, help="Target DB path")
    parser.add_argument("--dir",    type=Path, default=_BACKUP_DIR, help="Backup directory")
    parser.add_argument("--file",   type=Path, default=None,        help="Specific backup file to restore")
    parser.add_argument("--latest", action="store_true",            help="Restore most recent backup")
    args = parser.parse_args()

    if args.file:
        chosen = args.file
    elif args.latest:
        backups = sorted(args.dir.glob(_GLOB), reverse=True)
        if not backups:
            print("[restore] No backups found.", file=sys.stderr)
            sys.exit(1)
        chosen = backups[0]
        print(f"[restore] Latest backup: {chosen.name}")
    else:
        chosen = _pick_interactive(args.dir)

    confirm = input(f"\nRestore '{chosen.name}' to '{args.db.name}'? [y/N]: ").strip().lower()
    if confirm != "y":
        print("[restore] Cancelled.")
        sys.exit(0)

    restore(backup_path=chosen, db_path=args.db)


if __name__ == "__main__":
    _main()

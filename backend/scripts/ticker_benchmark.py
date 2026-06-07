#!/usr/bin/env python3
"""
Measure false-ticker rate on recent published events.
Run: docker compose exec backend python scripts/ticker_benchmark.py
"""
import sys
sys.path.insert(0, "/app")

from app.db.database import get_db
from app.pipeline.ticker_validator import validate_tickers

def main():
    db = get_db()
    try:
        rows = db.execute("""
            SELECT DISTINCT ON (ec.id) ec.id, ec.canonical_title, ec.tickers
            FROM event_clusters ec
            JOIN telegram_sends ts ON ts.cluster_id = ec.id
            WHERE ts.ok = 1 AND ec.tickers IS NOT NULL AND ec.tickers != ''
            ORDER BY ec.id, ts.sent_at DESC
            LIMIT 100
        """).fetchall()

        total = len(rows)
        mismatches = []
        for row in rows:
            validated = validate_tickers(row["tickers"], row["canonical_title"])
            if validated != row["tickers"]:
                mismatches.append({
                    "cluster_id": row["id"],
                    "original": row["tickers"],
                    "validated": validated,
                    "title": row["canonical_title"][:80],
                })

        print(f"Checked {total} published clusters")
        print(f"Mismatch rate: {len(mismatches)}/{total} ({100*len(mismatches)//max(total,1)}%)")
        if mismatches:
            print("\nTop mismatches:")
            for m in mismatches[:10]:
                print(f"  #{m['cluster_id']}: {m['original']!r} → {m['validated']!r}")
                print(f"    «{m['title']}»")
    finally:
        db.close()

if __name__ == "__main__":
    main()

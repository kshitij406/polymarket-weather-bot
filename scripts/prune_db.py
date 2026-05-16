"""
Prune the SQLite database to keep it under GitHub's 100 MB file size limit.

- Nullifies raw_json in forecast_fetches older than 7 days (data already
  consumed by probability_snapshots).
- Deduplicates market_snapshots older than 30 days (keeps one row per
  market/bucket/day).
- VACUUMs to release freed pages.

Safe to run at any time; no structured data is lost.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from bot.database import prune_db

stats = prune_db()
print(
    f"Pruned DB: {stats['forecast_fetches_nullified']} raw_json rows cleared, "
    f"{stats['market_snapshots_deleted']} duplicate market snapshots deleted."
)

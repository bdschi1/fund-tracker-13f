#!/usr/bin/env python3
"""Resolve unknown CUSIPs via OpenFIGI API (offline).

Run this periodically to resolve any new CUSIPs that aren't in the
bundled seed file, then re-export the seed:

    python scripts/resolve_new_cusips.py
    python scripts/export_cusip_seed.py

The app itself never calls the API — it uses the seed file only.
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from data.cache import DataCache
from data.cusip_resolver import resolve_cusips
from data.store import HoldingsStore

store = HoldingsStore()
cache = DataCache(store)

# Get all unique CUSIPs across all quarters
cusips = [
    r[0]
    for r in store._conn.execute(
        "SELECT DISTINCT cusip FROM holdings"
    ).fetchall()
]
print(f"Total unique CUSIPs: {len(cusips)}")

resolved = resolve_cusips(
    cusips=cusips,
    cache_read=cache.cusip_cache_read,
    cache_write=cache.cusip_cache_write,
    api_key=settings.openfigi_api_key,
)
print(f"Resolved: {len(resolved)}")
print("Run 'python scripts/export_cusip_seed.py' to update the seed file.")

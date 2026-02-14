#!/usr/bin/env python3
"""Export cusip_map from SQLite to config/cusip_tickers.json.

Run after resolving CUSIPs to update the bundled seed file:

    python scripts/export_cusip_seed.py

The seed file ships with the repo so new installs get instant
ticker resolution without hitting the OpenFIGI API.
"""
from pathlib import Path

from data.store import HoldingsStore

SEED_PATH = Path("config/cusip_tickers.json")

store = HoldingsStore()
count = store.export_cusip_seed(SEED_PATH)
print(f"Exported {count} CUSIP->ticker mappings to {SEED_PATH}")

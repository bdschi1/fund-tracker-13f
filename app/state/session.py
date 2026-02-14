"""Centralized Streamlit session state management.

Initializes all shared state on first run. All pages access data
through st.session_state rather than creating their own instances.
"""

from __future__ import annotations

import logging
from datetime import date

import streamlit as st
import yaml

from config.settings import settings
from core.models import FundInfo, Tier
from data.cache import DataCache
from data.store import HoldingsStore

logger = logging.getLogger(__name__)


def _load_watchlist() -> list[FundInfo]:
    """Load fund watchlist from YAML config."""
    watchlist_path = settings.watchlist_path
    if not watchlist_path.exists():
        logger.warning("Watchlist not found at %s", watchlist_path)
        return []
    with open(watchlist_path) as f:
        data = yaml.safe_load(f)
    return [FundInfo(**fund) for fund in data.get("funds", [])]


def init_session_state() -> None:
    """Initialize all session state on first run.

    Call this at the top of main.py before any page renders.
    """
    if "initialized" in st.session_state:
        return

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    # Fund watchlist
    st.session_state.watchlist = _load_watchlist()
    logger.info("Loaded %d funds from watchlist", len(st.session_state.watchlist))

    # Holdings store (SQLite)
    st.session_state.store = HoldingsStore(settings.db_path)

    # Data cache
    st.session_state.cache = DataCache(st.session_state.store)

    # Sync watchlist funds into database
    st.session_state.store.upsert_funds(st.session_state.watchlist)

    # Current UI selections — auto-detect most recent quarter if DB has data
    quarters = st.session_state.store.get_all_available_quarters()
    st.session_state.selected_quarter = quarters[0] if quarters else None
    st.session_state.selected_tiers = list(Tier)
    st.session_state.selected_fund_cik = None

    # Computed results cache (per quarter)
    st.session_state.fund_diffs = {}
    st.session_state.cross_signals = {}

    st.session_state.initialized = True

    if quarters:
        logger.info(
            "Auto-selected quarter %s (%d quarters available)",
            quarters[0],
            len(quarters),
        )


def get_watchlist() -> list[FundInfo]:
    return st.session_state.watchlist


def get_store() -> HoldingsStore:
    return st.session_state.store


def get_cache() -> DataCache:
    return st.session_state.cache


def get_filtered_funds() -> list[FundInfo]:
    """Get watchlist filtered by currently selected tiers."""
    return [
        f
        for f in st.session_state.watchlist
        if f.tier in st.session_state.selected_tiers
    ]


def get_available_quarters() -> list[date]:
    """Get all quarters that have data in the database."""
    return st.session_state.store.get_all_available_quarters()

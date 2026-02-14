"""Caching utilities that wrap the SQLite store.

Provides convenient cache read/write functions for CUSIP resolution,
sector data, and prices with staleness checking.
"""

from __future__ import annotations

import logging
from datetime import date

from config.settings import settings
from data.store import HoldingsStore

logger = logging.getLogger(__name__)


class DataCache:
    """Convenience wrapper around HoldingsStore for caching operations."""

    def __init__(self, store: HoldingsStore | None = None) -> None:
        self._store = store or HoldingsStore(settings.db_path)

    @property
    def store(self) -> HoldingsStore:
        return self._store

    # ------------------------------------------------------------------
    # CUSIP cache (permanent — CUSIPs don't change)
    # ------------------------------------------------------------------

    def cusip_cache_read(self, cusip: str) -> str | None:
        """Read ticker from CUSIP cache. Returns None if not cached."""
        return self._store.get_cusip_ticker(cusip)

    def cusip_cache_write(
        self,
        cusip: str,
        ticker: str | None,
        name: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Write CUSIP→ticker mapping to cache."""
        self._store.store_cusip_mapping(cusip, ticker, name, exchange)

    def get_cusip_tickers(self, cusips: list[str]) -> dict[str, str]:
        """Bulk CUSIP→ticker lookup from cache only."""
        return self._store.get_cusip_tickers_bulk(cusips)

    # ------------------------------------------------------------------
    # Sector cache (staleness: 30 days)
    # ------------------------------------------------------------------

    def get_sector_info(self, ticker: str) -> dict | None:
        """Get sector info if cached and not stale."""
        return self._store.get_sector_info(ticker)

    def get_sector_info_bulk(self, tickers: list[str]) -> dict[str, dict]:
        """Bulk sector info lookup."""
        return self._store.get_sector_info_bulk(tickers)

    def store_sector_info(
        self,
        ticker: str,
        sector: str | None,
        industry: str | None,
        market_cap: float | None = None,
        shares_outstanding: int | None = None,
        float_shares: int | None = None,
    ) -> None:
        """Store sector info in cache."""
        self._store.store_sector_info(
            ticker, sector, industry, market_cap,
            shares_outstanding, float_shares,
        )

    # ------------------------------------------------------------------
    # Price cache (staleness configurable)
    # ------------------------------------------------------------------

    def get_prices(
        self, tickers: list[str], price_date: date
    ) -> dict[str, float]:
        """Get cached prices for tickers on a date."""
        return self._store.get_prices_bulk(tickers, price_date)

    def store_prices(self, prices: dict[str, float], price_date: date) -> None:
        """Store prices in cache."""
        self._store.store_prices(prices, price_date)

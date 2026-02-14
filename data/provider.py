"""Market data provider abstraction.

Defines the interface that all data providers (Yahoo Finance, Bloomberg,
Interactive Brokers) must implement.  The app never calls yfinance directly —
it goes through whichever provider is active.

Two capabilities:
    1. Price performance (current price + period returns)
    2. Sector/fundamental info (sector, industry, market cap, float)
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataProvider(ABC):
    """Base class for market data providers."""

    @property
    def name(self) -> str:
        """Human-readable provider name."""
        return type(self).__name__

    # ------------------------------------------------------------------
    # Price performance
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_price_history(
        self,
        ticker: str,
        days: int = 400,
    ) -> list[dict]:
        """Fetch daily OHLCV bars for a single ticker.

        Returns a list of dicts with keys::

            {"date": date, "open": float, "high": float, "low": float,
             "close": float, "volume": float}

        Sorted oldest → newest.  Returns ``[]`` on failure.
        """
        ...

    # ------------------------------------------------------------------
    # Fundamental / sector data
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_ticker_info(self, ticker: str) -> dict:
        """Fetch fundamental data for a single ticker.

        Returns a dict with keys::

            {"sector": str|None, "industry": str|None,
             "market_cap": float|None, "shares_outstanding": int|None,
             "float_shares": int|None}

        Returns dict with ``None`` values on failure.
        """
        ...

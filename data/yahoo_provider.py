"""Yahoo Finance data provider (default).

Wraps yfinance to implement the MarketDataProvider interface.
Free, no API key required.  EOD data with ~18-hour delay.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import yfinance as yf

from data.provider import MarketDataProvider

logger = logging.getLogger(__name__)


class YahooProvider(MarketDataProvider):
    """Yahoo Finance provider — free, no account required."""

    @property
    def name(self) -> str:
        return "Yahoo Finance"

    # ------------------------------------------------------------------
    # Price history
    # ------------------------------------------------------------------

    def fetch_price_history(
        self,
        ticker: str,
        days: int = 400,
    ) -> list[dict]:
        """Fetch daily OHLCV bars from yfinance."""
        today = date.today()
        start = today - timedelta(days=days)

        try:
            hist = yf.Ticker(ticker).history(
                start=start.isoformat(),
                end=(today + timedelta(days=1)).isoformat(),
                auto_adjust=True,
            )
            if hist.empty:
                logger.debug("No price history for %s", ticker)
                return []

            rows: list[dict] = []
            for idx, row in hist.iterrows():
                rows.append({
                    "date": idx.date() if hasattr(idx, "date") else idx,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": float(row["Volume"]),
                })
            return rows

        except Exception:
            logger.debug(
                "Failed to fetch price history for %s",
                ticker, exc_info=True,
            )
            return []

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def fetch_ticker_info(self, ticker: str) -> dict:
        """Fetch sector/industry/float data from yfinance .info."""
        defaults: dict = {
            "sector": None,
            "industry": None,
            "market_cap": None,
            "shares_outstanding": None,
            "float_shares": None,
        }

        try:
            info = yf.Ticker(ticker).info
            return {
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "float_shares": info.get("floatShares"),
            }
        except Exception:
            logger.debug(
                "Failed to fetch ticker info for %s",
                ticker, exc_info=True,
            )
            return defaults

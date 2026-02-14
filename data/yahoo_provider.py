"""Yahoo Finance implementation of PriceProvider.

Uses yfinance for free price data. Batches requests for efficiency.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import yfinance as yf

from data.price_provider import PriceProvider

logger = logging.getLogger(__name__)

BATCH_SIZE = 50  # yfinance works well with ~50 tickers at a time


class YahooPriceProvider(PriceProvider):
    """Fetch prices via yfinance (free, no API key)."""

    @property
    def name(self) -> str:
        return "Yahoo Finance"

    def fetch_current_prices(self, tickers: list[str]) -> dict[str, float]:
        """Fetch latest closing price for each ticker."""
        result: dict[str, float] = {}
        for batch in _chunked(tickers, BATCH_SIZE):
            try:
                data = yf.download(
                    batch,
                    period="5d",
                    progress=False,
                    threads=True,
                )
                if data.empty:
                    continue

                close = data["Close"]
                if len(batch) == 1:
                    # Single ticker: close is a Series, not a DataFrame
                    last = close.dropna()
                    if not last.empty:
                        result[batch[0]] = float(last.iloc[-1])
                else:
                    for ticker in batch:
                        if ticker in close.columns:
                            col = close[ticker].dropna()
                            if not col.empty:
                                result[ticker] = float(col.iloc[-1])
            except Exception:
                logger.warning(
                    "Failed to fetch current prices for batch of %d",
                    len(batch),
                    exc_info=True,
                )
        return result

    def fetch_prices_on_date(
        self, tickers: list[str], target_date: date
    ) -> dict[str, float]:
        """Fetch closing price on or near a specific date.

        Looks at a 5-day window around the target date to handle weekends/holidays.
        """
        start = target_date - timedelta(days=5)
        end = target_date + timedelta(days=3)
        result: dict[str, float] = {}

        for batch in _chunked(tickers, BATCH_SIZE):
            try:
                data = yf.download(
                    batch,
                    start=str(start),
                    end=str(end),
                    progress=False,
                    threads=True,
                )
                if data.empty:
                    continue

                close = data["Close"]
                if len(batch) == 1:
                    col = close.dropna()
                    if not col.empty:
                        # Get the price closest to (but not after) target_date
                        before = col[col.index.date <= target_date]
                        if not before.empty:
                            result[batch[0]] = float(before.iloc[-1])
                        else:
                            result[batch[0]] = float(col.iloc[0])
                else:
                    for ticker in batch:
                        if ticker in close.columns:
                            col = close[ticker].dropna()
                            if not col.empty:
                                before = col[col.index.date <= target_date]
                                if not before.empty:
                                    result[ticker] = float(before.iloc[-1])
                                else:
                                    result[ticker] = float(col.iloc[0])
            except Exception:
                logger.warning(
                    "Failed to fetch prices on %s for batch of %d",
                    target_date,
                    len(batch),
                    exc_info=True,
                )
        return result


def _chunked(lst: list, size: int):
    """Yield successive chunks of size from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

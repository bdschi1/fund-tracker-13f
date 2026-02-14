"""Abstract price provider interface.

Follows the ABC provider pattern from ls-portfolio-lab.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date


class PriceProvider(ABC):
    """Abstract interface for fetching stock prices."""

    @property
    def name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def fetch_current_prices(self, tickers: list[str]) -> dict[str, float]:
        """Fetch latest closing price for each ticker.

        Returns: {ticker: price}
        """
        ...

    @abstractmethod
    def fetch_prices_on_date(
        self, tickers: list[str], target_date: date
    ) -> dict[str, float]:
        """Fetch closing price on or near a specific date.

        Returns: {ticker: price}
        """
        ...

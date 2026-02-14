"""Sector/industry classification provider.

Fetches sector, industry, market cap, and share count data via the active
MarketDataProvider.  Results are cached in SQLite with 30-day staleness.

Note: Sector enrichment is NOT called during interactive analysis (too slow).
It's available for offline scripts or batch enrichment.
"""

from __future__ import annotations

import logging

from data.cache import DataCache
from data.provider import MarketDataProvider

logger = logging.getLogger(__name__)


def enrich_sectors(
    tickers: list[str],
    cache: DataCache,
    staleness_days: int = 30,
    provider: MarketDataProvider | None = None,
) -> dict[str, dict]:
    """Fetch sector/industry info for tickers, using cache when possible.

    Args:
        tickers: List of ticker symbols.
        cache: DataCache instance for reading/writing.
        staleness_days: Re-fetch if cached data is older than this.
        provider: MarketDataProvider instance. Defaults to YahooProvider.

    Returns:
        {ticker: {sector, industry, market_cap, shares_outstanding, float_shares}}
    """
    result: dict[str, dict] = {}
    to_fetch: list[str] = []

    # Lazy default to Yahoo
    if provider is None:
        from data.yahoo_provider import YahooProvider
        provider = YahooProvider()

    # Check cache
    cached = cache.get_sector_info_bulk(tickers)
    for ticker in tickers:
        if ticker in cached:
            result[ticker] = cached[ticker]
        else:
            to_fetch.append(ticker)

    if not to_fetch:
        return result

    logger.info(
        "Fetching sector info for %d tickers via %s",
        len(to_fetch),
        provider.name,
    )

    for ticker in to_fetch:
        try:
            info = provider.fetch_ticker_info(ticker)
            data = {
                "ticker": ticker,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("market_cap"),
                "shares_outstanding": info.get("shares_outstanding"),
                "float_shares": info.get("float_shares"),
            }
            result[ticker] = data

            cache.store_sector_info(
                ticker=ticker,
                sector=data["sector"],
                industry=data["industry"],
                market_cap=data["market_cap"],
                shares_outstanding=data["shares_outstanding"],
                float_shares=data["float_shares"],
            )
        except Exception:
            logger.debug("Failed to fetch sector info for %s", ticker, exc_info=True)
            result[ticker] = {
                "ticker": ticker,
                "sector": None,
                "industry": None,
                "market_cap": None,
                "shares_outstanding": None,
                "float_shares": None,
            }

    return result

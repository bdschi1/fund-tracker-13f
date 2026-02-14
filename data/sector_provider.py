"""Sector/industry classification provider.

Fetches sector, industry, market cap, and share count data from yfinance.
Results are cached in SQLite with 30-day staleness.
"""

from __future__ import annotations

import logging

import yfinance as yf

from data.cache import DataCache

logger = logging.getLogger(__name__)


def enrich_sectors(
    tickers: list[str],
    cache: DataCache,
    staleness_days: int = 30,
) -> dict[str, dict]:
    """Fetch sector/industry info for tickers, using cache when possible.

    Args:
        tickers: List of ticker symbols.
        cache: DataCache instance for reading/writing.
        staleness_days: Re-fetch if cached data is older than this.

    Returns:
        {ticker: {sector, industry, market_cap, shares_outstanding, float_shares}}
    """
    result: dict[str, dict] = {}
    to_fetch: list[str] = []

    # Check cache
    cached = cache.get_sector_info_bulk(tickers)
    for ticker in tickers:
        if ticker in cached:
            result[ticker] = cached[ticker]
        else:
            to_fetch.append(ticker)

    if not to_fetch:
        return result

    logger.info("Fetching sector info for %d tickers from yfinance", len(to_fetch))

    for ticker in to_fetch:
        try:
            info = yf.Ticker(ticker).info
            sector = info.get("sector")
            industry = info.get("industry")
            market_cap = info.get("marketCap")
            shares_out = info.get("sharesOutstanding")
            float_shares = info.get("floatShares")

            data = {
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "market_cap": market_cap,
                "shares_outstanding": shares_out,
                "float_shares": float_shares,
            }
            result[ticker] = data

            cache.store_sector_info(
                ticker=ticker,
                sector=sector,
                industry=industry,
                market_cap=market_cap,
                shares_outstanding=shares_out,
                float_shares=float_shares,
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

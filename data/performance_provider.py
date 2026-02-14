"""Price performance provider.

Fetches current price and period returns (1w, 1m, YTD, 1yr) via the active
MarketDataProvider.  Results cached in SQLite with configurable staleness
(default 12 hours).  Only called for the handful of tickers that appear
in Top Findings.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from data.cache import DataCache
from data.provider import MarketDataProvider

logger = logging.getLogger(__name__)


def _compute_return(
    current: float, hist_close: float | None,
) -> float | None:
    """Compute percentage return from historical close to current price."""
    if hist_close is None or hist_close <= 0:
        return None
    return (current - hist_close) / hist_close


def fetch_price_performance(
    tickers: list[str],
    cache: DataCache,
    max_age_hours: int = 12,
    provider: MarketDataProvider | None = None,
) -> dict[str, dict]:
    """Fetch price performance for tickers, using cache when possible.

    For each ticker returns::

        {
            "current_price": float,
            "return_1w":  float | None,
            "return_1m":  float | None,
            "return_ytd": float | None,
            "return_1yr": float | None,
        }

    Args:
        tickers: Ticker symbols to look up.
        cache: DataCache instance for reading/writing.
        max_age_hours: Re-fetch if cached data is older than this.
        provider: MarketDataProvider instance. Defaults to YahooProvider.

    Returns:
        ``{ticker: performance_dict}`` for tickers with data.
    """
    if not tickers:
        return {}

    # Lazy default to Yahoo
    if provider is None:
        from data.yahoo_provider import YahooProvider
        provider = YahooProvider()

    result: dict[str, dict] = {}
    to_fetch: list[str] = []

    # Check cache first
    cached = cache.store.get_price_performance_bulk(
        tickers, max_age_hours=max_age_hours,
    )
    for ticker in tickers:
        if ticker in cached:
            result[ticker] = cached[ticker]
        else:
            to_fetch.append(ticker)

    if not to_fetch:
        return result

    logger.info(
        "Fetching price performance for %d tickers via %s",
        len(to_fetch),
        provider.name,
    )

    today = date.today()
    ytd_start = date(today.year, 1, 1)

    for ticker in to_fetch:
        try:
            rows = provider.fetch_price_history(ticker, days=400)
            if not rows:
                logger.debug("No price history for %s", ticker)
                continue

            # Build a date → close lookup
            closes: dict[date, float] = {r["date"]: r["close"] for r in rows}
            current_price = rows[-1]["close"]

            def _close_on_or_before(target: date) -> float | None:
                # Walk backwards up to 10 days to find a trading day
                for offset in range(11):
                    d = target - timedelta(days=offset)
                    if d in closes:
                        return closes[d]
                return None

            close_1w = _close_on_or_before(today - timedelta(weeks=1))
            close_1m = _close_on_or_before(today - timedelta(days=30))
            close_ytd = _close_on_or_before(ytd_start - timedelta(days=1))
            close_1yr = _close_on_or_before(today - timedelta(days=365))

            perf = {
                "ticker": ticker,
                "current_price": current_price,
                "return_1w": _compute_return(current_price, close_1w),
                "return_1m": _compute_return(current_price, close_1m),
                "return_ytd": _compute_return(current_price, close_ytd),
                "return_1yr": _compute_return(current_price, close_1yr),
            }
            result[ticker] = perf

            cache.store.store_price_performance(
                ticker=ticker,
                current_price=perf["current_price"],
                return_1w=perf["return_1w"],
                return_1m=perf["return_1m"],
                return_ytd=perf["return_ytd"],
                return_1yr=perf["return_1yr"],
            )

        except Exception:
            logger.debug(
                "Failed to fetch price performance for %s",
                ticker, exc_info=True,
            )

    return result


def format_price_tag(perf: dict) -> str:
    """Format a compact inline price performance string.

    Example output::

        "$255.78 · 1w +2.3% · 1m −1.5% · YTD +12.4% · 1yr +28.1%"
    """
    parts: list[str] = []

    price = perf.get("current_price")
    if price is not None:
        parts.append(f"${price:,.2f}")

    for label, key in [
        ("1w", "return_1w"),
        ("1m", "return_1m"),
        ("YTD", "return_ytd"),
        ("1yr", "return_1yr"),
    ]:
        val = perf.get(key)
        if val is not None:
            pct = val * 100
            sign = "+" if pct >= 0 else ""
            parts.append(f"{label} {sign}{pct:.1f}%")

    return " · ".join(parts)

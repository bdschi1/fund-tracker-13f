"""CUSIP-to-ticker resolution via OpenFIGI API.

OpenFIGI is free (250 req/day without API key, 25k with key).
Each request can map up to 100 CUSIPs. Results are cached permanently
in SQLite since CUSIPs don't change.
"""

from __future__ import annotations

import logging
from typing import Callable

import httpx

logger = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE = 100  # Max items per OpenFIGI request


def resolve_cusips(
    cusips: list[str],
    cache_read: Callable[[str], str | None],
    cache_write: Callable[[str, str | None, str | None, str | None], None],
    api_key: str | None = None,
) -> dict[str, str]:
    """Resolve a list of CUSIPs to tickers.

    Uses cache first, then falls back to OpenFIGI API for unknowns.

    Args:
        cusips: List of 9-character CUSIPs.
        cache_read: Function(cusip) -> ticker or None.
        cache_write: Function(cusip, ticker, name, exchange) -> None.
        api_key: Optional OpenFIGI API key for higher rate limits.

    Returns:
        {cusip: ticker} mapping. CUSIPs that couldn't be resolved are omitted.
    """
    result: dict[str, str | None] = {}
    unknown: list[str] = []

    # Deduplicate
    unique_cusips = list(set(cusips))

    # Check cache first
    for cusip in unique_cusips:
        cached = cache_read(cusip)
        if cached is not None:
            result[cusip] = cached
        else:
            unknown.append(cusip)

    if not unknown:
        return {k: v for k, v in result.items() if v}

    logger.info("Resolving %d unknown CUSIPs via OpenFIGI", len(unknown))

    # Resolve unknowns via OpenFIGI in batches
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    for batch in _chunked(unknown, BATCH_SIZE):
        payload = [
            {"idType": "ID_CUSIP", "idValue": cusip}
            for cusip in batch
        ]
        try:
            resp = httpx.post(
                OPENFIGI_URL,
                json=payload,
                headers=headers,
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()

            for i, item in enumerate(data):
                cusip = batch[i]
                if "data" in item and item["data"]:
                    # Pick the best match: prefer US equity
                    best = _pick_best_match(item["data"])
                    ticker = best.get("ticker", "")
                    name = best.get("name", "")
                    exchange = best.get("exchCode", "")
                    result[cusip] = ticker if ticker else None
                    cache_write(cusip, ticker, name, exchange)
                    logger.debug("Resolved %s -> %s", cusip, ticker)
                else:
                    result[cusip] = None
                    # Cache the miss too so we don't re-query
                    cache_write(cusip, None, None, None)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning(
                    "OpenFIGI rate limit hit. %d CUSIPs unresolved.",
                    len(batch),
                )
                break
            logger.warning("OpenFIGI HTTP error: %s", e)
        except Exception:
            logger.warning(
                "OpenFIGI batch failed for %d CUSIPs", len(batch), exc_info=True
            )

    resolved = {k: v for k, v in result.items() if v}
    logger.info(
        "CUSIP resolution complete: %d resolved, %d unresolved",
        len(resolved),
        len(unique_cusips) - len(resolved),
    )
    return resolved


def _pick_best_match(matches: list[dict]) -> dict:
    """Pick the best OpenFIGI match, preferring US equity listings."""
    # Prefer US exchanges
    us_exchanges = {"US", "UN", "UA", "UW", "UQ", "UR"}

    for match in matches:
        if match.get("exchCode", "") in us_exchanges:
            return match

    # Prefer anything with a ticker
    for match in matches:
        if match.get("ticker"):
            return match

    # Fallback to first match
    return matches[0]


def _chunked(lst: list, size: int):
    """Yield successive chunks of size from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

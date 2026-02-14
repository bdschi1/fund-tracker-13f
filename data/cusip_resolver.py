"""CUSIP-to-ticker resolution via OpenFIGI API.

OpenFIGI free tier: 10 items/request, 5 req/min, 250 req/day.
With API key: 100 items/request, 25 req/6s, 25k req/day.
Results are cached permanently in SQLite since CUSIPs don't change.
"""

from __future__ import annotations

import logging
import time
from typing import Callable

import httpx

logger = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# OpenFIGI limits: without key = 10/batch, 5 req/min
# With key = 100/batch, 25 req/6s
BATCH_SIZE_FREE = 10
BATCH_SIZE_KEYED = 100
DELAY_FREE = 12.5  # seconds between batches (5 req/min = 12s)
DELAY_KEYED = 0.25  # seconds between batches (25 req/6s)


def resolve_cusips(
    cusips: list[str],
    cache_read: Callable[[str], str | None],
    cache_write: Callable[
        [str, str | None, str | None, str | None], None
    ],
    api_key: str | None = None,
) -> dict[str, str]:
    """Resolve a list of CUSIPs to tickers.

    Uses cache first, then falls back to OpenFIGI API for unknowns.

    Args:
        cusips: List of 9-character CUSIPs.
        cache_read: Function(cusip) -> ticker or None.
        cache_write: Function(cusip, ticker, name, exchange).
        api_key: Optional OpenFIGI API key for higher limits.

    Returns:
        {cusip: ticker} mapping. Unresolved CUSIPs are omitted.
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

    logger.info(
        "Resolving %d unknown CUSIPs via OpenFIGI", len(unknown),
    )

    # Select batch size and delay based on whether we have a key
    batch_size = BATCH_SIZE_KEYED if api_key else BATCH_SIZE_FREE
    delay = DELAY_KEYED if api_key else DELAY_FREE

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    n_batches = (len(unknown) + batch_size - 1) // batch_size
    for batch_idx, batch in enumerate(
        _chunked(unknown, batch_size),
    ):
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
                    best = _pick_best_match(item["data"])
                    ticker = best.get("ticker", "")
                    name = best.get("name", "")
                    exchange = best.get("exchCode", "")
                    result[cusip] = ticker if ticker else None
                    cache_write(cusip, ticker, name, exchange)
                    logger.debug(
                        "Resolved %s -> %s", cusip, ticker,
                    )
                else:
                    result[cusip] = None
                    # Cache the miss too so we don't re-query
                    cache_write(cusip, None, None, None)

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 429:
                logger.warning(
                    "OpenFIGI rate limit hit after %d batches. "
                    "%d CUSIPs unresolved.",
                    batch_idx + 1,
                    len(unknown) - batch_idx * batch_size,
                )
                break
            if code == 413:
                logger.warning(
                    "Payload too large (%d items). "
                    "Retrying with smaller batch.",
                    len(batch),
                )
                # Retry this batch with half the size
                for mini in _chunked(batch, max(batch_size // 2, 5)):
                    _resolve_mini_batch(
                        mini, headers, result, cache_write,
                    )
                    time.sleep(delay)
                continue
            logger.warning("OpenFIGI HTTP error: %s", e)
        except Exception:
            logger.warning(
                "OpenFIGI batch failed for %d CUSIPs",
                len(batch), exc_info=True,
            )

        # Rate-limit delay between batches
        if batch_idx < n_batches - 1:
            time.sleep(delay)

    resolved = {k: v for k, v in result.items() if v}
    logger.info(
        "CUSIP resolution complete: %d resolved, %d unresolved",
        len(resolved),
        len(unique_cusips) - len(resolved),
    )
    return resolved


def _resolve_mini_batch(
    batch: list[str],
    headers: dict[str, str],
    result: dict[str, str | None],
    cache_write: Callable,
) -> None:
    """Resolve a small batch — helper for 413 retry."""
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
                best = _pick_best_match(item["data"])
                ticker = best.get("ticker", "")
                name = best.get("name", "")
                exchange = best.get("exchCode", "")
                result[cusip] = ticker if ticker else None
                cache_write(cusip, ticker, name, exchange)
            else:
                result[cusip] = None
                cache_write(cusip, None, None, None)
    except Exception:
        logger.debug(
            "Mini-batch failed for %d CUSIPs",
            len(batch), exc_info=True,
        )


def _pick_best_match(matches: list[dict]) -> dict:
    """Pick the best OpenFIGI match, preferring US equity."""
    us_exchanges = {"US", "UN", "UA", "UW", "UQ", "UR"}

    for match in matches:
        if match.get("exchCode", "") in us_exchanges:
            return match

    for match in matches:
        if match.get("ticker"):
            return match

    return matches[0]


def _chunked(lst: list, size: int):
    """Yield successive chunks of size from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

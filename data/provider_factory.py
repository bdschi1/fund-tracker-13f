"""Data provider factory with dynamic discovery.

Auto-detects which providers are available based on installed packages.
Defaults to Yahoo Finance.  Falls back gracefully on connection errors.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from data.provider import MarketDataProvider

logger = logging.getLogger(__name__)

# (display_name, module_path, class_name, availability_function_path | None)
_PROVIDER_REGISTRY: list[tuple[str, str, str, str | None]] = [
    ("Yahoo Finance", "data.yahoo_provider", "YahooProvider", None),
    (
        "Interactive Brokers",
        "data.ib_provider",
        "IBProvider",
        "data.ib_provider.is_available",
    ),
]

# Provider instance cache — reuse across calls
_provider_cache: dict[str, MarketDataProvider] = {}


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def available_providers() -> list[str]:
    """Return names of providers whose dependencies are installed.

    Yahoo Finance is always available (yfinance is a core dep).
    """
    available: list[str] = []
    for display_name, _mod, _cls, avail_func_path in _PROVIDER_REGISTRY:
        if avail_func_path is None:
            # Always available (core dependency)
            available.append(display_name)
            continue
        try:
            mod_path, func_name = avail_func_path.rsplit(".", 1)
            mod = importlib.import_module(mod_path)
            is_avail = getattr(mod, func_name)
            if is_avail():
                available.append(display_name)
        except Exception:
            pass
    return available


def get_provider(name: str = "Yahoo Finance", **kwargs: Any) -> MarketDataProvider:
    """Get provider instance by name.  Returns cached instance if no kwargs."""
    if name in _provider_cache and not kwargs:
        return _provider_cache[name]

    for display_name, module_path, class_name, _ in _PROVIDER_REGISTRY:
        if display_name == name:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            instance = cls(**kwargs)
            _provider_cache[name] = instance
            logger.info("Data provider initialized: %s", name)
            return instance

    avail = available_providers()
    raise ValueError(f"Unknown provider '{name}'. Available: {avail}")


def get_provider_safe(name: str = "Yahoo Finance", **kwargs: Any) -> MarketDataProvider:
    """Get provider, falling back to Yahoo Finance on any error."""
    try:
        return get_provider(name, **kwargs)
    except Exception as exc:
        logger.warning(
            "Failed to initialize %s provider (%s), falling back to Yahoo Finance",
            name,
            exc,
        )
        return get_provider("Yahoo Finance")


# ------------------------------------------------------------------
# Descriptions for UI
# ------------------------------------------------------------------

PROVIDER_DESCRIPTIONS: dict[str, str] = {
    "Yahoo Finance": "Free, no account required. EOD data, ~18hr delay.",
    "Interactive Brokers": "Brokerage account required. Real-time quotes via TWS/Gateway.",
}

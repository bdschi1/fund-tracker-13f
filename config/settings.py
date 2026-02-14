"""Application settings with Pydantic validation.

Supports .env file, environment variable overrides, and Streamlit Cloud
secrets.  All env vars prefixed with FT13F_ (e.g., FT13F_EDGAR_USER_AGENT).

On Streamlit Cloud, set secrets in the dashboard under the same FT13F_*
names — they are injected into os.environ before Settings() loads.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _inject_streamlit_secrets() -> None:
    """Copy Streamlit Cloud secrets into os.environ so pydantic-settings
    picks them up via its env_prefix mechanism.  No-op when running locally
    or when streamlit is not importable.
    """
    try:
        from streamlit import secrets  # type: ignore[attr-defined]

        for key, value in secrets.items():
            if isinstance(value, str) and key.upper().startswith("FT13F_"):
                os.environ.setdefault(key.upper(), value)
    except Exception:
        pass  # Not on Streamlit Cloud, or no secrets configured


_inject_streamlit_secrets()


class Settings(BaseSettings):
    """Global application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="FT13F_",
        extra="ignore",
    )

    # --- SEC EDGAR ---
    edgar_user_agent: str = Field(
        default="FundTracker13F contact@example.com",
        description="Required User-Agent header for SEC EDGAR requests",
    )
    edgar_rate_limit_rps: float = Field(
        default=5.0,
        description="Max requests per second to SEC EDGAR (limit is 10, use 5 for safety)",
    )

    # --- OpenFIGI ---
    openfigi_api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for higher rate limits on OpenFIGI",
    )

    # --- Database ---
    db_path: Path = Field(
        default=Path("data_cache/fund_tracker.db"),
        description="Path to SQLite database",
    )

    # --- Cache Staleness ---
    holdings_staleness_days: int = Field(
        default=1,
        description="Re-fetch holdings if older than this many days",
    )
    price_staleness_hours: int = Field(
        default=18,
        description="Re-fetch prices if older than this many hours",
    )
    sector_staleness_days: int = Field(
        default=30,
        description="Re-fetch sector data if older than this many days",
    )

    # --- Analysis Defaults ---
    min_funds_for_crowd: int = Field(
        default=3,
        description="Minimum funds buying for a crowded trade signal",
    )
    min_funds_for_consensus: int = Field(
        default=3,
        description="Minimum funds initiating for consensus signal",
    )
    options_aum_threshold: float = Field(
        default=0.005,
        description="Options position weight threshold for inclusion (0.5%)",
    )
    multistrat_top_positions: int = Field(
        default=50,
        description="Show top-N positions for multi-strat (Tier A) funds",
    )
    multistrat_top_changes: int = Field(
        default=20,
        description="Show top-N changes for multi-strat (Tier A) funds",
    )

    # --- Interactive Brokers (optional) ---
    ib_host: str = Field(
        default="127.0.0.1",
        description="IB TWS/Gateway hostname",
    )
    ib_port: int = Field(
        default=7497,
        description="IB TWS/Gateway port (7497=TWS paper, 7496=TWS live, 4002=Gateway paper)",
    )
    ib_client_id: int = Field(
        default=10,
        description="IB client ID for this connection",
    )

    # --- Application ---
    log_level: str = Field(default="INFO", description="Logging level")
    watchlist_path: Path = Field(
        default=Path("config/watchlist.yaml"),
        description="Path to fund watchlist YAML",
    )
    themes_path: Path = Field(
        default=Path("config/themes.yaml"),
        description="Path to thematic groupings YAML",
    )


# Singleton instance
settings = Settings()

"""Shared test fixtures for fund-tracker-13f."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import pytest

from core.models import (
    FundHoldings,
    FundInfo,
    Holding,
    Tier,
)
from data.store import HoldingsStore


@pytest.fixture
def sample_fund() -> FundInfo:
    """A sample stock-picker fund."""
    return FundInfo(name="Test Capital", cik="1234567", tier=Tier.B)


@pytest.fixture
def sample_fund_multistrat() -> FundInfo:
    """A sample multi-strat fund."""
    return FundInfo(name="Test Multi-Strat", cik="7654321", tier=Tier.A)


@pytest.fixture
def sample_holdings() -> list[Holding]:
    """Sample equity holdings for testing."""
    return [
        Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
            ticker="AAPL",
            sector="Technology",
        ),
        Holding(
            cusip="594918104",
            issuer_name="MICROSOFT CORP",
            title_of_class="COM",
            value_thousands=400_000,
            shares_or_prn_amt=1_000_000,
            ticker="MSFT",
            sector="Technology",
        ),
        Holding(
            cusip="02079K305",
            issuer_name="ALPHABET INC",
            title_of_class="CL A",
            value_thousands=300_000,
            shares_or_prn_amt=1_500_000,
            ticker="GOOGL",
            sector="Communication Services",
        ),
        Holding(
            cusip="67066G104",
            issuer_name="NVIDIA CORP",
            title_of_class="COM",
            value_thousands=200_000,
            shares_or_prn_amt=1_000_000,
            ticker="NVDA",
            sector="Technology",
        ),
        Holding(
            cusip="88160R101",
            issuer_name="TESLA INC",
            title_of_class="COM",
            value_thousands=100_000,
            shares_or_prn_amt=500_000,
            ticker="TSLA",
            sector="Consumer Discretionary",
        ),
    ]


@pytest.fixture
def sample_option_holding() -> Holding:
    """A sample PUT option holding."""
    return Holding(
        cusip="037833100",
        issuer_name="APPLE INC",
        title_of_class="PUT",
        value_thousands=50_000,
        shares_or_prn_amt=500_000,
        put_call="PUT",
        ticker="AAPL",
    )


@pytest.fixture
def sample_fund_holdings(sample_fund, sample_holdings) -> FundHoldings:
    """Sample FundHoldings for current quarter."""
    return FundHoldings(
        fund=sample_fund,
        quarter_end=date(2025, 9, 30),
        filing_date=date(2025, 11, 14),
        report_date=date(2025, 9, 30),
        holdings=sample_holdings,
    )


@pytest.fixture
def prior_holdings() -> list[Holding]:
    """Holdings from prior quarter (for diff testing)."""
    return [
        # AAPL: was 3M shares, now 2.5M = trimmed
        Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=600_000,
            shares_or_prn_amt=3_000_000,
            ticker="AAPL",
            sector="Technology",
        ),
        # MSFT: was 500K shares, now 1M = doubled (significant add)
        Holding(
            cusip="594918104",
            issuer_name="MICROSOFT CORP",
            title_of_class="COM",
            value_thousands=200_000,
            shares_or_prn_amt=500_000,
            ticker="MSFT",
            sector="Technology",
        ),
        # GOOGL: same shares = unchanged
        Holding(
            cusip="02079K305",
            issuer_name="ALPHABET INC",
            title_of_class="CL A",
            value_thousands=280_000,
            shares_or_prn_amt=1_500_000,
            ticker="GOOGL",
            sector="Communication Services",
        ),
        # AMZN: was held, now not = exited
        Holding(
            cusip="023135106",
            issuer_name="AMAZON COM INC",
            title_of_class="COM",
            value_thousands=150_000,
            shares_or_prn_amt=800_000,
            ticker="AMZN",
            sector="Consumer Discretionary",
        ),
        # No NVDA or TSLA in prior = they are NEW positions
    ]


@pytest.fixture
def prior_fund_holdings(sample_fund, prior_holdings) -> FundHoldings:
    """Prior quarter FundHoldings for diff testing."""
    return FundHoldings(
        fund=sample_fund,
        quarter_end=date(2025, 6, 30),
        filing_date=date(2025, 8, 14),
        report_date=date(2025, 6, 30),
        holdings=prior_holdings,
    )


@pytest.fixture
def tmp_db() -> HoldingsStore:
    """In-memory (temporary file) SQLite store for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = HoldingsStore(db_path)
        yield store
        store.close()

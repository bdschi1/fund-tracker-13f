"""Tests for core domain models."""

from __future__ import annotations

from datetime import date

from core.models import (
    FundHoldings,
    FundInfo,
    Holding,
    PositionChangeType,
    PositionDiff,
    Tier,
)


class TestHolding:
    def test_value_dollars(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
        )
        assert h.value_dollars == 500_000_000

    def test_is_equity(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
        )
        assert h.is_equity is True
        assert h.is_option is False

    def test_is_option_put(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="PUT",
            value_thousands=50_000,
            shares_or_prn_amt=500_000,
            put_call="PUT",
        )
        assert h.is_option is True
        assert h.is_equity is False

    def test_issuer_cusip_prefix(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
        )
        assert h.issuer_cusip_prefix == "037833"

    def test_display_label_equity(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
            ticker="AAPL",
        )
        assert h.display_label == "AAPL"

    def test_display_label_option(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="PUT",
            value_thousands=50_000,
            shares_or_prn_amt=500_000,
            put_call="PUT",
            ticker="AAPL",
        )
        assert h.display_label == "AAPL [PUT]"

    def test_display_label_no_ticker(self):
        h = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
        )
        assert h.display_label == "APPLE"


class TestFundInfo:
    def test_cik_padded(self):
        f = FundInfo(name="Test Fund", cik="1234567", tier=Tier.B)
        assert f.cik_padded == "0001234567"

    def test_cik_already_padded(self):
        f = FundInfo(name="Test Fund", cik="0001234567", tier=Tier.B)
        assert f.cik_padded == "0001234567"


class TestFundHoldings:
    def test_auto_compute_total(self, sample_fund, sample_holdings):
        fh = FundHoldings(
            fund=sample_fund,
            quarter_end=date(2025, 9, 30),
            filing_date=date(2025, 11, 14),
            report_date=date(2025, 9, 30),
            holdings=sample_holdings,
        )
        expected = sum(h.value_thousands for h in sample_holdings)
        assert fh.total_value_thousands == expected

    def test_filing_lag_days(self, sample_fund):
        fh = FundHoldings(
            fund=sample_fund,
            quarter_end=date(2025, 9, 30),
            filing_date=date(2025, 11, 14),
            report_date=date(2025, 9, 30),
            holdings=[],
        )
        assert fh.filing_lag_days == 45

    def test_portfolio_weight(self, sample_fund_holdings, sample_holdings):
        aapl = sample_holdings[0]
        weight = sample_fund_holdings.portfolio_weight(aapl)
        expected = aapl.value_thousands / sample_fund_holdings.total_value_thousands
        assert abs(weight - expected) < 0.001

    def test_get_holding_by_cusip(self, sample_fund_holdings):
        h = sample_fund_holdings.get_holding_by_cusip("037833100")
        assert h is not None
        assert h.issuer_name == "APPLE INC"

    def test_get_holding_by_cusip_missing(self, sample_fund_holdings):
        h = sample_fund_holdings.get_holding_by_cusip("XXXXXXXXX")
        assert h is None

    def test_equity_vs_option_holdings(self, sample_fund, sample_holdings, sample_option_holding):
        fh = FundHoldings(
            fund=sample_fund,
            quarter_end=date(2025, 9, 30),
            filing_date=date(2025, 11, 14),
            report_date=date(2025, 9, 30),
            holdings=sample_holdings + [sample_option_holding],
        )
        assert len(fh.equity_holdings) == 5
        assert len(fh.option_holdings) == 1


class TestPositionDiff:
    def test_significant_add(self):
        d = PositionDiff(
            cusip="TEST",
            issuer_name="TEST CO",
            change_type=PositionChangeType.ADDED,
            shares_change_pct=1.0,  # Doubled
        )
        assert d.is_significant_add is True

    def test_not_significant_add(self):
        d = PositionDiff(
            cusip="TEST",
            issuer_name="TEST CO",
            change_type=PositionChangeType.ADDED,
            shares_change_pct=0.1,  # Only 10% add
        )
        assert d.is_significant_add is False

    def test_significant_trim(self):
        d = PositionDiff(
            cusip="TEST",
            issuer_name="TEST CO",
            change_type=PositionChangeType.TRIMMED,
            shares_change_pct=-0.7,  # 70% cut
        )
        assert d.is_significant_trim is True

    def test_display_label_with_option(self):
        d = PositionDiff(
            cusip="TEST",
            issuer_name="TEST CO",
            ticker="TEST",
            put_call="PUT",
            change_type=PositionChangeType.NEW,
        )
        assert d.display_label == "TEST [PUT]"

"""Tests for the QoQ diff engine."""

from __future__ import annotations

from core.diff_engine import compute_fund_diff
from core.models import PositionChangeType


class TestDiffEngine:
    def test_detects_new_positions(self, sample_fund_holdings, prior_fund_holdings):
        """NVDA and TSLA are in current but not prior — should be NEW."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        new_cusips = {p.cusip for p in diff.new_positions}
        # NVDA (67066G104) and TSLA (88160R101) should be new
        assert "67066G104" in new_cusips
        assert "88160R101" in new_cusips

    def test_detects_exited_positions(self, sample_fund_holdings, prior_fund_holdings):
        """AMZN is in prior but not current — should be EXITED."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        exited_cusips = {p.cusip for p in diff.exited_positions}
        assert "023135106" in exited_cusips  # AMZN

    def test_detects_added_positions(self, sample_fund_holdings, prior_fund_holdings):
        """MSFT went from 500K to 1M shares — should be ADDED with 100% change."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        msft_adds = [p for p in diff.added_positions if p.cusip == "594918104"]
        assert len(msft_adds) == 1
        assert msft_adds[0].change_type == PositionChangeType.ADDED
        assert msft_adds[0].shares_change_pct == 1.0  # Doubled
        assert msft_adds[0].is_significant_add is True

    def test_detects_trimmed_positions(self, sample_fund_holdings, prior_fund_holdings):
        """AAPL went from 3M to 2.5M shares — should be TRIMMED."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        aapl_trims = [p for p in diff.trimmed_positions if p.cusip == "037833100"]
        assert len(aapl_trims) == 1
        assert aapl_trims[0].change_type == PositionChangeType.TRIMMED
        # 2.5M - 3M = -500K, -500K/3M = -16.7%
        assert abs(aapl_trims[0].shares_change_pct - (-1 / 6)) < 0.01

    def test_detects_unchanged_positions(self, sample_fund_holdings, prior_fund_holdings):
        """GOOGL has same shares in both quarters — should be UNCHANGED."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        googl = [p for p in diff.unchanged_positions if p.cusip == "02079K305"]
        assert len(googl) == 1
        assert googl[0].change_type == PositionChangeType.UNCHANGED

    def test_aum_change(self, sample_fund_holdings, prior_fund_holdings):
        """AUM change should be computed correctly."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        assert diff.current_aum_thousands == sample_fund_holdings.total_value_thousands
        assert diff.prior_aum_thousands == prior_fund_holdings.total_value_thousands

    def test_concentration_metrics(self, sample_fund_holdings, prior_fund_holdings):
        """HHI and top-10 should be computed."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        assert diff.current_hhi > 0
        assert diff.prior_hhi > 0
        assert diff.current_top10_weight > 0

    def test_adds_sorted_by_pct(self, sample_fund_holdings, prior_fund_holdings):
        """Added positions should be sorted by shares_change_pct descending."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)

        if len(diff.added_positions) > 1:
            for i in range(len(diff.added_positions) - 1):
                assert (
                    diff.added_positions[i].shares_change_pct
                    >= diff.added_positions[i + 1].shares_change_pct
                )

    def test_filing_lag(self, sample_fund_holdings, prior_fund_holdings):
        """Filing lag should be computed."""
        diff = compute_fund_diff(sample_fund_holdings, prior_fund_holdings)
        assert diff.filing_lag_days == 45  # Nov 14 - Sep 30

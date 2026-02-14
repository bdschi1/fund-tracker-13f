"""Tests for the smart options filter."""

from __future__ import annotations

from core.models import Holding, PositionChangeType
from core.options_filter import classify_option


class TestOptionsFilter:
    def test_equity_always_included(self, sample_holdings):
        """Equity positions should always return INCLUDE."""
        result = classify_option(
            holding=sample_holdings[0],  # AAPL equity
            all_holdings=sample_holdings,
            total_aum_thousands=1_500_000,
            change_type=PositionChangeType.UNCHANGED,
        )
        assert result == "INCLUDE"

    def test_new_put_without_equity_included(self):
        """New PUT on a stock the fund doesn't own = directional bet = INCLUDE."""
        put_holding = Holding(
            cusip="NEWPUT100",
            issuer_name="SHORT TARGET INC",
            title_of_class="PUT",
            value_thousands=20_000,
            shares_or_prn_amt=200_000,
            put_call="PUT",
        )
        # Fund holds no equity in NEWPUT (different CUSIP prefix)
        other_holdings = [
            Holding(
                cusip="037833100",
                issuer_name="APPLE INC",
                title_of_class="COM",
                value_thousands=500_000,
                shares_or_prn_amt=2_500_000,
            )
        ]
        result = classify_option(
            holding=put_holding,
            all_holdings=other_holdings + [put_holding],
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.NEW,
        )
        assert result == "INCLUDE"

    def test_large_option_by_weight_included(self):
        """Option > 0.5% of AUM should be INCLUDE."""
        option = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="CALL",
            value_thousands=10_000,  # 1% of 1M AUM
            shares_or_prn_amt=100_000,
            put_call="CALL",
        )
        result = classify_option(
            holding=option,
            all_holdings=[option],
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.NEW,
            aum_threshold=0.005,
        )
        assert result == "INCLUDE"

    def test_small_hedge_excluded(self):
        """Small option alongside large equity = routine hedge = EXCLUDE."""
        equity = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="COM",
            value_thousands=500_000,
            shares_or_prn_amt=2_500_000,
        )
        put_hedge = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="PUT",
            value_thousands=5_000,  # 1% of equity value = tiny hedge
            shares_or_prn_amt=50_000,
            put_call="PUT",
        )
        result = classify_option(
            holding=put_hedge,
            all_holdings=[equity, put_hedge],
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.UNCHANGED,
        )
        assert result == "EXCLUDE"

    def test_market_making_noise_excluded(self):
        """Fund with 20+ small option positions = market-making noise."""
        # Create 25 small option positions
        options = [
            Holding(
                cusip=f"TEST{i:05d}0",
                issuer_name=f"COMPANY {i}",
                title_of_class="CALL",
                value_thousands=500,  # Tiny
                shares_or_prn_amt=10_000,
                put_call="CALL",
            )
            for i in range(25)
        ]
        # Test one of them
        result = classify_option(
            holding=options[0],
            all_holdings=options,
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.UNCHANGED,
        )
        assert result == "EXCLUDE"

    def test_significant_change_included(self):
        """Options position that changed 50%+ QoQ = INCLUDE."""
        current = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="CALL",
            value_thousands=3_000,
            shares_or_prn_amt=30_000,
            put_call="CALL",
        )
        prior = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="CALL",
            value_thousands=1_500,  # Doubled
            shares_or_prn_amt=15_000,
            put_call="CALL",
        )
        result = classify_option(
            holding=current,
            all_holdings=[current],
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.ADDED,
            prior_holding=prior,
        )
        assert result == "INCLUDE"

    def test_default_flag(self):
        """Medium-sized option that doesn't match any rule = FLAG."""
        option = Holding(
            cusip="037833100",
            issuer_name="APPLE INC",
            title_of_class="CALL",
            value_thousands=3_000,  # 0.3% of AUM
            shares_or_prn_amt=30_000,
            put_call="CALL",
        )
        result = classify_option(
            holding=option,
            all_holdings=[option],
            total_aum_thousands=1_000_000,
            change_type=PositionChangeType.UNCHANGED,
        )
        assert result == "FLAG"

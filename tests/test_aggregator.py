"""Tests for cross-fund signal aggregation."""

from __future__ import annotations

from datetime import date

from core.aggregator import aggregate_signals
from core.models import FundDiff, FundInfo, PositionChangeType, PositionDiff, Tier


def _make_fund_diff(
    name: str,
    new: list[str] | None = None,
    exited: list[str] | None = None,
    added: list[str] | None = None,
    trimmed: list[str] | None = None,
) -> FundDiff:
    """Helper to create a FundDiff with minimal data."""
    fund = FundInfo(name=name, cik=f"CIK_{name}", tier=Tier.B)

    def _make_diffs(cusips: list[str], change_type: PositionChangeType):
        return [
            PositionDiff(
                cusip=c,
                issuer_name=f"Issuer {c}",
                change_type=change_type,
            )
            for c in (cusips or [])
        ]

    return FundDiff(
        fund=fund,
        current_quarter=date(2025, 9, 30),
        prior_quarter=date(2025, 6, 30),
        filing_date=date(2025, 11, 14),
        filing_lag_days=45,
        current_aum_thousands=1_000_000,
        prior_aum_thousands=900_000,
        aum_change_pct=0.111,
        new_positions=_make_diffs(new, PositionChangeType.NEW),
        exited_positions=_make_diffs(exited, PositionChangeType.EXITED),
        added_positions=_make_diffs(added, PositionChangeType.ADDED),
        trimmed_positions=_make_diffs(trimmed, PositionChangeType.TRIMMED),
        unchanged_positions=[],
        current_hhi=0.05,
        prior_hhi=0.04,
        hhi_change=0.01,
        current_top10_weight=0.60,
        prior_top10_weight=0.55,
    )


class TestAggregator:
    def test_crowded_trade_detected(self):
        """3+ funds buying same stock = crowded trade."""
        diffs = [
            _make_fund_diff("Fund A", added=["CUSIP_X"]),
            _make_fund_diff("Fund B", added=["CUSIP_X"]),
            _make_fund_diff("Fund C", new=["CUSIP_X"]),
        ]
        signals = aggregate_signals(diffs, date(2025, 9, 30), min_funds_for_crowd=3)

        assert len(signals.crowded_trades) == 1
        ct = signals.crowded_trades[0]
        assert ct.cusip == "CUSIP_X"
        assert ct.total_funds_buying == 3
        assert ct.net_fund_sentiment == 3

    def test_no_crowded_trade_below_threshold(self):
        """2 funds buying = not a crowded trade at threshold 3."""
        diffs = [
            _make_fund_diff("Fund A", added=["CUSIP_X"]),
            _make_fund_diff("Fund B", added=["CUSIP_X"]),
        ]
        signals = aggregate_signals(diffs, date(2025, 9, 30), min_funds_for_crowd=3)
        assert len(signals.crowded_trades) == 0

    def test_consensus_initiation(self):
        """3+ funds initiating NEW = consensus initiation."""
        diffs = [
            _make_fund_diff("Fund A", new=["CUSIP_Y"]),
            _make_fund_diff("Fund B", new=["CUSIP_Y"]),
            _make_fund_diff("Fund C", new=["CUSIP_Y"]),
        ]
        signals = aggregate_signals(
            diffs, date(2025, 9, 30), min_funds_for_consensus=3
        )
        assert len(signals.consensus_initiations) == 1
        assert len(signals.consensus_initiations[0].funds_initiated) == 3

    def test_divergence_detected(self):
        """Fund A initiated, Fund B exited = divergence."""
        diffs = [
            _make_fund_diff("Fund A", new=["CUSIP_Z"]),
            _make_fund_diff("Fund B", exited=["CUSIP_Z"]),
        ]
        signals = aggregate_signals(diffs, date(2025, 9, 30))

        assert len(signals.divergences) == 1
        div = signals.divergences[0]
        assert "Fund A" in div.initiated_by
        assert "Fund B" in div.exited_by

    def test_no_divergence_same_direction(self):
        """Both funds buying = no divergence."""
        diffs = [
            _make_fund_diff("Fund A", new=["CUSIP_Z"]),
            _make_fund_diff("Fund B", added=["CUSIP_Z"]),
        ]
        signals = aggregate_signals(diffs, date(2025, 9, 30))
        assert len(signals.divergences) == 0

    def test_funds_analyzed_count(self):
        """Should report correct number of funds analyzed."""
        diffs = [
            _make_fund_diff("Fund A", new=["C1"]),
            _make_fund_diff("Fund B", new=["C2"]),
            _make_fund_diff("Fund C", new=["C3"]),
        ]
        signals = aggregate_signals(diffs, date(2025, 9, 30))
        assert signals.funds_analyzed == 3

    def test_net_sentiment_with_selling(self):
        """Net sentiment should account for sellers."""
        diffs = [
            _make_fund_diff("Fund A", new=["CUSIP_X"]),
            _make_fund_diff("Fund B", added=["CUSIP_X"]),
            _make_fund_diff("Fund C", added=["CUSIP_X"]),
            _make_fund_diff("Fund D", trimmed=["CUSIP_X"]),
            _make_fund_diff("Fund E", exited=["CUSIP_X"]),
        ]
        signals = aggregate_signals(
            diffs, date(2025, 9, 30), min_funds_for_crowd=3
        )
        assert len(signals.crowded_trades) == 1
        ct = signals.crowded_trades[0]
        assert ct.total_funds_buying == 3
        assert ct.total_funds_selling == 2
        assert ct.net_fund_sentiment == 1

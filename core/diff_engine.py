"""Quarter-over-quarter diff computation with signal-rich metrics.

Solves the "Citadel/NVDA problem": a $2B add from $6B to $8B is noise.
Surfaces only high-conviction, non-obvious position changes by ranking
on shares_change_pct and weight_change, not raw dollar value.
"""

from __future__ import annotations

import logging

from core.models import (
    FundDiff,
    FundHoldings,
    Holding,
    PositionChangeType,
    PositionDiff,
)
from core.options_filter import classify_option

logger = logging.getLogger(__name__)


def compute_fund_diff(
    current: FundHoldings,
    prior: FundHoldings,
) -> FundDiff:
    """Compute QoQ diff between two quarters of holdings for one fund.

    Args:
        current: This quarter's holdings.
        prior: Prior quarter's holdings.

    Returns:
        FundDiff with categorized position changes and concentration metrics.
    """
    current_aum = current.total_value_thousands
    prior_aum = prior.total_value_thousands

    # Build lookup maps keyed by (cusip, put_call) to distinguish
    # equity vs options on the same underlying
    current_map: dict[tuple[str, str | None], Holding] = {
        (h.cusip, h.put_call): h for h in current.holdings
    }
    prior_map: dict[tuple[str, str | None], Holding] = {
        (h.cusip, h.put_call): h for h in prior.holdings
    }

    # Union of all position keys
    all_keys = set(current_map.keys()) | set(prior_map.keys())

    # Compute diffs
    new_positions: list[PositionDiff] = []
    exited_positions: list[PositionDiff] = []
    added_positions: list[PositionDiff] = []
    trimmed_positions: list[PositionDiff] = []
    unchanged_positions: list[PositionDiff] = []

    for key in all_keys:
        cusip, put_call = key
        curr = current_map.get(key)
        prev = prior_map.get(key)

        diff = _build_position_diff(
            cusip=cusip,
            put_call=put_call,
            current_holding=curr,
            prior_holding=prev,
            current_aum_k=current_aum,
            prior_aum_k=prior_aum,
            all_current_holdings=current.holdings,
        )

        # Skip excluded options
        if diff.is_options_position and diff.options_filter_action == "EXCLUDE":
            continue

        match diff.change_type:
            case PositionChangeType.NEW:
                new_positions.append(diff)
            case PositionChangeType.EXITED:
                exited_positions.append(diff)
            case PositionChangeType.ADDED:
                added_positions.append(diff)
            case PositionChangeType.TRIMMED:
                trimmed_positions.append(diff)
            case PositionChangeType.UNCHANGED:
                unchanged_positions.append(diff)

    # Sort by signal strength
    new_positions.sort(key=lambda d: d.current_value_thousands, reverse=True)
    exited_positions.sort(key=lambda d: d.prior_value_thousands, reverse=True)
    added_positions.sort(key=lambda d: d.shares_change_pct, reverse=True)
    trimmed_positions.sort(key=lambda d: d.shares_change_pct)  # Most negative first

    # Concentration metrics
    current_hhi = _compute_hhi(current.holdings, current_aum)
    prior_hhi = _compute_hhi(prior.holdings, prior_aum)
    current_top10 = _top_n_weight(current.holdings, current_aum, n=10)
    prior_top10 = _top_n_weight(prior.holdings, prior_aum, n=10)

    aum_change_pct = (
        (current_aum - prior_aum) / prior_aum if prior_aum > 0 else 0.0
    )

    return FundDiff(
        fund=current.fund,
        current_quarter=current.quarter_end,
        prior_quarter=prior.quarter_end,
        filing_date=current.filing_date,
        filing_lag_days=current.filing_lag_days,
        current_aum_thousands=current_aum,
        prior_aum_thousands=prior_aum,
        aum_change_pct=aum_change_pct,
        new_positions=new_positions,
        exited_positions=exited_positions,
        added_positions=added_positions,
        trimmed_positions=trimmed_positions,
        unchanged_positions=unchanged_positions,
        current_hhi=current_hhi,
        prior_hhi=prior_hhi,
        hhi_change=current_hhi - prior_hhi,
        current_top10_weight=current_top10,
        prior_top10_weight=prior_top10,
    )


def _build_position_diff(
    cusip: str,
    put_call: str | None,
    current_holding: Holding | None,
    prior_holding: Holding | None,
    current_aum_k: int,
    prior_aum_k: int,
    all_current_holdings: list[Holding],
) -> PositionDiff:
    """Build a PositionDiff for a single position key (cusip, put_call)."""
    curr_shares = current_holding.shares_or_prn_amt if current_holding else 0
    prev_shares = prior_holding.shares_or_prn_amt if prior_holding else 0
    curr_value_k = current_holding.value_thousands if current_holding else 0
    prev_value_k = prior_holding.value_thousands if prior_holding else 0

    # Determine change type
    if prev_shares == 0 and curr_shares > 0:
        change_type = PositionChangeType.NEW
    elif curr_shares == 0 and prev_shares > 0:
        change_type = PositionChangeType.EXITED
    elif curr_shares > prev_shares:
        change_type = PositionChangeType.ADDED
    elif curr_shares < prev_shares:
        change_type = PositionChangeType.TRIMMED
    else:
        change_type = PositionChangeType.UNCHANGED

    # Share change metrics
    shares_change = curr_shares - prev_shares
    if prev_shares > 0:
        shares_change_pct = shares_change / prev_shares
    elif curr_shares > 0:
        shares_change_pct = 1.0  # New position = 100% increase conceptually
    else:
        shares_change_pct = 0.0

    # Portfolio weight (as percentage, e.g., 3.5 means 3.5%)
    current_weight = (
        (curr_value_k / current_aum_k * 100) if current_aum_k > 0 else 0.0
    )
    prior_weight = (
        (prev_value_k / prior_aum_k * 100) if prior_aum_k > 0 else 0.0
    )
    weight_change = current_weight - prior_weight

    # Issuer name from whichever holding exists
    issuer = ""
    ticker = None
    sector = None
    if current_holding:
        issuer = current_holding.issuer_name
        ticker = current_holding.ticker
        sector = current_holding.sector
    elif prior_holding:
        issuer = prior_holding.issuer_name
        ticker = prior_holding.ticker
        sector = prior_holding.sector

    # Options classification
    is_option = put_call is not None
    options_action = "FLAG"
    if is_option and current_holding:
        options_action = classify_option(
            holding=current_holding,
            all_holdings=all_current_holdings,
            total_aum_thousands=current_aum_k,
            change_type=change_type,
            prior_holding=prior_holding,
        )
    elif is_option and not current_holding:
        # Exited option — always include exits
        options_action = "INCLUDE"

    return PositionDiff(
        cusip=cusip,
        ticker=ticker,
        issuer_name=issuer,
        put_call=put_call,
        sector=sector,
        current_shares=curr_shares,
        current_value_thousands=curr_value_k,
        current_weight_pct=current_weight,
        prior_shares=prev_shares,
        prior_value_thousands=prev_value_k,
        prior_weight_pct=prior_weight,
        change_type=change_type,
        shares_change=shares_change,
        shares_change_pct=shares_change_pct,
        value_change_thousands=curr_value_k - prev_value_k,
        weight_change_pct=weight_change,
        is_options_position=is_option,
        options_filter_action=options_action,
    )


def _compute_hhi(holdings: list[Holding], total_value_k: int) -> float:
    """Herfindahl-Hirschman Index — sum of squared portfolio weights.

    Lower = more diversified, higher = more concentrated.
    Range: 0 to 1 (or 0 to 10000 if using percentage points).
    """
    if total_value_k == 0:
        return 0.0
    return sum(
        (h.value_thousands / total_value_k) ** 2
        for h in holdings
    )


def _top_n_weight(
    holdings: list[Holding], total_value_k: int, n: int = 10
) -> float:
    """Sum of top-N position weights (as fraction, 0 to 1)."""
    if total_value_k == 0:
        return 0.0
    weights = sorted(
        [h.value_thousands / total_value_k for h in holdings],
        reverse=True,
    )
    return sum(weights[:n])

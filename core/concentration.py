"""Concentration metrics and historical conviction tracking.

Computes:
- HHI (Herfindahl-Hirschman Index) over time
- Top-N position concentration
- Conviction scoring for individual positions (quarters held, add/trim pattern)
"""

from __future__ import annotations

import logging
from datetime import date

from core.models import ConvictionTrack, Holding

logger = logging.getLogger(__name__)


def build_conviction_tracks(
    holding_history: list[tuple[date, list[Holding]]],
    fund_name: str,
    cusip: str,
    total_values: dict[date, int] | None = None,
) -> ConvictionTrack | None:
    """Build conviction tracking for a single fund-position pair.

    Args:
        holding_history: List of (quarter_end, holdings_for_that_cusip)
            sorted from most recent to oldest.
        fund_name: Name of the fund.
        cusip: CUSIP being tracked.
        total_values: Optional {quarter: total_aum_thousands} for weight calc.

    Returns:
        ConvictionTrack or None if no history found.
    """
    if not holding_history:
        return None

    quarters_held = 0
    consecutive_adds = 0
    consecutive_trims = 0
    weight_history: list[float] = []
    shares_history: list[int] = []
    prev_shares: int | None = None
    issuer_name = ""
    ticker = None

    # Walk through history from oldest to newest for add/trim tracking
    for quarter_end, holdings in reversed(holding_history):
        matching = [h for h in holdings if h.cusip == cusip]
        if not matching:
            continue

        h = matching[0]
        quarters_held += 1
        issuer_name = h.issuer_name
        ticker = h.ticker or ticker

        shares_history.append(h.shares_or_prn_amt)

        # Calculate weight if we have AUM
        if total_values and quarter_end in total_values:
            aum = total_values[quarter_end]
            weight = (h.value_thousands / aum * 100) if aum > 0 else 0.0
            weight_history.append(round(weight, 2))

        # Track consecutive adds/trims
        if prev_shares is not None:
            if h.shares_or_prn_amt > prev_shares:
                consecutive_adds += 1
                consecutive_trims = 0
            elif h.shares_or_prn_amt < prev_shares:
                consecutive_trims += 1
                consecutive_adds = 0
            # unchanged: don't reset either counter

        prev_shares = h.shares_or_prn_amt

    if quarters_held == 0:
        return None

    return ConvictionTrack(
        fund_name=fund_name,
        cusip=cusip,
        ticker=ticker,
        issuer_name=issuer_name,
        quarters_held=quarters_held,
        consecutive_adds=consecutive_adds,
        consecutive_trims=consecutive_trims,
        weight_history=weight_history,
        shares_history=shares_history,
    )


def compute_portfolio_concentration(
    holdings: list[Holding],
    total_value_k: int,
) -> dict[str, float]:
    """Compute various concentration metrics for a portfolio.

    Returns:
        {
            "hhi": float,
            "top5_weight": float,
            "top10_weight": float,
            "top20_weight": float,
            "position_count": int,
            "effective_positions": float,  # 1/HHI — number of equally-weighted positions
        }
    """
    if total_value_k == 0 or not holdings:
        return {
            "hhi": 0.0,
            "top5_weight": 0.0,
            "top10_weight": 0.0,
            "top20_weight": 0.0,
            "position_count": 0,
            "effective_positions": 0.0,
        }

    weights = sorted(
        [h.value_thousands / total_value_k for h in holdings],
        reverse=True,
    )

    hhi = sum(w**2 for w in weights)
    effective = 1.0 / hhi if hhi > 0 else 0.0

    return {
        "hhi": hhi,
        "top5_weight": sum(weights[:5]),
        "top10_weight": sum(weights[:10]),
        "top20_weight": sum(weights[:20]),
        "position_count": len(holdings),
        "effective_positions": effective,
    }

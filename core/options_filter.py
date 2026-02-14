"""Smart options inclusion/exclusion filter.

Rules:
  INCLUDE when:
    - New PUT on a stock the fund does NOT own as equity (directional bet)
    - New large CALL position (> 0.5% of AUM)
    - Significant options exposure change (> 50% change in options value)
    - Options in top-10 positions by dollar value
    - Any option > 0.5% of AUM

  EXCLUDE when:
    - Small option alongside a large equity position in same issuer (routine hedge)
    - Fund has 20+ small option positions (market-making noise)

  FLAG (include with [PUT]/[CALL] annotation) when:
    - All other cases
"""

from __future__ import annotations

from typing import Literal

from core.models import Holding, PositionChangeType


def classify_option(
    holding: Holding,
    all_holdings: list[Holding],
    total_aum_thousands: int,
    change_type: PositionChangeType,
    prior_holding: Holding | None = None,
    aum_threshold: float = 0.005,
) -> Literal["INCLUDE", "EXCLUDE", "FLAG"]:
    """Determine whether an options position should be included, excluded, or flagged.

    Args:
        holding: The options position to classify.
        all_holdings: All holdings in the same quarter (for context).
        total_aum_thousands: Total fund AUM in thousands.
        change_type: How this position changed QoQ.
        prior_holding: The same position last quarter (if it existed).
        aum_threshold: Weight threshold for automatic inclusion (default 0.5%).

    Returns:
        "INCLUDE", "EXCLUDE", or "FLAG"
    """
    if not holding.is_option:
        return "INCLUDE"  # Not an option — always include equities

    weight = (
        holding.value_thousands / total_aum_thousands
        if total_aum_thousands > 0
        else 0
    )

    # --- INCLUDE conditions ---

    # 1. New PUT on stock fund doesn't own as equity (directional bearish bet)
    if change_type == PositionChangeType.NEW and holding.put_call == "PUT":
        if not _fund_has_equity_in_issuer(holding.issuer_cusip_prefix, all_holdings):
            return "INCLUDE"

    # 2. New CALL that's significant by weight
    if change_type == PositionChangeType.NEW and holding.put_call == "CALL":
        if weight >= aum_threshold:
            return "INCLUDE"

    # --- EXCLUDE conditions (checked BEFORE weight threshold) ---

    # 3. Small option alongside large equity in same issuer (routine hedge)
    #    This takes priority over weight threshold — a 0.5% hedge on a 5% equity
    #    position is still just a hedge.
    equity_value = _get_equity_value_for_issuer(
        holding.issuer_cusip_prefix, all_holdings
    )
    if equity_value > 0 and holding.value_thousands < equity_value * 0.10:
        return "EXCLUDE"

    # 4. Any option > threshold % of AUM (only if not a routine hedge)
    if weight >= aum_threshold:
        return "INCLUDE"

    # 5. Market-making noise: fund has 20+ small option positions
    small_option_count = sum(
        1
        for h in all_holdings
        if h.is_option
        and total_aum_thousands > 0
        and (h.value_thousands / total_aum_thousands) < 0.002
    )
    if small_option_count >= 20:
        return "EXCLUDE"

    # --- More INCLUDE conditions ---

    # 6. In top-10 by dollar value (only meaningful with 10+ holdings)
    if len(all_holdings) >= 10 and _in_top_n_by_value(holding, all_holdings, n=10):
        return "INCLUDE"

    # 7. Significant options exposure change (> 50% QoQ)
    if prior_holding is not None and prior_holding.value_thousands > 0:
        change_pct = (
            abs(holding.value_thousands - prior_holding.value_thousands)
            / prior_holding.value_thousands
        )
        if change_pct >= 0.50:
            return "INCLUDE"

    # 8. New options position with meaningful $ value (> $10M notional)
    if change_type == PositionChangeType.NEW and holding.value_thousands >= 10_000:
        return "INCLUDE"

    # --- Default: FLAG (include with annotation) ---
    return "FLAG"


def _fund_has_equity_in_issuer(
    issuer_prefix: str, holdings: list[Holding]
) -> bool:
    """Check if the fund holds equity (non-option) in the same issuer."""
    return any(
        h.issuer_cusip_prefix == issuer_prefix and h.is_equity
        for h in holdings
    )


def _get_equity_value_for_issuer(
    issuer_prefix: str, holdings: list[Holding]
) -> int:
    """Get total equity value (in thousands) for the same issuer."""
    return sum(
        h.value_thousands
        for h in holdings
        if h.issuer_cusip_prefix == issuer_prefix and h.is_equity
    )


def _in_top_n_by_value(
    holding: Holding, all_holdings: list[Holding], n: int = 10
) -> bool:
    """Check if this holding is in the top N by dollar value."""
    sorted_holdings = sorted(
        all_holdings, key=lambda h: h.value_thousands, reverse=True
    )
    top_keys = {
        (h.cusip, h.put_call) for h in sorted_holdings[:n]
    }
    return (holding.cusip, holding.put_call) in top_keys

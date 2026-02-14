"""Signal Scanner — primary dashboard.

Shows high-conviction signals across all watched funds for the selected quarter.
Tabs for New Positions, Exits, Largest Adds, Largest Trims.
"""

from __future__ import annotations

from datetime import date

import streamlit as st

from app.components.diff_table import render_diff_table
from core.models import FundDiff, PositionDiff, Tier


def _quarter_tag(q: date) -> str:
    """Short quarter label like 'Q3 25'."""
    qnum = (q.month - 1) // 3 + 1
    return f"Q{qnum} {q.year % 100}"


def render() -> None:
    """Render the Signal Scanner page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>4</span>"
        "Signal Scanner</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Surfaces the **highest-conviction position changes** across all tracked "
        "funds — new initiations, full exits, and concentrated adds/trims — "
        "ranked by signal strength (% share change), not just dollar size. "
        "A fund doubling a small position is more informative than a mega-cap "
        "adding 2% to an existing $5B stake."
    )

    quarter = st.session_state.get("selected_quarter")
    fund_diffs: dict[date, list[FundDiff]] = st.session_state.get("fund_diffs", {})

    if not quarter or quarter not in fund_diffs:
        st.info(
            "No analysis data available. Select a quarter and ensure data has been "
            "fetched and analyzed."
        )
        return

    diffs = fund_diffs[quarter]
    selected_tiers = st.session_state.get("selected_tiers", list(Tier))
    filtered_diffs = [d for d in diffs if d.fund.tier in selected_tiers]

    if not filtered_diffs:
        st.warning("No funds match the selected tier filters.")
        return

    # Show quarter coverage breakdown if funds span different quarters
    quarter_counts: dict[str, int] = {}
    for d in filtered_diffs:
        tag = _quarter_tag(d.current_quarter)
        quarter_counts[tag] = quarter_counts.get(tag, 0) + 1
    if len(quarter_counts) > 1:
        breakdown = " | ".join(f"**{tag}**: {n} funds" for tag, n in quarter_counts.items())
        st.caption(f"Data mix: {breakdown}")

    # Summary metrics
    all_new = _collect_across_funds(filtered_diffs, "new_positions")
    all_exited = _collect_across_funds(filtered_diffs, "exited_positions")
    all_added = _collect_across_funds(filtered_diffs, "added_positions")
    all_trimmed = _collect_across_funds(filtered_diffs, "trimmed_positions")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("New Positions", len(all_new))
    col2.metric("Exits", len(all_exited))
    col3.metric("Significant Adds", len([a for a in all_added if a.is_significant_add]))
    col4.metric("Significant Trims", len([t for t in all_trimmed if t.is_significant_trim]))

    # Options filter
    options_filter = st.radio(
        "Options",
        options=["Show All", "Equity Only", "Options Only"],
        horizontal=True,
        index=0,
    )

    # Tabs
    tab_new, tab_exit, tab_add, tab_trim, tab_weight = st.tabs([
        f"New ({len(all_new)})",
        f"Exits ({len(all_exited)})",
        f"Adds ({len(all_added)})",
        f"Trims ({len(all_trimmed)})",
        "Weight Changes",
    ])

    with tab_new:
        st.caption(
            "Brand new positions initiated this quarter (not held last quarter). "
            "Sorted by dollar value. Prefixed with [Fund Name]."
        )
        filtered = _apply_options_filter(all_new, options_filter)
        render_diff_table(filtered, title="New Positions Across All Funds")

    with tab_exit:
        st.caption(
            "Positions fully liquidated — held last quarter, zero this quarter. "
            "Sorted by prior-quarter value."
        )
        filtered = _apply_options_filter(all_exited, options_filter)
        render_diff_table(filtered, title="Exited Positions")

    with tab_add:
        st.caption(
            "Existing positions where the fund increased shares by 50%+. "
            "Sorted by percentage share increase — a fund doubling a position "
            "is a stronger signal than a small add to a large holding."
        )
        sig_adds = [a for a in all_added if a.is_significant_add]
        filtered = _apply_options_filter(sig_adds, options_filter)
        render_diff_table(filtered, title="Significant Adds (50%+ Share Increase)")

    with tab_trim:
        st.caption(
            "Existing positions where the fund cut shares by 60%+. "
            "Near-total liquidations that didn't quite reach zero — "
            "often the fund winding down a position over 2 quarters."
        )
        sig_trims = [t for t in all_trimmed if t.is_significant_trim]
        filtered = _apply_options_filter(sig_trims, options_filter)
        render_diff_table(filtered, title="Significant Trims (60%+ Share Decrease)")

    with tab_weight:
        st.caption(
            "All position changes ranked by absolute portfolio weight change "
            "(percentage points of AUM). Shows which moves had the biggest "
            "impact on portfolio construction, regardless of direction."
        )
        # All changes sorted by absolute weight change
        all_changes = all_new + all_exited + all_added + all_trimmed
        all_changes.sort(key=lambda d: abs(d.weight_change_pct), reverse=True)
        filtered = _apply_options_filter(all_changes[:100], options_filter)
        render_diff_table(filtered, title="Largest Weight Changes")


def _collect_across_funds(
    diffs: list[FundDiff], field: str
) -> list[PositionDiff]:
    """Collect position diffs from all funds, adding fund name + quarter context."""
    # Determine if we need quarter tags (mixed quarters)
    quarters_seen = {d.current_quarter for d in diffs}
    show_quarter = len(quarters_seen) > 1

    result: list[PositionDiff] = []
    for d in diffs:
        positions = getattr(d, field, [])
        qtag = f" {_quarter_tag(d.current_quarter)}" if show_quarter else ""
        for pos in positions:
            enriched = pos.model_copy()
            stock_label = pos.ticker or pos.issuer_name
            enriched.issuer_name = f"[{d.fund.name}{qtag}] {stock_label}"
            result.append(enriched)
    # Sort by value for new/exit, by change% for adds/trims
    if field in ("new_positions", "exited_positions"):
        result.sort(
            key=lambda p: p.current_value_thousands or p.prior_value_thousands,
            reverse=True,
        )
    else:
        result.sort(key=lambda p: abs(p.shares_change_pct), reverse=True)
    return result


def _apply_options_filter(
    diffs: list[PositionDiff], filter_mode: str
) -> list[PositionDiff]:
    """Filter diffs based on options selection."""
    if filter_mode == "Equity Only":
        return [d for d in diffs if not d.is_options_position]
    if filter_mode == "Options Only":
        return [d for d in diffs if d.is_options_position]
    return diffs

"""Full holdings table component."""

from __future__ import annotations

import streamlit as st

from core.models import Holding


def render_holdings_table(
    holdings: list[Holding],
    total_value_thousands: int,
    title: str = "Current Holdings",
    max_rows: int = 100,
) -> None:
    """Render a sortable table of all holdings.

    Args:
        holdings: List of Holding objects (should be sorted by value desc).
        total_value_thousands: Total portfolio AUM for weight calculation.
        title: Section title.
        max_rows: Maximum rows to display.
    """
    if not holdings:
        st.info("No holdings data available.")
        return

    st.markdown(f"### {title} ({len(holdings)} positions)")

    rows = []
    for h in holdings[:max_rows]:
        weight = (
            (h.value_thousands / total_value_thousands * 100)
            if total_value_thousands > 0
            else 0.0
        )
        rows.append({
            "Stock": h.display_label,
            "CUSIP": h.cusip,
            "Value": _fmt_value(h.value_thousands),
            "Shares": f"{h.shares_or_prn_amt:,}",
            "Weight %": f"{weight:.2f}%",
            "Sector": h.sector or "—",
            "Type": h.put_call or "Equity",
        })

    st.dataframe(
        rows,
        use_container_width=True,
        hide_index=True,
    )

    if len(holdings) > max_rows:
        st.caption(f"Showing top {max_rows} of {len(holdings)} positions")


def _fmt_value(thousands: int) -> str:
    """Format $thousands into human-readable."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if abs(dollars) >= 1_000:
        return f"${dollars / 1_000:.0f}K"
    return f"${dollars:.0f}"

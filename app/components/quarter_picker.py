"""Quarter selection widget with per-fund coverage display."""

from __future__ import annotations

from datetime import date

import streamlit as st


def render_quarter_picker(available_quarters: list[date]) -> date | None:
    """Render quarter selection in sidebar. Returns selected quarter.

    Automatically selects the most recent quarter and shows how many
    funds have data for it vs an earlier quarter.
    """
    if not available_quarters:
        st.info("No data loaded yet. Use 'Fetch Data' to pull filings from EDGAR.")
        return None

    labels = [_quarter_label(q) for q in available_quarters]

    # Build coverage annotation for each quarter
    store = st.session_state.get("store")
    watchlist = st.session_state.get("watchlist", [])
    if store and watchlist:
        for i, q in enumerate(available_quarters):
            n_funds = store.get_holdings_count_by_quarter(q)
            labels[i] = f"{labels[i]}  ({n_funds} funds)"

    selected_label = st.selectbox(
        "Quarter",
        options=labels,
        index=0,
        key="quarter_picker",
    )

    idx = labels.index(selected_label)
    selected = available_quarters[idx]
    st.session_state.selected_quarter = selected
    return selected


def _quarter_label(d: date) -> str:
    """Convert date to 'Q4 2025' format."""
    quarter = (d.month - 1) // 3 + 1
    return f"Q{quarter} {d.year}"

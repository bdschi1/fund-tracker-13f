"""Fund and tier selection sidebar widgets."""

from __future__ import annotations

import streamlit as st

from core.models import FundInfo, Tier

TIER_LABELS = {
    Tier.A: "A — Multi-Strat",
    Tier.B: "B — Stock Pickers",
    Tier.C: "C — Event-Driven",
    Tier.D: "D — Emerging",
    Tier.E: "E — Healthcare",
}


def render_tier_filter() -> list[Tier]:
    """Render tier checkboxes in sidebar. Returns selected tiers."""
    st.markdown("**Fund Tiers**")
    selected: list[Tier] = []
    for tier in Tier:
        if st.checkbox(TIER_LABELS[tier], value=True, key=f"tier_{tier.value}"):
            selected.append(tier)
    st.session_state.selected_tiers = selected
    return selected


def render_fund_picker(funds: list[FundInfo]) -> FundInfo | None:
    """Render a selectbox for picking a single fund. Returns selected fund."""
    if not funds:
        st.info("No funds available for selected tiers.")
        return None

    fund_names = [f.name for f in funds]
    selected_name = st.selectbox(
        "Select Fund",
        options=fund_names,
        index=0,
        key="fund_picker",
    )

    selected = next((f for f in funds if f.name == selected_name), None)
    if selected:
        st.session_state.selected_fund_cik = selected.cik
    return selected

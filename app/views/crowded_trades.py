"""Crowded Trades — cross-fund aggregation view.

Shows consensus buys, consensus sells, and divergences across all
watched funds.
"""

from __future__ import annotations

from datetime import date

import streamlit as st

from app.components.charts import (
    crowded_trade_dot_plot,
    crowded_trades_bar_chart,
    sector_flows_chart,
)
from core.models import CrossFundSignals


def render() -> None:
    """Render the Crowded Trades page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>5</span>"
        "Crowded Trades</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Aggregates position changes across all funds to identify "
        "**crowded trades** (3+ funds buying the same stock), "
        "**consensus initiations** (3+ funds opening brand-new positions), "
        "and **divergences** (one fund initiated while another exited). "
        "Also shows net sector flows to reveal broad allocation shifts."
    )

    quarter = st.session_state.get("selected_quarter")
    cross_signals: dict[date, CrossFundSignals] = st.session_state.get(
        "cross_signals", {}
    )

    if not quarter or quarter not in cross_signals:
        st.info("No cross-fund signals available. Ensure data has been fetched and analyzed.")
        return

    signals = cross_signals[quarter]

    # Summary
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Funds Analyzed", signals.funds_analyzed)
    col2.metric("Consensus Buys", len(signals.crowded_trades))
    col3.metric("Consensus Initiations", len(signals.consensus_initiations))
    col4.metric("Divergences", len(signals.divergences))

    st.divider()

    tab_crowd, tab_consensus, tab_div, tab_sector = st.tabs([
        "Crowded Trades",
        "Consensus Initiations",
        "Divergences",
        "Sector Flows",
    ])

    with tab_crowd:
        if signals.crowded_trades:
            st.caption(
                "Stocks where 3+ funds acted in the same direction. "
                "The bar chart shows net fund count (green = buying, "
                "red = selling). The dot plot below shows exactly WHICH "
                "funds are on each side — hover for names."
            )
            # Summary bar chart
            st.plotly_chart(
                crowded_trades_bar_chart(signals.crowded_trades),
                use_container_width=True,
            )
            # Fund-level dot plot detail
            st.plotly_chart(
                crowded_trade_dot_plot(signals.crowded_trades),
                use_container_width=True,
            )
            st.markdown("### Details")
            rows = []
            for ct in signals.crowded_trades:
                rows.append({
                    "Stock": ct.display_label,
                    "Buying": ct.total_funds_buying,
                    "Selling": ct.total_funds_selling,
                    "Net": ct.net_fund_sentiment,
                    "Initiated By": ", ".join(ct.funds_initiated[:5]),
                    "Added By": ", ".join(ct.funds_added[:5]),
                    "Sector": ct.sector or "—",
                    "Themes": ", ".join(ct.themes[:2]) if ct.themes else "",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No crowded trades detected (fewer than 3 funds buying any single stock).")

    with tab_consensus:
        if signals.consensus_initiations:
            st.caption(
                "The strongest buy signal: multiple funds independently "
                "opened brand-new positions in the same stock this quarter. "
                "These are fresh convictions, not existing positions being topped up."
            )
            st.markdown(
                "Stocks where **3+ funds initiated a brand new position** this quarter."
            )
            rows = []
            for ct in signals.consensus_initiations:
                rows.append({
                    "Stock": ct.display_label,
                    "Funds Initiated": len(ct.funds_initiated),
                    "Names": ", ".join(ct.funds_initiated),
                    "Also Added By": len(ct.funds_added),
                    "Sector": ct.sector or "—",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No consensus initiations detected.")

    with tab_div:
        if signals.divergences:
            st.caption(
                "Opposing bets: one fund is buying in while another is selling out "
                "of the same stock. These are the most interesting analytical debates — "
                "both sides see different futures for the same company."
            )
            st.markdown(
                "Stocks where **one fund initiated** while **another exited** — "
                "who's right?"
            )
            rows = []
            for div in signals.divergences:
                rows.append({
                    "Stock": div.display_label,
                    "Initiated By": ", ".join(div.initiated_by),
                    "Exited By": ", ".join(div.exited_by),
                    "Sector": div.sector or "—",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No divergences detected.")

    with tab_sector:
        if signals.sector_flows:
            st.caption(
                "Net fund count by sector: positive = more funds buying "
                "than selling in that sector, negative = net selling. "
                "Reveals broad allocation shifts (e.g., rotation from "
                "tech into healthcare). Requires sector enrichment data."
            )
            st.plotly_chart(
                sector_flows_chart(signals.sector_flows),
                use_container_width=True,
            )
        else:
            st.info("No sector flow data available.")

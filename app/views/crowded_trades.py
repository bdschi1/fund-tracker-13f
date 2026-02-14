"""Crowded Trades — cross-fund aggregation view.

Shows consensus buys, consensus sells, divergences, crowding risk,
and dollar-weighted sector flows across all watched funds.
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


def _fmt_val(thousands: int) -> str:
    """Format $thousands into human-readable."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if abs(dollars) >= 1_000:
        return f"${dollars / 1_000:.0f}K"
    return f"${dollars:,.0f}"


def render() -> None:
    """Render the Crowded Trades page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>5</span>"
        "Crowded Trades</h2>",
        unsafe_allow_html=True,
    )
    st.caption(
        "3+ funds buying the same stock · consensus initiations · "
        "divergences · float crowding risk · sector flows."
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
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Funds Analyzed", signals.funds_analyzed)
    col2.metric("Consensus Buys", len(signals.crowded_trades))
    col3.metric("Consensus Initiations", len(signals.consensus_initiations))
    col4.metric("Divergences", len(signals.divergences))
    col5.metric(
        "Crowding Risks",
        len(signals.crowding_risks),
        help="Stocks where tracked funds own >5% of float",
    )

    st.divider()

    tab_crowd, tab_consensus, tab_div, tab_crowding, tab_sector = st.tabs([
        "Crowded Trades",
        "Consensus Initiations",
        "Divergences",
        "🚨 Crowding Risk",
        "Sector Flows",
    ])

    with tab_crowd:
        if signals.crowded_trades:
            st.caption(
                "3+ funds on the same side. Bar = net count · "
                "dot plot = which funds. Hover for names."
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
                row = {
                    "Stock": ct.display_label,
                    "Buying": ct.total_funds_buying,
                    "Selling": ct.total_funds_selling,
                    "Net": ct.net_fund_sentiment,
                    "Agg. Value": _fmt_val(ct.aggregate_value_thousands),
                    "Initiated By": ", ".join(ct.funds_initiated[:5]),
                    "Added By": ", ".join(ct.funds_added[:5]),
                    "Sector": ct.sector or "—",
                    "Themes": ", ".join(ct.themes[:2]) if ct.themes else "",
                }
                if ct.float_ownership_pct is not None:
                    row["Float %"] = f"{ct.float_ownership_pct:.1f}%"
                else:
                    row["Float %"] = "—"
                rows.append(row)
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No crowded trades detected (fewer than 3 funds buying any single stock).")

    with tab_consensus:
        if signals.consensus_initiations:
            st.caption(
                "Strongest buy signal — 3+ funds independently opened brand-new positions "
                "in the same stock. Fresh convictions, not top-ups."
            )
            rows = []
            for ct in signals.consensus_initiations:
                rows.append({
                    "Stock": ct.display_label,
                    "Funds Initiated": len(ct.funds_initiated),
                    "Names": ", ".join(ct.funds_initiated),
                    "Also Added By": len(ct.funds_added),
                    "Agg. Value": _fmt_val(ct.aggregate_value_thousands),
                    "Sector": ct.sector or "—",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No consensus initiations detected.")

    with tab_div:
        if signals.divergences:
            st.caption(
                "Opposing bets — one fund initiated while "
                "another exited the same stock. Who's right?"
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

    with tab_crowding:
        st.caption(
            "Tracked funds collectively own ≥ 5% of float. "
            "High ownership = liquidation risk if multiple funds exit at once."
        )
        if signals.crowding_risks:
            rows = []
            for ct in signals.crowding_risks:
                rows.append({
                    "Stock": ct.display_label,
                    "Float %": f"{ct.float_ownership_pct:.1f}%",
                    "Agg. Value": _fmt_val(ct.aggregate_value_thousands),
                    "Agg. Shares": f"{ct.aggregate_shares:,.0f}",
                    "Float Shares": f"{ct.float_shares:,.0f}" if ct.float_shares else "—",
                    "Sector": ct.sector or "—",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)

            # Highlight top risks
            top = signals.crowding_risks[0]
            if top.float_ownership_pct and top.float_ownership_pct >= 10:
                st.error(
                    f"⚠️ **{top.display_label}**: tracked funds own "
                    f"**{top.float_ownership_pct:.1f}%** of the float "
                    f"({_fmt_val(top.aggregate_value_thousands)} aggregate). "
                    f"Exit congestion risk is elevated."
                )
        else:
            st.info(
                "No crowding risks detected. Either no stocks exceed 5% float "
                "ownership, or sector enrichment hasn't been run (float data "
                "not available)."
            )

    with tab_sector:
        # Show both fund-count and dollar-weighted views
        if signals.sector_flows or signals.sector_dollar_flows:
            view_mode = st.radio(
                "View",
                options=["Fund Count", "Dollar-Weighted"],
                horizontal=True,
                index=0,
                help=(
                    "Fund Count: how many funds are buying vs selling each sector. "
                    "Dollar-Weighted: aggregate $ value flowing into/out of each sector."
                ),
            )

            if view_mode == "Fund Count" and signals.sector_flows:
                st.caption("Net funds buying vs. selling per sector. Reveals rotation patterns.")
                st.plotly_chart(
                    sector_flows_chart(signals.sector_flows),
                    use_container_width=True,
                )
            elif signals.sector_dollar_flows:
                st.caption("Aggregate $ flowing in/out per sector — not just head counts.")
                # Build a formatted table
                sorted_sectors = sorted(
                    signals.sector_dollar_flows.items(),
                    key=lambda x: abs(x[1]["net_k"]),
                    reverse=True,
                )
                rows = []
                for sector, counts in sorted_sectors:
                    if sector == "Unknown":
                        continue
                    net = counts["net_k"]
                    arrow = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
                    rows.append({
                        "Sector": sector,
                        "Buying": _fmt_val(counts["buying_k"]),
                        "Selling": _fmt_val(counts["selling_k"]),
                        "Net Flow": f"{arrow} {_fmt_val(net)}",
                    })
                if rows:
                    st.dataframe(rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No sector dollar flow data available.")
            else:
                st.info("No sector flow data available for this view.")
        else:
            st.info("No sector flow data available.")

"""Portfolio Overlap Matrix — heatmap of fund-to-fund similarity.

Uses Jaccard similarity on CUSIP sets to measure overlap between
any two funds' portfolios. Includes Sankey view of shared holdings.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.components.charts import overlap_heatmap, shared_holdings_sankey
from core.aggregator import compute_most_widely_held
from core.models import Tier


def _compute_jaccard(
    funds_with_data, all_holdings,
) -> tuple[list[str], list[list[float]], dict[str, set[str]]]:
    """Compute Jaccard similarity matrix.

    Returns (names, matrix, fund_cusips_dict).
    """
    fund_cusips: dict[str, set[str]] = {}
    for fund in funds_with_data:
        holdings = all_holdings.get(fund.cik, [])
        equity_cusips = {h.cusip for h in holdings if not h.is_option}
        fund_cusips[fund.cik] = equity_cusips

    names = [f.name for f in funds_with_data]
    n = len(funds_with_data)
    matrix = [[0.0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i == j:
                matrix[i][j] = 1.0
            else:
                set_i = fund_cusips[funds_with_data[i].cik]
                set_j = fund_cusips[funds_with_data[j].cik]
                union = set_i | set_j
                if union:
                    matrix[i][j] = len(set_i & set_j) / len(union)
                else:
                    matrix[i][j] = 0.0

    return names, matrix, fund_cusips


def _select_top_connected(
    names: list[str],
    matrix: list[list[float]],
    max_funds: int,
) -> tuple[list[str], list[list[float]], list[int]]:
    """Select the top N most-connected funds by average overlap.

    Returns (filtered_names, filtered_matrix, original_indices).
    """
    n = len(names)
    if n <= max_funds:
        return names, matrix, list(range(n))

    # Score each fund by its average off-diagonal overlap
    avg_overlaps = []
    for i in range(n):
        total = sum(matrix[i][j] for j in range(n) if i != j)
        avg_overlaps.append((i, total / (n - 1) if n > 1 else 0))

    # Take top N by average overlap (most connected funds)
    avg_overlaps.sort(key=lambda x: x[1], reverse=True)
    indices = sorted([idx for idx, _ in avg_overlaps[:max_funds]])

    filtered_names = [names[i] for i in indices]
    filtered_matrix = [
        [matrix[i][j] for j in indices]
        for i in indices
    ]
    return filtered_names, filtered_matrix, indices


def render() -> None:
    """Render the Overlap Matrix page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>6</span>"
        "Portfolio Overlap Matrix</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Compares every pair of fund portfolios to find **which funds "
        "are holding the same stocks**. Overlap is measured as a "
        "percentage: if Fund A holds 100 stocks and Fund B holds 80, "
        "and 20 of those are the same, the overlap is "
        "20 ÷ 160 unique stocks = **12.5%**. Higher overlap means "
        "the funds are fishing in the same pond — useful for spotting "
        "crowding risk or validating conviction when two independent "
        "managers agree. Only equity positions are compared (options "
        "excluded)."
    )

    quarter = st.session_state.get("selected_quarter")
    store = st.session_state.get("store")
    watchlist = st.session_state.get("watchlist", [])
    selected_tiers = st.session_state.get("selected_tiers", list(Tier))

    if not quarter or not store:
        st.info("No data available. Ensure filings have been fetched.")
        return

    # Filter funds by tier
    funds = [f for f in watchlist if f.tier in selected_tiers]
    if len(funds) < 2:
        st.warning("Need at least 2 funds to compute overlap.")
        return

    # Load holdings for all funds
    all_holdings = store.get_all_holdings_for_quarter(quarter)

    # Filter to funds that actually have data
    funds_with_data = [f for f in funds if f.cik in all_holdings]
    if len(funds_with_data) < 2:
        st.warning("Need at least 2 funds with data for this quarter.")
        return

    # Compute full Jaccard matrix
    names, matrix, fund_cusips = _compute_jaccard(funds_with_data, all_holdings)
    n = len(names)

    st.caption(f"Showing {n} funds with data for {quarter}")

    # ---------------------------------------------------------------
    # Tabs — Top Pairs first (always readable), then Heatmap, Sankey
    # ---------------------------------------------------------------
    tab_pairs, tab_heatmap, tab_sankey = st.tabs([
        "Top Pairs",
        "Overlap Heatmap",
        "Shared Holdings",
    ])

    # --- Top Pairs (default) ---
    with tab_pairs:
        st.markdown("### Most Overlapping Fund Pairs")
        st.markdown(
            "Every possible fund pair, ranked by portfolio overlap. "
            "**Overlap** = the percentage of their combined stock "
            "universe that both funds hold. **Stocks in Common** = "
            "the actual count of stocks held by both. "
            "**A Holdings / B Holdings** = total equity positions "
            "in each fund's portfolio."
        )
        pairs = []
        for i in range(n):
            for j in range(i + 1, n):
                shared = len(
                    fund_cusips[funds_with_data[i].cik]
                    & fund_cusips[funds_with_data[j].cik]
                )
                total_a = len(fund_cusips[funds_with_data[i].cik])
                total_b = len(fund_cusips[funds_with_data[j].cik])
                pairs.append({
                    "Fund A": names[i],
                    "Fund B": names[j],
                    "Overlap": matrix[i][j],
                    "Stocks in Common": shared,
                    "A Holdings": total_a,
                    "B Holdings": total_b,
                })
        pairs.sort(key=lambda p: p["Overlap"], reverse=True)

        # Format for display
        df_pairs = pd.DataFrame(pairs[:50])
        if not df_pairs.empty:
            df_pairs["Overlap"] = df_pairs["Overlap"].apply(
                lambda x: f"{x:.1%}"
            )
            st.dataframe(
                df_pairs,
                use_container_width=True,
                hide_index=True,
                height=min(600, 38 * (len(df_pairs) + 1)),
            )

            # Quick insights
            top = pairs[0]
            if top["Overlap"] > 0.15:
                st.info(
                    f"🔗 Highest overlap: **{top['Fund A']}** & "
                    f"**{top['Fund B']}** hold "
                    f"**{top['Stocks in Common']}** of the same "
                    f"stocks ({top['Overlap']:.1%} overlap)."
                )
        else:
            st.info("No fund pairs to compare.")

    # --- Heatmap ---
    with tab_heatmap:
        # Controls for large fund counts
        if n > 15:
            st.markdown(
                f"*{n} funds available — use the slider to control "
                f"how many appear on the heatmap. Funds are ranked by "
                f"average overlap (most connected shown first).*"
            )
            max_show = st.slider(
                "Funds to display",
                min_value=5,
                max_value=min(n, 40),
                value=min(n, 20),
                step=5,
                key="overlap_heatmap_n",
            )
            h_names, h_matrix, _ = _select_top_connected(
                names, matrix, max_show,
            )
            if len(h_names) < n:
                st.caption(
                    f"Showing top {len(h_names)} most-connected funds "
                    f"(of {n} total)"
                )
        else:
            h_names, h_matrix = names, matrix

        fig = overlap_heatmap(h_matrix, h_names)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("How to Read the Matrix"):
            st.markdown(
                "The diagonal is masked (always 100%). Color intensity "
                "reflects overlap between each pair of funds.\n\n"
                "| Score | Meaning |\n"
                "|-------|--------|\n"
                "| **0–5%** | No meaningful overlap |\n"
                "| **5–15%** | Low — a few shared large-caps |\n"
                "| **15–30%** | Moderate — similar sector focus |\n"
                "| **30%+** | High — correlated, crowding risk |"
            )

    # --- Sankey ---
    with tab_sankey:
        st.markdown(
            "The stocks on the **right** (orange) are the positions held by "
            "the **most funds** simultaneously — these are the consensus "
            "holdings across the tracked universe. Funds on the **left** "
            "(blue) are connected to each stock they hold, with **flow "
            "width proportional to portfolio weight** (% of AUM). "
            "Thicker flows = larger positions. Hover for exact weights."
        )
        fund_lookup = {f.cik: f for f in funds_with_data}
        widely_held = compute_most_widely_held(
            all_holdings, fund_lookup, top_n=20
        )
        if widely_held:
            fig_sankey = shared_holdings_sankey(widely_held, max_stocks=15)
            st.plotly_chart(fig_sankey, use_container_width=True)
        else:
            st.info("No shared holdings data available.")

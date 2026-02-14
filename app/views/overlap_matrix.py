"""Portfolio Overlap Matrix — heatmap of fund-to-fund similarity.

Supports two similarity measures:
- **Jaccard** (name-count): what % of their combined stock universe do two funds share?
- **Cosine** (value-weighted): how correlated are their portfolio weight vectors?
  Two funds with the same top 10 at similar weights score high even if the rest differs.

Includes Sankey view of shared holdings and crowding risk flags.
"""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st

from app.components.charts import overlap_heatmap, shared_holdings_sankey
from core.aggregator import compute_most_widely_held
from core.models import Holding, Tier


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


def _compute_cosine(
    funds_with_data, all_holdings,
) -> tuple[list[str], list[list[float]], dict[str, set[str]]]:
    """Compute cosine similarity on portfolio weight vectors.

    Each fund's portfolio is a vector of position weights (value / total AUM).
    The union of all CUSIPs forms the dimensions. Cosine similarity measures
    how aligned two funds' bets are, weighting large positions more heavily.

    Returns (names, matrix, fund_cusips_dict).
    """
    # Build weight vectors per fund
    fund_weights: dict[str, dict[str, float]] = {}
    fund_cusips: dict[str, set[str]] = {}

    for fund in funds_with_data:
        holdings: list[Holding] = all_holdings.get(fund.cik, [])
        equity = [h for h in holdings if not h.is_option]
        total = sum(h.value_thousands for h in equity)
        if total == 0:
            fund_weights[fund.cik] = {}
            fund_cusips[fund.cik] = set()
            continue

        weights = {h.cusip: h.value_thousands / total for h in equity}
        fund_weights[fund.cik] = weights
        fund_cusips[fund.cik] = set(weights.keys())

    names = [f.name for f in funds_with_data]
    n = len(funds_with_data)
    matrix = [[0.0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i == j:
                matrix[i][j] = 1.0
            else:
                wi = fund_weights[funds_with_data[i].cik]
                wj = fund_weights[funds_with_data[j].cik]
                shared_cusips = set(wi.keys()) & set(wj.keys())
                if not shared_cusips:
                    matrix[i][j] = 0.0
                    continue
                # Dot product on shared dimensions
                dot = sum(wi[c] * wj[c] for c in shared_cusips)
                mag_i = math.sqrt(sum(v * v for v in wi.values()))
                mag_j = math.sqrt(sum(v * v for v in wj.values()))
                if mag_i > 0 and mag_j > 0:
                    matrix[i][j] = dot / (mag_i * mag_j)
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
    st.caption(
        "Which funds hold the same stocks? **Jaccard** = name overlap · "
        "**Cosine** = value-weighted alignment. Equity only."
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

    # Similarity mode selector
    sim_mode = st.radio(
        "Similarity Measure",
        options=["Jaccard (Name-Count)", "Cosine (Value-Weighted)"],
        horizontal=True,
        index=0,
        help=(
            "Jaccard: % of their combined stock universe that both funds hold. "
            "Cosine: how correlated their portfolio weight vectors are "
            "(weights large positions more heavily)."
        ),
    )
    use_cosine = "Cosine" in sim_mode

    # Compute similarity matrix
    if use_cosine:
        names, matrix, fund_cusips = _compute_cosine(funds_with_data, all_holdings)
    else:
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
        measure_label = "Cosine Similarity" if use_cosine else "Jaccard Overlap"
        st.markdown(f"### Most Overlapping Fund Pairs ({measure_label})")
        if use_cosine:
            st.caption(
                "Ranked by cosine similarity on weight vectors. "
                "Higher = similar-sized bets on the same stocks → correlated drawdown risk."
            )
        else:
            st.caption(
                "All fund pairs ranked by Jaccard overlap. "
                "Overlap = % of combined universe shared · Stocks in Common = raw count."
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
                    "Similarity": matrix[i][j],
                    "Stocks in Common": shared,
                    "A Holdings": total_a,
                    "B Holdings": total_b,
                })
        pairs.sort(key=lambda p: p["Similarity"], reverse=True)

        # Format for display
        df_pairs = pd.DataFrame(pairs[:50])
        if not df_pairs.empty:
            df_pairs["Similarity"] = df_pairs["Similarity"].apply(
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
            if top["Similarity"] > 0.15:
                st.info(
                    f"🔗 Highest similarity: **{top['Fund A']}** & "
                    f"**{top['Fund B']}** — "
                    f"**{top['Stocks in Common']}** shared stocks "
                    f"({top['Similarity']:.1%} {measure_label.lower()})."
                )
        else:
            st.info("No fund pairs to compare.")

    # --- Heatmap ---
    with tab_heatmap:
        # Controls for large fund counts
        if n > 15:
            st.caption(
                f"{n} funds available — slider controls how many appear. "
                f"Ranked by average similarity (most connected first)."
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

        title_suffix = " (Cosine)" if use_cosine else " (Jaccard)"
        fig = overlap_heatmap(h_matrix, h_names, title_suffix=title_suffix)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("How to Read the Matrix"):
            if use_cosine:
                st.markdown(
                    "**Cosine similarity** measures how aligned two funds' "
                    "portfolio weight vectors are. A score of 1.0 means "
                    "identical portfolios; 0.0 means no overlap.\n\n"
                    "| Score | Meaning |\n"
                    "|-------|--------|\n"
                    "| **0–5%** | No meaningful correlation |\n"
                    "| **5–15%** | Low — a few shared bets |\n"
                    "| **15–30%** | Moderate — correlated sector tilts |\n"
                    "| **30%+** | High — correlated drawdown risk |"
                )
            else:
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
        st.caption(
            "Right (orange) = most widely held stocks · "
            "Left (blue) = funds. Flow width = portfolio weight "
            "(% of AUM). Thicker = larger position."
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

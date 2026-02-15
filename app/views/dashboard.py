"""Dashboard — visual summary of the quarter at a glance.

Shows top findings, fund summary table, AUM changes, top position moves,
activity heatmap, and concentration metrics across all tracked funds.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from app.components.charts import (
    fund_activity_heatmap,
    top_moves_chart,
)
from core.aggregator import compute_top_findings
from core.models import CrossFundSignals, FundBaseline, FundDiff
from data.performance_provider import fetch_price_performance, format_price_tag


def _fmt_aum(thousands: int) -> str:
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000_000:
        return f"${dollars / 1_000_000_000_000:.1f}T"
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    return f"${dollars / 1_000_000:.0f}M"


_FINDING_ICONS = {
    "crowded_buy": "🟢",
    "crowded_sell": "🔴",
    "divergence": "🔀",
    "concentration": "📊",
    "new_position": "🆕",
    "exit": "🚪",
    "activity": "⚡",
}


def _esc(text: str) -> str:
    """Escape dollar signs for Streamlit markdown (prevents LaTeX rendering)."""
    return text.replace("$", r"\$")


def _get_findings_prices(findings: list[dict]) -> dict[str, dict]:
    """Fetch price performance for tickers in findings (cached, ~5 calls max)."""
    cache = st.session_state.get("cache")
    if not cache:
        return {}

    tickers = [
        f["ticker"] for f in findings
        if f.get("ticker") is not None
    ]
    if not tickers:
        return {}

    # Deduplicate
    tickers = list(dict.fromkeys(tickers))

    try:
        return fetch_price_performance(tickers, cache)
    except Exception:
        return {}


def _render_findings(
    diffs: list[FundDiff],
    signals: CrossFundSignals | None,
    baselines: dict[str, FundBaseline] | None = None,
) -> None:
    """Render the Top 5 Findings box at the top of the Dashboard."""
    findings = compute_top_findings(
        diffs, signals, n=5, baselines=baselines,
    )
    if not findings:
        return

    # Fetch price performance for tickers mentioned in findings
    price_perf = _get_findings_prices(findings)

    st.markdown("### 🔍 Top Findings")
    for f in findings:
        icon = _FINDING_ICONS.get(f["category"], "•")
        headline = _esc(f["headline"])
        detail = _esc(f["detail"])

        # Add price tag if available
        ticker = f.get("ticker")
        price_tag = ""
        if ticker and ticker in price_perf:
            tag = format_price_tag(price_perf[ticker])
            price_tag = f"  \n&emsp;📈 *{_esc(tag)}*"

        st.markdown(f"{icon} **{headline}** — {detail}{price_tag}")

    with st.expander("ℹ️ How findings are ranked"):
        n_baselined = len(baselines) if baselines else 0
        st.markdown(
            "Ranked by **surprise value** — each fund's activity is "
            "compared against its own historical baseline.\n\n"
            "| Signal type | Scoring method |\n"
            "|---|---|\n"
            "| Cross-fund (consensus, crowded, divergences) | Fund count |\n"
            "| Per-fund (activity, new positions, concentration) "
            "| Z-score vs. fund history |\n\n"
            "**Z-score multipliers:** "
            "normal range → 0.5× · above normal → 1.0× · "
            "very unusual (2σ+) → 1.6×\n\n"
            f"**{n_baselined}** of {len(diffs)} funds have enough "
            "history (3+ quarters) for baseline scoring."
        )

    st.divider()


def _render_fund_summary_table(diffs: list[FundDiff]) -> None:
    """Render a sortable summary table with key metrics per fund."""
    rows = []
    for d in sorted(diffs, key=lambda x: x.current_aum_thousands, reverse=True):
        sig_adds = sum(1 for p in d.added_positions if p.is_significant_add)
        sig_trims = sum(1 for p in d.trimmed_positions if p.is_significant_trim)
        rows.append({
            "Fund": d.fund.name,
            "Tier": d.fund.tier.value,
            "AUM": _fmt_aum(d.current_aum_thousands),
            "New": len(d.new_positions),
            "Exits": len(d.exited_positions),
            "Adds>50%": sig_adds,
            "Trims>60%": sig_trims,
            "Top-10 Wt": f"{d.current_top10_weight:.0%}",
            "HHI Δ bps": f"{d.hhi_change * 10000:+.0f}",
            "Filing Lag": f"{d.filing_lag_days}d",
        })

    df = pd.DataFrame(rows)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400, 38 * (len(rows) + 1)),
        column_config={
            "Fund": st.column_config.TextColumn(
                "Fund", width="large", pinned=True,
            ),
        },
    )


def _render_concentration_table(diffs: list[FundDiff]) -> None:
    """Sortable table of fund concentration metrics."""
    rows = []
    for d in diffs:
        hhi_bps = d.hhi_change * 10000
        if hhi_bps > 10:
            direction = "📈 Concentrating"
        elif hhi_bps < -10:
            direction = "📉 Diversifying"
        else:
            direction = "— Flat"
        rows.append({
            "Fund": d.fund.name,
            "Top-10 Wt": f"{d.current_top10_weight:.0%}",
            "Prior Top-10": f"{d.prior_top10_weight:.0%}",
            "Δ Top-10": f"{(d.current_top10_weight - d.prior_top10_weight) * 100:+.1f}pp",
            "HHI Δ bps": f"{hhi_bps:+.0f}",
            "Direction": direction,
        })

    # Sort by absolute HHI change descending
    rows.sort(key=lambda r: abs(float(r["HHI Δ bps"])), reverse=True)
    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400, 38 * (len(rows) + 1)),
    )


def render() -> None:
    """Render the Dashboard page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>1</span>"
        "Dashboard</h2>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Signals · fund metrics · activity · concentration — all in one view."
    )

    quarter = st.session_state.get("selected_quarter")
    fund_diffs: dict[date, list[FundDiff]] = st.session_state.get(
        "fund_diffs", {}
    )

    if not quarter or quarter not in fund_diffs:
        st.info(
            "No analysis data available. Fetch data and run the analysis "
            "using the sidebar workflow."
        )
        return

    diffs = fund_diffs[quarter]
    if not diffs:
        st.warning("No fund diffs available for this quarter.")
        return

    # Coverage warning
    n_watchlist = len(st.session_state.get("watchlist", []))
    if len(diffs) < n_watchlist * 0.5:
        st.warning(
            f"Only **{len(diffs)}** of {n_watchlist} funds have "
            f"QoQ data for this quarter. Some funds may lack a prior "
            f"quarter for comparison."
        )

    # ---------------------------------------------------------------
    # Row 0: Top 5 Findings
    # ---------------------------------------------------------------
    signals: CrossFundSignals | None = st.session_state.get(
        "cross_signals", {}
    ).get(quarter)
    baselines: dict[str, FundBaseline] | None = st.session_state.get(
        "fund_baselines", {}
    ).get(quarter)
    _render_findings(diffs, signals, baselines=baselines)

    # ---------------------------------------------------------------
    # Row 1: Summary metrics
    # ---------------------------------------------------------------
    total_aum = sum(d.current_aum_thousands for d in diffs)
    avg_lag = sum(d.filing_lag_days for d in diffs) / len(diffs)
    total_new = sum(len(d.new_positions) for d in diffs)
    total_exits = sum(len(d.exited_positions) for d in diffs)

    # Compact summary row — smaller font via custom CSS
    st.markdown(
        "<style>"
        "[data-testid='stMetric'] [data-testid='stMetricValue'] "
        "{ font-size: 1.4rem; }"
        "</style>",
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("AUM Represented", _fmt_aum(total_aum))
    c2.metric("Funds Analyzed", len(diffs))
    c3.metric("Avg Filing Lag", f"{avg_lag:.0f} days")
    c4.metric("New / Exits", f"{total_new} / {total_exits}")

    st.divider()

    # ---------------------------------------------------------------
    # Row 2: Fund Summary Table (full width)
    # ---------------------------------------------------------------
    st.markdown("#### Fund Summary")
    st.caption(
        "Sorted by AUM. Click any column header to re-sort. "
        "HHI Δ: + = concentrating, − = diversifying."
    )
    _render_fund_summary_table(diffs)

    # ---------------------------------------------------------------
    # Row 3: Top Position Moves (full width)
    # ---------------------------------------------------------------
    st.markdown("#### Top Position Moves")
    st.caption(
        "Ranked by weight change, not dollar size. "
        "🟢 New · 🔴 Exit · light green = add >50% · light red = trim >60%."
    )
    st.plotly_chart(
        top_moves_chart(diffs), use_container_width=True
    )

    # ---------------------------------------------------------------
    # Row 5: Activity Heatmap (full width)
    # ---------------------------------------------------------------
    st.markdown("#### Activity Heatmap")
    st.caption("Darker = more activity. Compare rows to spot active vs. quiet funds.")
    q_num = (quarter.month - 1) // 3 + 1
    q_label = f"Q{q_num} {quarter.year}"
    st.plotly_chart(
        fund_activity_heatmap(diffs, quarter_label=q_label),
        use_container_width=True,
    )

    # ---------------------------------------------------------------
    # Row 6: Concentration Table (full width)
    # ---------------------------------------------------------------
    st.markdown("#### Portfolio Concentration")
    st.caption(
        "Top-10 weight = % of AUM in 10 largest positions. "
        "HHI Δ (bps): 📈 positive = concentrating, 📉 negative = diversifying."
    )
    _render_concentration_table(diffs)

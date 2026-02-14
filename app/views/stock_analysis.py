"""Stock Analysis — single-stock view across all tracked funds.

Enter a ticker to see every fund's position in that stock: who holds it,
who initiated or exited this quarter, share changes, portfolio weight,
and QoQ dollar-value moves.  All data comes from already-analyzed
FundDiff objects in session state — no additional API calls.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from core.models import FundDiff, PositionChangeType


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


def _esc(text: str) -> str:
    """Escape dollar signs for Streamlit markdown."""
    return text.replace("$", r"\$")


_ACTION_LABELS = {
    PositionChangeType.NEW: "🟢 New",
    PositionChangeType.EXITED: "🔴 Exit",
    PositionChangeType.ADDED: "📈 Added",
    PositionChangeType.TRIMMED: "📉 Trimmed",
    PositionChangeType.UNCHANGED: "— Held",
}


def _find_matches(
    diffs: list[FundDiff], query: str,
) -> list[dict]:
    """Search all position changes across funds for a ticker/name match.

    Returns a list of dicts with fund + position details.
    """
    q = query.strip().upper()
    results: list[dict] = []

    for d in diffs:
        for p in d.all_changes + d.unchanged_positions:
            ticker_match = p.ticker and p.ticker.upper() == q
            name_match = q in p.issuer_name.upper()

            if not (ticker_match or name_match):
                continue

            results.append({
                "fund": d.fund.name,
                "tier": d.fund.tier.value,
                "ticker": p.ticker or "—",
                "issuer": p.issuer_name,
                "cusip": p.cusip,
                "action": _ACTION_LABELS.get(p.change_type, p.change_type.value),
                "change_type": p.change_type,
                "current_val_k": p.current_value_thousands,
                "prior_val_k": p.prior_value_thousands,
                "value_change_k": p.value_change_thousands,
                "shares_change_pct": p.shares_change_pct,
                "current_weight": p.current_weight_pct,
                "prior_weight": p.prior_weight_pct,
                "weight_change": p.weight_change_pct,
                "current_shares": p.current_shares,
                "prior_shares": p.prior_shares,
                "put_call": p.put_call,
            })

    return results


def render() -> None:
    """Render the Stock Analysis page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>2</span>"
        "Stock Analysis</h2>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Search a ticker across all funds — who's buying, selling, initiating, or exiting."
    )

    quarter: date | None = st.session_state.get("selected_quarter")
    fund_diffs: dict[date, list[FundDiff]] = st.session_state.get(
        "fund_diffs", {},
    )

    if not quarter or quarter not in fund_diffs:
        st.info("Run an analysis first using the sidebar.")
        return

    diffs = fund_diffs[quarter]
    q_num = (quarter.month - 1) // 3 + 1
    q_label = f"Q{q_num} {quarter.year}"

    # --- Ticker input ---
    query = st.text_input(
        "Ticker or company name",
        placeholder="e.g. AAPL, NVIDIA, META …",
        key="stock_analysis_input",
    )

    if not query or len(query.strip()) < 2:
        st.caption(
            "Type a ticker symbol or part of a company name to search "
            f"across all {len(diffs)} analyzed funds for {q_label}."
        )
        return

    # Easter egg
    if query.strip().lower() == "garcia":
        st.markdown(
            "🎸 [What a long, strange trip it's been…]"
            "(https://en.wikipedia.org/wiki/Jerry_Garcia)",
        )
        return

    matches = _find_matches(diffs, query)

    if not matches:
        st.warning(
            f"No funds hold a position matching '{query.strip()}' "
            f"in {q_label}."
        )
        return

    # Resolve display name from first match
    ticker = matches[0]["ticker"]
    issuer = matches[0]["issuer"]
    display_name = f"**{ticker}**" if ticker != "—" else f"**{issuer}**"

    # --- Summary metrics ---
    n_funds_holding = len(matches)
    equity_matches = [m for m in matches if m["put_call"] is None]
    option_matches = [m for m in matches if m["put_call"] is not None]

    new_count = sum(
        1 for m in matches
        if m["change_type"] == PositionChangeType.NEW
    )
    exit_count = sum(
        1 for m in matches
        if m["change_type"] == PositionChangeType.EXITED
    )
    add_count = sum(
        1 for m in matches
        if m["change_type"] == PositionChangeType.ADDED
    )
    trim_count = sum(
        1 for m in matches
        if m["change_type"] == PositionChangeType.TRIMMED
    )
    total_current_val = sum(m["current_val_k"] for m in matches)

    st.markdown(f"### {_esc(display_name)}  ·  {q_label}")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Funds Holding", n_funds_holding)
    c2.metric("Initiated", new_count)
    c3.metric("Exited", exit_count)
    c4.metric("Added To", add_count)
    c5.metric("Trimmed", trim_count)

    st.caption(
        f"Aggregate 13F-reported value across all funds: "
        f"**{_esc(_fmt_val(total_current_val))}**"
    )

    # Float ownership context (if sector enrichment available)
    sector_data = st.session_state.get("sector_data", {}).get(quarter, {})
    if ticker != "—" and ticker in sector_data:
        info = sector_data[ticker]
        float_shares = info.get("float_shares")
        if float_shares and float_shares > 0:
            agg_shares = sum(m["current_shares"] for m in matches)
            if agg_shares > 0:
                float_pct = agg_shares / float_shares * 100
                color = "🔴" if float_pct >= 10 else "🟡" if float_pct >= 5 else "🟢"
                st.caption(
                    f"**Float ownership**: {color} tracked funds hold "
                    f"**{float_pct:.1f}%** of the public float "
                    f"({agg_shares:,.0f} of {float_shares:,.0f} shares)"
                )

    st.divider()

    # --- Detail table ---
    st.markdown("#### Fund-by-Fund Breakdown")

    rows = []
    for m in sorted(
        matches, key=lambda x: x["current_val_k"], reverse=True,
    ):
        opt_tag = ""
        if m["put_call"]:
            opt_tag = f" [{m['put_call']}]"

        rows.append({
            "Fund": m["fund"],
            "Tier": m["tier"],
            "Action": m["action"],
            "Value": _fmt_val(m["current_val_k"]),
            "Prior Value": _fmt_val(m["prior_val_k"]),
            "Δ Value": _fmt_val(m["value_change_k"]),
            "Shares Δ%": (
                f"{m['shares_change_pct']:+.0%}"
                if m["change_type"] not in (
                    PositionChangeType.NEW,
                    PositionChangeType.EXITED,
                )
                else m["action"].split()[-1]  # "New" or "Exit"
            ),
            "Weight%": f"{m['current_weight']:.2f}%",
            "Type": f"Equity{opt_tag}" if not m["put_call"] else opt_tag.strip(),
        })

    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(500, 38 * (len(rows) + 1)),
    )

    # --- Sentiment summary ---
    st.divider()
    st.markdown("#### Signal Summary")

    net = new_count + add_count - exit_count - trim_count
    n_buy = new_count + add_count
    n_sell = exit_count + trim_count
    if net > 0:
        sentiment = (
            f"🟢 **Net bullish** — {n_buy} funds buying "
            f"vs. {n_sell} selling"
        )
    elif net < 0:
        sentiment = (
            f"🔴 **Net bearish** — {n_sell} funds selling "
            f"vs. {n_buy} buying"
        )
    else:
        sentiment = (
            "⚪ **Neutral** — buying and selling "
            "activity is balanced"
        )

    st.markdown(sentiment)

    if new_count > 0:
        initiators = [
            m["fund"] for m in matches
            if m["change_type"] == PositionChangeType.NEW
        ]
        st.markdown(f"**Initiated by:** {', '.join(initiators)}")

    if exit_count > 0:
        exiters = [
            m["fund"] for m in matches
            if m["change_type"] == PositionChangeType.EXITED
        ]
        st.markdown(f"**Exited by:** {', '.join(exiters)}")

    if add_count > 0:
        adders = [
            m["fund"] for m in matches
            if m["change_type"] == PositionChangeType.ADDED
        ]
        st.markdown(f"**Added by:** {', '.join(adders)}")

    if trim_count > 0:
        trimmers = [
            m["fund"] for m in matches
            if m["change_type"] == PositionChangeType.TRIMMED
        ]
        st.markdown(f"**Trimmed by:** {', '.join(trimmers)}")

    # Show equity vs options breakdown if both exist
    if equity_matches and option_matches:
        st.divider()
        st.caption(
            f"{len(equity_matches)} equity position(s), "
            f"{len(option_matches)} options position(s) matched."
        )

"""Plotly chart builders for the Streamlit app."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from core.models import CrowdedTrade, FundDiff, Holding


def crowded_trades_bar_chart(crowded: list[CrowdedTrade], max_items: int = 20) -> go.Figure:
    """Horizontal bar chart of crowded trades by net fund sentiment."""
    if not crowded:
        return go.Figure()

    items = crowded[:max_items]
    labels = [ct.display_label for ct in items]
    buying = [ct.total_funds_buying for ct in items]
    selling = [-ct.total_funds_selling for ct in items]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=labels,
        x=buying,
        name="Buying",
        orientation="h",
        marker_color="#2ecc71",
    ))
    fig.add_trace(go.Bar(
        y=labels,
        x=selling,
        name="Selling",
        orientation="h",
        marker_color="#e74c3c",
    ))
    fig.update_layout(
        barmode="relative",
        title="Crowded Trades — Fund Count",
        xaxis_title="Number of Funds",
        yaxis=dict(autorange="reversed"),
        height=max(400, len(items) * 30),
        margin=dict(l=150),
    )
    return fig


def sector_flows_chart(sector_flows: dict[str, dict[str, int]]) -> go.Figure:
    """Bar chart of sector-level fund flows."""
    if not sector_flows:
        return go.Figure()

    # Filter out Unknown, sort by absolute net
    sectors = {
        k: v for k, v in sector_flows.items() if k != "Unknown"
    }
    sorted_sectors = sorted(sectors.items(), key=lambda x: x[1]["net"])

    labels = [s[0] for s in sorted_sectors]
    nets = [s[1]["net"] for s in sorted_sectors]
    colors = ["#2ecc71" if n >= 0 else "#e74c3c" for n in nets]

    fig = go.Figure(go.Bar(
        y=labels,
        x=nets,
        orientation="h",
        marker_color=colors,
    ))
    fig.update_layout(
        title="Sector Flows — Net Fund Activity",
        xaxis_title="Net Funds (Buying - Selling)",
        height=max(400, len(labels) * 25),
        margin=dict(l=180),
    )
    return fig


def overlap_heatmap(
    overlap_matrix: list[list[float]],
    fund_names: list[str],
    title_suffix: str = "",
) -> go.Figure:
    """Plotly heatmap of portfolio overlap between fund pairs.

    Masks the diagonal (always 1.0) so the color scale highlights
    actual inter-fund overlap.  Adaptive sizing for readability.
    """
    n = len(fund_names)

    # Mask diagonal with None so it doesn't dominate the color range
    masked = [
        [None if i == j else overlap_matrix[i][j] for j in range(n)]
        for i in range(n)
    ]
    # Text keeps the diagonal label for hover context
    text = [
        ["—" if i == j else f"{overlap_matrix[i][j]:.0%}" for j in range(n)]
        for i in range(n)
    ]

    # Adaptive text / sizing
    if n <= 12:
        text_size, show_text, cell_px = 13, True, 48
    elif n <= 20:
        text_size, show_text, cell_px = 10, True, 36
    elif n <= 30:
        text_size, show_text, cell_px = 8, True, 30
    else:
        text_size, show_text, cell_px = 7, False, 24

    # Find a reasonable zmax: use the 95th-percentile off-diagonal value
    # so sparse matrices don't wash out all color
    off_diag = [
        overlap_matrix[i][j]
        for i in range(n) for j in range(n) if i != j
    ]
    if off_diag:
        off_diag_sorted = sorted(off_diag)
        p95 = off_diag_sorted[int(len(off_diag_sorted) * 0.95)]
        zmax = max(p95 * 1.2, 0.10)  # At least 10% to avoid over-saturation
    else:
        zmax = 1.0

    fig = go.Figure(data=go.Heatmap(
        z=masked,
        x=fund_names,
        y=fund_names,
        colorscale=[
            [0.0, "#f7fbff"],
            [0.25, "#c6dbef"],
            [0.50, "#6baed6"],
            [0.75, "#2171b5"],
            [1.0, "#08306b"],
        ],
        zmin=0,
        zmax=zmax,
        text=text,
        texttemplate="%{text}" if show_text else "",
        textfont={"size": text_size},
        hovertemplate="%{y} × %{x}<br>Overlap: %{text}<extra></extra>",
        colorbar=dict(title="Overlap", tickformat=".0%"),
    ))

    size = max(550, n * cell_px + 160)
    tick_size = 11 if n <= 15 else 10 if n <= 25 else 9
    fig.update_layout(
        title=f"Portfolio Similarity{title_suffix}",
        height=size,
        width=size + 60,
        xaxis=dict(
            tickangle=45, tickfont=dict(size=tick_size),
            side="bottom",
        ),
        yaxis=dict(
            tickfont=dict(size=tick_size),
            autorange="reversed",
        ),
        margin=dict(l=180, r=40, t=60, b=160),
        template="plotly_white",
    )
    return fig


def concentration_chart(fund_diffs: list[FundDiff]) -> go.Figure:
    """Scatter plot of funds by concentration change."""
    if not fund_diffs:
        return go.Figure()

    names = [d.fund.name for d in fund_diffs]
    current_top10 = [d.current_top10_weight * 100 for d in fund_diffs]
    hhi_changes = [d.hhi_change * 10000 for d in fund_diffs]  # basis points

    # Assign alternating text positions to prevent label overlap
    positions = [
        "top center", "bottom center", "top right", "bottom left",
        "top left", "bottom right", "middle right", "middle left",
    ]
    text_pos = [positions[i % len(positions)] for i in range(len(names))]

    fig = go.Figure(go.Scatter(
        x=current_top10,
        y=hhi_changes,
        text=names,
        mode="markers+text",
        textposition=text_pos,
        textfont={"size": 11},
        marker=dict(
            size=12,
            color=hhi_changes,
            colorscale="RdYlGn_r",
            showscale=True,
            colorbar=dict(title="HHI Δ (bps)"),
        ),
    ))
    fig.update_layout(
        title="Fund Concentration: Top-10 Weight vs. HHI Change",
        xaxis_title="Current Top-10 Weight (%)",
        yaxis_title="HHI Change (bps, + = more concentrated)",
        height=500,
        **_LAYOUT_DEFAULTS,
    )
    return fig


# ---------------------------------------------------------------------------
# Color constants
# ---------------------------------------------------------------------------

TIER_COLORS = {
    "A": "#3498db",
    "B": "#2ecc71",
    "C": "#e67e22",
    "D": "#9b59b6",
    "E": "#e74c3c",
}

_LAYOUT_DEFAULTS = dict(
    template="plotly_white",
    margin=dict(l=60, r=20, t=50, b=40),
)


def _fmt_aum(thousands: int) -> str:
    """Format $thousands into human-readable."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    return f"${dollars / 1_000:.0f}K"


# ---------------------------------------------------------------------------
# Chart 1: Fund Scorecard Bar Chart (replaces treemap)
# ---------------------------------------------------------------------------


def fund_scorecard_bars(fund_diffs: list[FundDiff]) -> go.Figure:
    """Horizontal bar chart of QoQ AUM change % by fund.

    Every fund gets equal visual weight regardless of absolute AUM,
    solving the problem where one mega-fund dominates a treemap.
    Bar color: green = grew, red = shrank. Label shows absolute AUM.
    """
    if not fund_diffs:
        return go.Figure()

    # Sort by QoQ % change descending
    sorted_diffs = sorted(
        fund_diffs, key=lambda d: d.aum_change_pct
    )

    names = [d.fund.name for d in sorted_diffs]
    pcts = [d.aum_change_pct * 100 for d in sorted_diffs]
    colors = ["#2ecc71" if p >= 0 else "#e74c3c" for p in pcts]
    hover = [
        f"<b>{d.fund.name}</b> (Tier {d.fund.tier.value})<br>"
        f"AUM: {_fmt_aum(d.current_aum_thousands)}<br>"
        f"QoQ: {d.aum_change_pct:+.1%}<br>"
        f"New: {len(d.new_positions)} | Exits: {len(d.exited_positions)}"
        for d in sorted_diffs
    ]

    fig = go.Figure(go.Bar(
        y=names,
        x=pcts,
        orientation="h",
        marker_color=colors,
        text=[
            f"{p:+.1f}%  ({_fmt_aum(d.current_aum_thousands)})"
            for p, d in zip(pcts, sorted_diffs)
        ],
        textposition="outside",
        hovertext=hover,
        hoverinfo="text",
    ))
    fig.add_vline(x=0, line_color="gray", line_width=1)
    fig.update_layout(
        title="Fund AUM Change — QoQ %",
        xaxis_title="Quarter-over-Quarter Change (%)",
        height=max(350, len(names) * 40),
        margin=dict(l=180, r=120, t=50, b=40),
        **{k: v for k, v in _LAYOUT_DEFAULTS.items() if k != "margin"},
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 1b: Top Position Moves across all funds
# ---------------------------------------------------------------------------


def top_moves_chart(
    fund_diffs: list[FundDiff], max_items: int = 25,
) -> go.Figure:
    """Horizontal bar chart of the largest position moves across ALL funds.

    Includes new initiations, exits, significant adds (50%+), and significant
    trims (60%+). Ranked by portfolio weight change magnitude. This is the
    comprehensive summary of every notable move in the quarter.
    """
    if not fund_diffs:
        return go.Figure()

    # Color map: action → (bar color, positive/negative direction)
    ACTION_COLORS = {
        "New": "#2ecc71",       # bright green
        "Add >50%": "#82e0aa",  # light green
        "Trim >60%": "#f1948a", # light red
        "Exit": "#e74c3c",      # bright red
    }

    moves: list[dict] = []
    for d in fund_diffs:
        for p in d.new_positions[:8]:
            moves.append({
                "label": p.display_label,
                "fund": d.fund.name,
                "weight": p.current_weight_pct,
                "value_k": p.current_value_thousands,
                "action": "New",
                "color": ACTION_COLORS["New"],
            })
        for p in d.exited_positions[:8]:
            moves.append({
                "label": p.display_label,
                "fund": d.fund.name,
                "weight": -p.prior_weight_pct,
                "value_k": p.prior_value_thousands,
                "action": "Exit",
                "color": ACTION_COLORS["Exit"],
            })
        for p in d.added_positions:
            if p.is_significant_add:
                moves.append({
                    "label": p.display_label,
                    "fund": d.fund.name,
                    "weight": p.weight_change_pct,
                    "value_k": p.current_value_thousands,
                    "action": "Add >50%",
                    "color": ACTION_COLORS["Add >50%"],
                })
        for p in d.trimmed_positions:
            if p.is_significant_trim:
                moves.append({
                    "label": p.display_label,
                    "fund": d.fund.name,
                    "weight": p.weight_change_pct,
                    "value_k": p.current_value_thousands,
                    "action": "Trim >60%",
                    "color": ACTION_COLORS["Trim >60%"],
                })

    if not moves:
        return go.Figure()

    # Sort by absolute weight and take top N
    moves.sort(key=lambda m: abs(m["weight"]), reverse=True)
    moves = moves[:max_items]
    moves.reverse()  # Bottom-to-top for horizontal bar

    # Use ticker-style labels; shorten fund name to first word
    labels = [
        f"{m['label']} — {m['fund'].split()[0]}" for m in moves
    ]
    weights = [m["weight"] for m in moves]
    colors = [m["color"] for m in moves]
    hover = [
        f"<b>{m['label']}</b><br>"
        f"Fund: {m['fund']}<br>"
        f"Action: {m['action']}<br>"
        f"Weight Δ: {abs(m['weight']):.2f}%<br>"
        f"Value: {_fmt_aum(m['value_k'])}"
        for m in moves
    ]

    fig = go.Figure(go.Bar(
        y=labels,
        x=weights,
        orientation="h",
        marker_color=colors,
        text=[f"{w:+.2f}%" for w in weights],
        textposition="outside",
        hovertext=hover,
        hoverinfo="text",
    ))
    fig.add_vline(x=0, line_color="gray", line_width=1)
    fig.update_layout(
        title=(
            "Top Position Moves — All Funds"
            "<br><sup>New initiations, exits, adds >50%, trims >60% "
            "— ranked by portfolio weight</sup>"
        ),
        xaxis_title="← Sells / Trims  |  Buys / Adds →  (weight %)",
        height=max(450, len(moves) * 28),
        margin=dict(l=200, r=80, t=65, b=40),
        **{k: v for k, v in _LAYOUT_DEFAULTS.items() if k != "margin"},
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 2: Fund Activity Heatmap
# ---------------------------------------------------------------------------


def fund_activity_heatmap(
    fund_diffs: list[FundDiff], quarter_label: str = "",
) -> go.Figure:
    """Heatmap of position change activity by fund and action type."""
    if not fund_diffs:
        return go.Figure()

    # Compute activity counts
    data = []
    for d in fund_diffs:
        sig_adds = sum(1 for p in d.added_positions if p.is_significant_add)
        sig_trims = sum(
            1 for p in d.trimmed_positions if p.is_significant_trim
        )
        total = (
            len(d.new_positions) + len(d.exited_positions)
            + sig_adds + sig_trims
        )
        data.append({
            "name": d.fund.name,
            "new": len(d.new_positions),
            "exited": len(d.exited_positions),
            "sig_adds": sig_adds,
            "sig_trims": sig_trims,
            "total": total,
        })

    # Sort by total activity descending
    data.sort(key=lambda x: x["total"], reverse=True)

    # Filter out low-activity funds (< 5 total moves) to reduce noise
    min_activity = 5
    data = [r for r in data if r["total"] >= min_activity]
    if not data:
        # Fallback: show top 10 if threshold filtered everything
        data.sort(key=lambda x: x["total"], reverse=True)
        data = data[:10]

    names = [r["name"] for r in data]
    cols = ["New", "Exited", "Adds >50%", "Trims >60%"]
    z = [
        [r["new"], r["exited"], r["sig_adds"], r["sig_trims"]]
        for r in data
    ]

    n_rows = len(names)
    row_height = 45  # px per row — enough to read numbers clearly

    fig = go.Figure(data=go.Heatmap(
        z=z,
        x=cols,
        y=names,
        colorscale="OrRd",
        text=[[str(v) for v in row] for row in z],
        texttemplate="%{text}",
        textfont={"size": 12},
    ))
    fig.update_layout(
        title=dict(
            text=f"Fund Activity — {quarter_label}" if quarter_label
            else "Fund Activity",
            y=0.98,
        ),
        height=n_rows * row_height + 100,
        yaxis=dict(
            autorange="reversed",
            tickfont=dict(size=11),
        ),
        xaxis=dict(side="top", tickfont=dict(size=11)),
        margin=dict(l=220, r=20, t=70, b=20),
        template="plotly_white",
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 3: AUM Change Waterfall
# ---------------------------------------------------------------------------


def aum_waterfall(
    fund_diffs: list[FundDiff], max_funds: int = 15
) -> go.Figure:
    """Waterfall chart of aggregate AUM changes across funds."""
    if not fund_diffs:
        return go.Figure()

    total_prior = sum(d.prior_aum_thousands for d in fund_diffs)
    total_current = sum(d.current_aum_thousands for d in fund_diffs)

    # Compute per-fund AUM change and sort by magnitude
    changes = [
        (d.fund.name, d.current_aum_thousands - d.prior_aum_thousands)
        for d in fund_diffs
    ]
    changes.sort(key=lambda x: abs(x[1]), reverse=True)

    # Show top N individually, aggregate the rest
    shown = changes[:max_funds]
    rest = changes[max_funds:]
    rest_total = sum(c[1] for c in rest)

    x_labels = ["Prior Quarter Total"]
    y_values = [total_prior * 1000]  # Convert to dollars
    measures = ["absolute"]

    for name, change in shown:
        x_labels.append(name)
        y_values.append(change * 1000)
        measures.append("relative")

    if rest:
        x_labels.append(f"Other ({len(rest)} funds)")
        y_values.append(rest_total * 1000)
        measures.append("relative")

    x_labels.append("Current Quarter Total")
    y_values.append(total_current * 1000)
    measures.append("total")

    fig = go.Figure(go.Waterfall(
        x=x_labels,
        y=y_values,
        measure=measures,
        increasing=dict(marker_color="#2ecc71"),
        decreasing=dict(marker_color="#e74c3c"),
        totals=dict(marker_color="#3498db"),
        textposition="outside",
        text=[_fmt_aum(v // 1000) for v in y_values],
    ))
    fig.update_layout(
        title="Aggregate AUM Change",
        yaxis_title="AUM ($)",
        height=500,
        xaxis=dict(tickangle=45, tickfont=dict(size=9)),
        **_LAYOUT_DEFAULTS,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 4: New Position Conviction Scatter
# ---------------------------------------------------------------------------


def new_position_conviction_scatter(
    fund_diffs: list[FundDiff],
) -> go.Figure:
    """Scatter: # new positions vs. avg weight of new positions per fund."""
    if not fund_diffs:
        return go.Figure()

    names = []
    x_counts: list[int] = []
    y_weights: list[float] = []
    sizes: list[float] = []
    tier_colors: list[str] = []

    for d in fund_diffs:
        n_new = len(d.new_positions)
        if n_new == 0:
            continue
        avg_wt = sum(
            p.current_weight_pct for p in d.new_positions
        ) / n_new
        total_val = sum(
            p.current_value_thousands for p in d.new_positions
        )

        names.append(d.fund.name)
        x_counts.append(n_new)
        y_weights.append(avg_wt)
        # Scale marker size: min 8, max 50
        sizes.append(max(8, min(50, total_val / 500_000)))
        tier_colors.append(
            TIER_COLORS.get(d.fund.tier.value, "#888888")
        )

    if not names:
        return go.Figure()

    fig = go.Figure(go.Scatter(
        x=x_counts,
        y=y_weights,
        text=names,
        mode="markers+text",
        textposition="top center",
        textfont=dict(size=8),
        marker=dict(
            size=sizes,
            color=tier_colors,
            line=dict(width=1, color="white"),
        ),
        hovertemplate=(
            "<b>%{text}</b><br>"
            "New positions: %{x}<br>"
            "Avg weight: %{y:.2f}%<br>"
            "<extra></extra>"
        ),
    ))

    # Add quadrant reference lines at medians
    if x_counts:
        med_x = sorted(x_counts)[len(x_counts) // 2]
        med_y = sorted(y_weights)[len(y_weights) // 2]
        fig.add_hline(
            y=med_y, line_dash="dot",
            line_color="gray", opacity=0.5,
        )
        fig.add_vline(
            x=med_x, line_dash="dot",
            line_color="gray", opacity=0.5,
        )

    fig.update_layout(
        title="New Position Conviction",
        xaxis_title="# New Positions",
        yaxis_title="Avg Weight of New Positions (%)",
        height=500,
        **_LAYOUT_DEFAULTS,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 5: Crowded Trade Dot Plot
# ---------------------------------------------------------------------------


def crowded_trade_dot_plot(
    crowded: list[CrowdedTrade], max_items: int = 20
) -> go.Figure:
    """Dot plot showing which funds are buying/selling each crowded stock."""
    if not crowded:
        return go.Figure()

    items = crowded[:max_items]
    items.reverse()  # Bottom to top (most crowded at top)

    fig = go.Figure()

    actions = [
        ("Initiated", "funds_initiated", "#2ecc71", "circle"),
        ("Added", "funds_added", "#82e0aa", "triangle-up"),
        ("Trimmed", "funds_trimmed", "#f1948a", "triangle-down"),
        ("Exited", "funds_exited", "#e74c3c", "x"),
    ]

    for action_label, field, color, symbol in actions:
        x_vals: list[int] = []
        y_vals: list[str] = []
        hover: list[str] = []

        for ct in items:
            fund_list = getattr(ct, field, [])
            for i, fund_name in enumerate(fund_list):
                # Offset x to avoid overlap within same action
                x_offset = (
                    1 if "nitiat" in action_label or "dded" in action_label
                    else -1
                )
                x_vals.append(x_offset * (i + 1))
                y_vals.append(ct.display_label)
                hover.append(fund_name)

        if x_vals:
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="markers",
                name=action_label,
                marker=dict(
                    color=color, size=10, symbol=symbol,
                    line=dict(width=1, color="white"),
                ),
                hovertext=hover,
                hovertemplate=(
                    "<b>%{hovertext}</b> — "
                    + action_label
                    + "<extra></extra>"
                ),
            ))

    fig.add_vline(x=0, line_color="gray", line_width=1)

    fig.update_layout(
        title="Crowded Trades — Fund-Level Detail",
        xaxis_title="← Selling  |  Buying →",
        height=max(400, len(items) * 35),
        xaxis=dict(zeroline=True, showticklabels=False),
        yaxis=dict(tickfont=dict(size=10)),
        legend=dict(
            orientation="h", yanchor="bottom",
            y=1.02, xanchor="center", x=0.5,
        ),
        **_LAYOUT_DEFAULTS,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 6: Filing Lag Chart
# ---------------------------------------------------------------------------


def filing_lag_chart(fund_diffs: list[FundDiff]) -> go.Figure:
    """Horizontal bar chart of filing lag days per fund, color-coded."""
    if not fund_diffs:
        return go.Figure()

    # Sort by lag descending
    sorted_diffs = sorted(
        fund_diffs, key=lambda d: d.filing_lag_days
    )

    names = [d.fund.name for d in sorted_diffs]
    lags = [d.filing_lag_days for d in sorted_diffs]
    colors = []
    for lag in lags:
        if lag <= 30:
            colors.append("#2ecc71")  # Green — early filer
        elif lag <= 45:
            colors.append("#f39c12")  # Yellow — on time
        else:
            colors.append("#e74c3c")  # Red — late / stale

    fig = go.Figure(go.Bar(
        y=names,
        x=lags,
        orientation="h",
        marker_color=colors,
        text=[f"{lag}d" for lag in lags],
        textposition="outside",
    ))

    # SEC 13F deadline line at 45 days
    fig.add_vline(
        x=45, line_dash="dash", line_color="#e74c3c",
        annotation_text="45-day deadline",
        annotation_position="top",
    )

    fig.update_layout(
        title="Filing Lag (Days After Quarter End)",
        xaxis_title="Days",
        height=max(350, len(names) * 22),
        margin=dict(l=160, r=40, t=50, b=40),
        **{k: v for k, v in _LAYOUT_DEFAULTS.items() if k != "margin"},
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 7: Shared Holdings Sankey
# ---------------------------------------------------------------------------


def shared_holdings_sankey(
    widely_held: list[dict],
    max_stocks: int = 15,
) -> go.Figure:
    """Sankey diagram: funds → most widely held stocks.

    Left nodes = funds (blue).  Right nodes = stocks (orange).
    Flow width = position weight (% of that fund's AUM).
    Stocks are the positions held by the MOST funds simultaneously.

    widely_held: list of dicts with keys:
        issuer_name, ticker (optional), fund_count,
        funds: [{name, weight_pct, value_thousands}]
    """
    if not widely_held:
        return go.Figure()

    stocks = widely_held[:max_stocks]

    # Collect unique fund names and stock labels for node indices
    fund_names: list[str] = []
    fund_set: set[str] = set()
    # Use ticker if available, otherwise truncated issuer_name
    stock_labels = [
        s.get("ticker") or s["issuer_name"][:22]
        for s in stocks
    ]

    for s in stocks:
        for f in s["funds"]:
            if f["name"] not in fund_set:
                fund_set.add(f["name"])
                fund_names.append(f["name"])

    # Nodes: funds first, then stocks
    node_labels = fund_names + stock_labels
    n_funds = len(fund_names)

    # Node colors
    node_colors = ["#3498db"] * n_funds + ["#e67e22"] * len(stock_labels)

    # Links
    sources: list[int] = []
    targets: list[int] = []
    values: list[float] = []
    link_labels: list[str] = []

    for si, stock in enumerate(stocks):
        stock_idx = n_funds + si
        stock_label = stock_labels[si]
        for f in stock["funds"]:
            fund_idx = fund_names.index(f["name"])
            sources.append(fund_idx)
            targets.append(stock_idx)
            val = max(f["weight_pct"], 0.1)  # Minimum visible width
            values.append(val)
            link_labels.append(
                f"{f['name']} → {stock_label}: "
                f"{f['weight_pct']:.1f}% of AUM"
            )

    fig = go.Figure(go.Sankey(
        node=dict(
            label=node_labels,
            color=node_colors,
            pad=15,
            thickness=20,
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            label=link_labels,
            color="rgba(52, 152, 219, 0.2)",
        ),
    ))
    fig.update_layout(
        title=(
            f"Shared Holdings — Top {len(stocks)} Most Widely Held"
            "<br><sup>Left = funds (blue) · Right = stocks held by the most "
            "funds (orange) · Flow width = portfolio weight %</sup>"
        ),
        height=max(600, len(fund_names) * 25),
        font=dict(size=10),
        margin=dict(l=60, r=20, t=75, b=40),
        template="plotly_white",
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 8: Position Weight Distribution
# ---------------------------------------------------------------------------


def position_weight_distribution(
    current_holdings: list[Holding],
    current_total_k: int,
    prior_holdings: list[Holding] | None = None,
    prior_total_k: int | None = None,
    fund_name: str = "",
) -> go.Figure:
    """Histogram of position weights with optional prior quarter overlay."""
    if not current_holdings or current_total_k == 0:
        return go.Figure()

    current_weights = [
        h.value_thousands / current_total_k * 100
        for h in current_holdings
        if not h.is_option
    ]

    fig = make_subplots(
        rows=2, cols=1, row_heights=[0.8, 0.2],
        shared_xaxes=True, vertical_spacing=0.05,
    )

    # Prior quarter overlay (ghost)
    if prior_holdings and prior_total_k and prior_total_k > 0:
        prior_weights = [
            h.value_thousands / prior_total_k * 100
            for h in prior_holdings
            if not h.is_option
        ]
        fig.add_trace(
            go.Histogram(
                x=prior_weights,
                name="Prior Quarter",
                marker_color="rgba(149, 165, 166, 0.4)",
                nbinsx=40,
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Box(
                x=prior_weights, name="Prior",
                marker_color="rgba(149, 165, 166, 0.6)",
                boxmean=True,
            ),
            row=2, col=1,
        )

    # Current quarter
    fig.add_trace(
        go.Histogram(
            x=current_weights,
            name="Current Quarter",
            marker_color="rgba(52, 152, 219, 0.7)",
            nbinsx=40,
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Box(
            x=current_weights, name="Current",
            marker_color="#3498db",
            boxmean=True,
        ),
        row=2, col=1,
    )

    title = "Position Weight Distribution"
    if fund_name:
        title = f"{fund_name} — {title}"

    fig.update_layout(
        title=title,
        height=450,
        barmode="overlay",
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom",
            y=1.02, xanchor="center", x=0.5,
        ),
        **_LAYOUT_DEFAULTS,
    )
    fig.update_xaxes(title_text="Position Weight (% of AUM)", row=2, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=1)

    return fig

"""Position change table component.

Renders a styled dataframe of PositionDiff objects with color coding
for adds/trims/new/exited.
"""

from __future__ import annotations

import streamlit as st

from core.models import PositionChangeType, PositionDiff


def render_diff_table(
    diffs: list[PositionDiff],
    title: str = "",
    max_rows: int = 50,
    show_options_col: bool = True,
) -> None:
    """Render a table of position changes.

    Args:
        diffs: List of PositionDiff to display.
        title: Optional section title.
        max_rows: Maximum rows to show.
        show_options_col: Whether to show the PUT/CALL column.
    """
    if not diffs:
        if title:
            st.markdown(f"**{title}**: None")
        return

    if title:
        st.markdown(f"### {title} ({len(diffs)})")

    rows = []
    for d in diffs[:max_rows]:
        row = {
            "Stock": d.display_label,
            "Change": d.change_type.value,
            "Value": _fmt_value(d.current_value_thousands or d.prior_value_thousands),
            "Weight %": f"{d.current_weight_pct:.2f}%",
            "Share Δ%": _fmt_change_pct(d.shares_change_pct, d.change_type),
            "Weight Δ": f"{d.weight_change_pct:+.2f}pp",
            "Sector": d.sector or "—",
        }
        if d.price_change_since_quarter is not None:
            row["Since QE"] = f"{d.price_change_since_quarter:+.1f}%"
        else:
            row["Since QE"] = "—"

        if d.themes:
            row["Themes"] = ", ".join(d.themes[:2])
        else:
            row["Themes"] = ""

        if show_options_col and d.is_options_position:
            row["Type"] = d.put_call or ""
        elif show_options_col:
            row["Type"] = "Equity"

        rows.append(row)

    st.dataframe(
        rows,
        use_container_width=True,
        hide_index=True,
    )


def render_compact_diff_list(
    diffs: list[PositionDiff],
    title: str,
    max_items: int = 10,
) -> None:
    """Render a compact bullet list of position changes."""
    if not diffs:
        return

    st.markdown(f"**{title}** ({len(diffs)})")
    for d in diffs[:max_items]:
        icon = _change_icon(d.change_type)
        value_str = _fmt_value(d.current_value_thousands or d.prior_value_thousands)
        pct_str = _fmt_change_pct(d.shares_change_pct, d.change_type)
        st.markdown(f"- {icon} **{d.display_label}** — {value_str} ({pct_str})")

    if len(diffs) > max_items:
        st.caption(f"...and {len(diffs) - max_items} more")


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


def _fmt_change_pct(pct: float, change_type: PositionChangeType) -> str:
    """Format share change percentage."""
    if change_type == PositionChangeType.NEW:
        return "NEW"
    if change_type == PositionChangeType.EXITED:
        return "EXIT"
    return f"{pct:+.0%}"


def _change_icon(change_type: PositionChangeType) -> str:
    """Emoji indicator for change type."""
    return {
        PositionChangeType.NEW: "🟢",
        PositionChangeType.EXITED: "🔴",
        PositionChangeType.ADDED: "⬆️",
        PositionChangeType.TRIMMED: "⬇️",
        PositionChangeType.UNCHANGED: "➖",
    }.get(change_type, "")

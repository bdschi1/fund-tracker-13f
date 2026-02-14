"""Report Export — markdown preview and download.

Generates the full quarterly report as markdown and provides
a preview and download button.
"""

from __future__ import annotations

from datetime import date

import streamlit as st

from core.models import CrossFundSignals, FundDiff
from core.report import generate_quarterly_report


def render() -> None:
    """Render the Report Export page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>7</span>"
        "Export Report</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Generates a complete quarterly 13F analysis report in Markdown format. "
        "Includes cross-fund signals, crowded trades, divergences, and "
        "optional per-fund breakdowns. Preview below, then download."
    )

    quarter = st.session_state.get("selected_quarter")
    fund_diffs: dict[date, list[FundDiff]] = st.session_state.get("fund_diffs", {})
    cross_signals: dict[date, CrossFundSignals] = st.session_state.get(
        "cross_signals", {}
    )

    if not quarter or quarter not in fund_diffs or quarter not in cross_signals:
        st.info("No analysis data available. Fetch and analyze data first.")
        return

    diffs = fund_diffs[quarter]
    signals = cross_signals[quarter]

    # Options
    col1, col2 = st.columns(2)
    with col1:
        include_details = st.checkbox("Include Individual Fund Details", value=True)
    with col2:
        max_positions = st.slider(
            "Max positions per section",
            min_value=5,
            max_value=50,
            value=15,
        )

    # Generate report
    report_md = generate_quarterly_report(
        fund_diffs=diffs,
        signals=signals,
        quarter=quarter,
        include_fund_details=include_details,
        max_positions_per_section=max_positions,
    )

    # Download button
    q_num = (quarter.month - 1) // 3 + 1
    filename = f"13f_report_Q{q_num}_{quarter.year}.md"

    st.download_button(
        label=f"Download {filename}",
        data=report_md,
        file_name=filename,
        mime="text/markdown",
    )

    # Preview
    st.divider()
    st.markdown("### Preview")
    st.markdown(report_md)

"""Fund Deep Dive — single fund QoQ analysis.

Shows AUM, concentration, filing lag, and complete position changes
for a selected fund.  Also supports ad-hoc analysis of ANY 13F filer
by CIK (not limited to the watchlist).
"""

from __future__ import annotations

from datetime import date

import streamlit as st

from app.components.charts import position_weight_distribution
from app.components.diff_table import render_diff_table
from app.components.holdings_table import render_holdings_table
from config.settings import settings
from core.diff_engine import compute_fund_diff
from core.models import FundDiff, FundHoldings, FundInfo, Tier
from data.edgar_client import EdgarClient
from data.store import HoldingsStore

# -------------------------------------------------------------------
# Shared rendering for any FundDiff (watchlist or ad-hoc)
# -------------------------------------------------------------------

def _render_fund_diff(diff: FundDiff, quarter: date) -> None:
    """Render header metrics and tabbed position changes for a diff."""
    q_num = (diff.current_quarter.month - 1) // 3 + 1
    q_label = f"Q{q_num} {diff.current_quarter.year}"
    st.subheader(f"{diff.fund.name}")
    st.caption(
        f"Tier {diff.fund.tier.value} | CIK {diff.fund.cik} "
        f"| Holdings as of **{q_label}**"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "AUM",
        _fmt_value(diff.current_aum_thousands),
    )
    col2.metric(
        "Top-10 Weight",
        f"{diff.current_top10_weight:.1%}",
        f"{(diff.current_top10_weight - diff.prior_top10_weight):+.1%}",
    )
    col3.metric(
        "Filing Lag",
        f"{diff.filing_lag_days}d",
        "STALE" if diff.is_stale else "OK",
        delta_color="inverse" if diff.is_stale else "off",
    )
    col4.metric(
        "Positions Changed",
        len(diff.all_changes),
        (
            f"{len(diff.new_positions)} new, "
            f"{len(diff.exited_positions)} exits"
        ),
    )

    st.divider()

    # Position change tabs
    tab_new, tab_exit, tab_add, tab_trim, tab_shape, tab_all = st.tabs([
        f"New ({len(diff.new_positions)})",
        f"Exits ({len(diff.exited_positions)})",
        f"Adds ({len(diff.added_positions)})",
        f"Trims ({len(diff.trimmed_positions)})",
        "Portfolio Shape",
        "All Holdings",
    ])

    with tab_new:
        st.caption("First-time positions this quarter. Sorted by $ value.")
        render_diff_table(
            diff.new_positions, title="New Positions",
        )

    with tab_exit:
        st.caption("Fully sold to zero. Sorted by prior value.")
        render_diff_table(
            diff.exited_positions, title="Exited Positions",
        )

    with tab_add:
        st.caption("Shares increased. Sorted by % change — a 2× beats a 10% top-up.")
        render_diff_table(
            diff.added_positions,
            title="Added Positions (by % increase)",
        )

    with tab_trim:
        st.caption("Shares reduced. Sorted by % decrease — near-total cuts signal lost conviction.")
        render_diff_table(
            diff.trimmed_positions,
            title="Trimmed Positions (by % decrease)",
        )

    with tab_shape:
        st.caption(
            "Weight distribution: concentrated (few big bets) vs. diversified. "
            "Gray overlay = prior quarter."
        )
        store = st.session_state.get("store")
        if store:
            current_h = store.get_holdings(
                diff.fund.cik, quarter,
            )
            prior_h = store.get_holdings(
                diff.fund.cik, diff.prior_quarter,
            )
            fig = position_weight_distribution(
                current_holdings=current_h,
                current_total_k=diff.current_aum_thousands,
                prior_holdings=prior_h if prior_h else None,
                prior_total_k=(
                    diff.prior_aum_thousands if prior_h else None
                ),
                fund_name=diff.fund.name,
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab_all:
        st.caption("Raw 13F filing data — all equity and options positions with weights.")
        store = st.session_state.get("store")
        if store:
            holdings = store.get_holdings(diff.fund.cik, quarter)
            render_holdings_table(
                holdings,
                diff.current_aum_thousands,
                title=f"Full Holdings ({len(holdings)} positions)",
            )


# -------------------------------------------------------------------
# Ad-hoc single-fund pipeline
# -------------------------------------------------------------------

def _run_adhoc_pipeline(cik: str, name: str) -> None:
    """Fetch, resolve, and analyze a single fund by CIK."""
    store: HoldingsStore = st.session_state.store
    fund = FundInfo(name=name, cik=cik, tier=Tier.D)

    status = st.status(
        f"Analyzing {name}…", expanded=True,
    )

    with status:
        # Step 1: Fetch filings
        st.write("**① Fetching filings from EDGAR…**")
        with EdgarClient(
            user_agent=settings.edgar_user_agent,
        ) as client:
            from data.parser import parse_info_table_xml

            filings = client.find_13f_filings(
                cik, n_quarters=2,
            )
            if not filings:
                st.error("No 13F filings found for this CIK.")
                status.update(
                    label="No filings found", state="error",
                )
                return

            n_fetched = 0
            for filing in filings:
                if store.is_filing_processed(
                    cik, filing.accession_number,
                ):
                    continue
                xml_text = client.fetch_info_table_xml(filing)
                fh = parse_info_table_xml(
                    xml_text=xml_text,
                    fund=fund,
                    quarter_end=filing.quarter_end,
                    filing_date=date.fromisoformat(
                        filing.filing_date,
                    ),
                    report_date=date.fromisoformat(
                        filing.report_date,
                    ),
                )
                count = store.store_holdings(fh)
                store.store_filing_index(
                    cik=cik,
                    accession_number=filing.accession_number,
                    filing_date=filing.filing_date,
                    report_date=filing.report_date,
                    quarter_end=filing.quarter_end.isoformat(),
                    form_type=filing.form_type,
                    primary_doc=filing.primary_doc,
                    holdings_count=count,
                    total_value_thousands=fh.total_value_thousands,
                )
                n_fetched += 1
            st.write(f"✓ {n_fetched} new filings fetched")

        # Step 2: Resolve CUSIPs
        st.write("**② Resolving CUSIPs…**")
        quarters = store.get_available_quarters(cik)
        if len(quarters) < 2:
            st.error(
                "Need at least 2 quarters for QoQ comparison. "
                f"Only found {len(quarters)}."
            )
            status.update(
                label="Not enough data", state="error",
            )
            return

        current_q = quarters[0]
        prior_q = quarters[1]

        from data.cache import DataCache
        from data.cusip_resolver import resolve_cusips

        cache: DataCache = st.session_state.cache
        cusips = store.get_unique_cusips_for_quarter(current_q)
        ticker_map = {}
        if cusips:
            resolved = resolve_cusips(
                cusips=cusips,
                cache_read=cache.cusip_cache_read,
                cache_write=cache.cusip_cache_write,
                api_key=settings.openfigi_api_key,
            )
            ticker_map = cache.get_cusip_tickers(cusips)
            st.write(f"✓ {len(resolved)} CUSIPs resolved")

        # Step 3: Build diff
        st.write("**③ Running diff engine…**")
        current_holdings = store.get_holdings(cik, current_q)
        prior_holdings = store.get_holdings(cik, prior_q)

        for h in current_holdings + prior_holdings:
            if h.cusip in ticker_map:
                h.ticker = ticker_map[h.cusip]

        filing_date = (
            store.get_filing_date(cik, current_q) or current_q
        )
        current_fh = FundHoldings(
            fund=fund,
            quarter_end=current_q,
            filing_date=filing_date,
            report_date=current_q,
            holdings=current_holdings,
        )
        prior_fdate = (
            store.get_filing_date(cik, prior_q) or prior_q
        )
        prior_fh = FundHoldings(
            fund=fund,
            quarter_end=prior_q,
            filing_date=prior_fdate,
            report_date=prior_q,
            holdings=prior_holdings,
        )

        diff = compute_fund_diff(current_fh, prior_fh)
        st.write(
            f"✓ {len(diff.all_changes)} position changes "
            f"({current_q} vs. {prior_q})"
        )

    status.update(
        label=f"Analysis complete — {name}",
        state="complete",
        expanded=False,
    )

    # Store in session state
    st.session_state["adhoc_fund_diff"] = diff
    st.session_state["adhoc_fund_info"] = fund
    st.session_state["adhoc_quarter"] = current_q
    st.rerun()


# -------------------------------------------------------------------
# Main render
# -------------------------------------------------------------------

def render() -> None:
    """Render the Fund Deep Dive page."""
    st.markdown(
        "<h2 style='margin-bottom:0;'>"
        "<span style='font-size:1.4rem; font-weight:700; "
        "color:#4A9EFF; margin-right:8px;'>3</span>"
        "Fund Deep Dive</h2>",
        unsafe_allow_html=True,
    )
    st.caption("QoQ analysis of a single fund. Pick a fund below or look up any CIK.")

    quarter = st.session_state.get("selected_quarter")
    fund_diffs: dict[date, list[FundDiff]] = st.session_state.get(
        "fund_diffs", {},
    )

    # --- Inline fund picker (always visible when analysis data exists) ---
    if quarter and quarter in fund_diffs:
        diffs = fund_diffs[quarter]
        fund_names = [d.fund.name for d in diffs]

        # Determine current selection index
        selected_cik = st.session_state.get("selected_fund_cik")
        current_idx = 0
        if selected_cik:
            for i, d in enumerate(diffs):
                if d.fund.cik == selected_cik:
                    current_idx = i
                    break

        chosen = st.selectbox(
            "Select Fund",
            options=fund_names,
            index=current_idx,
            key="fund_deep_dive_picker",
        )
        # Sync the selection back to session state
        chosen_diff = next(
            (d for d in diffs if d.fund.name == chosen), None,
        )
        if chosen_diff:
            st.session_state["selected_fund_cik"] = chosen_diff.fund.cik
            _render_fund_diff(chosen_diff, quarter)
        return

    # --- Ad-hoc fund (if previously analyzed) ---
    adhoc_diff: FundDiff | None = st.session_state.get(
        "adhoc_fund_diff",
    )
    if adhoc_diff:
        adhoc_q = st.session_state.get("adhoc_quarter")
        _render_fund_diff(adhoc_diff, adhoc_q)
        if st.button("Clear ad-hoc analysis"):
            del st.session_state["adhoc_fund_diff"]
            del st.session_state["adhoc_fund_info"]
            del st.session_state["adhoc_quarter"]
            st.rerun()
        return

    # --- No analysis data yet ---
    st.info("Run an analysis first using the sidebar.")

    st.divider()

    # Ad-hoc CIK lookup
    st.markdown("#### Look Up Any Fund")
    st.caption(
        "Enter a CIK number to fetch and analyze any 13F filer "
        "— not limited to the watchlist. Find CIKs at "
        "[sec.gov/cgi-bin/browse-edgar]"
        "(https://www.sec.gov/cgi-bin/browse-edgar"
        "?action=getcompany&type=13F&dateb=&owner="
        "include&count=40)."
    )

    cik_input = st.text_input(
        "CIK Number",
        placeholder="e.g. 1067983 (Berkshire Hathaway)",
        key="adhoc_cik_input",
    )

    # Easter egg
    if cik_input and cik_input.strip().lower() == "garcia":
        st.markdown(
            "🎸 [What a long, strange trip it's been…]"
            "(https://en.wikipedia.org/wiki/Jerry_Garcia)",
        )
        return

    if cik_input and cik_input.strip().isdigit():
        cik = cik_input.strip()

        # Validate CIK
        if st.button("🔍 Look up", key="adhoc_lookup_btn"):
            with st.spinner("Looking up CIK on EDGAR…"):
                with EdgarClient(
                    user_agent=settings.edgar_user_agent,
                ) as client:
                    entity = client.lookup_entity(cik)

            if entity:
                st.session_state["adhoc_entity"] = entity
            else:
                st.error(
                    f"CIK {cik} not found on EDGAR. "
                    "Check the number and try again."
                )

        # Show entity info + fetch button
        entity = st.session_state.get("adhoc_entity")
        if entity and entity["cik"] == cik:
            st.success(
                f"**{entity['name']}** — CIK {entity['cik']}"
            )
            if st.button(
                "▶ Fetch & Analyze",
                type="primary",
                key="adhoc_fetch_btn",
            ):
                _run_adhoc_pipeline(
                    cik=entity["cik"],
                    name=entity["name"],
                )

    elif cik_input and not cik_input.strip().isdigit():
        st.caption("CIK must be a number (digits only).")


def _fmt_value(thousands: int) -> str:
    """Format $thousands into human-readable."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    return f"${dollars / 1_000:.0f}K"

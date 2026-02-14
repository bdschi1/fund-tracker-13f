"""Fund Tracker 13F — Main Streamlit Application.

Entry point: streamlit run app/main.py
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import streamlit as st

st.set_page_config(
    page_title="Fund Tracker 13F",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Allow text selection across the entire app
st.markdown(
    "<style>"
    "* { -webkit-user-select: text !important; "
    "user-select: text !important; }"
    "</style>",
    unsafe_allow_html=True,
)

from app.components.fund_selector import render_fund_picker, render_tier_filter  # noqa: E402
from app.components.quarter_picker import render_quarter_picker  # noqa: E402
from app.components.ticker_lookup import render_ticker_lookup  # noqa: E402
from app.state.session import (  # noqa: E402
    get_available_quarters,
    get_filtered_funds,
    init_session_state,
)
from app.views import (  # noqa: E402
    crowded_trades,
    dashboard,
    fund_overview,
    overlap_matrix,
    report_export,
    signal_scanner,
    stock_analysis,
)
from config.settings import settings  # noqa: E402
from core.aggregator import aggregate_signals, compute_fund_baselines, tag_themes  # noqa: E402
from core.diff_engine import compute_fund_diff  # noqa: E402
from core.models import CrossFundSignals, FundDiff, FundHoldings, FundInfo  # noqa: E402
from core.report import generate_quarterly_report  # noqa: E402
from data.cache import DataCache  # noqa: E402
from data.edgar_client import EdgarClient  # noqa: E402
from data.filing_parser import parse_info_table_xml  # noqa: E402
from data.store import HoldingsStore  # noqa: E402

logger = logging.getLogger(__name__)

init_session_state()


# ---------------------------------------------------------------------------
# Data Pipeline Functions
# ---------------------------------------------------------------------------


def fetch_filings(
    funds: list[FundInfo],
    n_quarters: int = 2,
    progress_bar=None,
) -> int:
    """Fetch and store 13F filings from EDGAR for all funds.

    Returns number of filings processed.
    """
    store: HoldingsStore = st.session_state.store
    total_processed = 0

    with EdgarClient(user_agent=settings.edgar_user_agent) as client:
        for i, fund in enumerate(funds):
            if progress_bar:
                progress_bar.progress(
                    (i + 1) / len(funds),
                    text=f"Fetching {fund.name}...",
                )

            try:
                filings = client.find_13f_filings(fund.cik, n_quarters=n_quarters)

                for filing in filings:
                    # Skip already-processed filings
                    if store.is_filing_processed(fund.cik, filing.accession_number):
                        logger.debug("Skipping already-processed: %s", filing.accession_number)
                        continue

                    # Fetch and parse
                    xml_text = client.fetch_info_table_xml(filing)
                    fund_holdings = parse_info_table_xml(
                        xml_text=xml_text,
                        fund=fund,
                        quarter_end=filing.quarter_end,
                        filing_date=date.fromisoformat(filing.filing_date),
                        report_date=date.fromisoformat(filing.report_date),
                    )

                    # Store
                    count = store.store_holdings(fund_holdings)
                    store.store_filing_index(
                        cik=fund.cik,
                        accession_number=filing.accession_number,
                        filing_date=filing.filing_date,
                        report_date=filing.report_date,
                        quarter_end=filing.quarter_end.isoformat(),
                        form_type=filing.form_type,
                        primary_doc=filing.primary_doc,
                        holdings_count=count,
                        total_value_thousands=fund_holdings.total_value_thousands,
                    )
                    total_processed += 1

            except Exception as e:
                # Provide cleaner error messages for common failures
                err_msg = str(e)
                if "RetryError" in type(e).__name__:
                    # Extract the inner exception from tenacity
                    inner = e.__cause__ or e
                    if hasattr(e, "last_attempt"):
                        try:
                            inner = e.last_attempt.result()
                        except Exception as inner_exc:
                            inner = inner_exc
                    err_msg = f"EDGAR unavailable after retries: {inner}"
                elif "HTTPStatusError" in type(e).__name__:
                    err_msg = f"HTTP {e.response.status_code}"

                logger.error(
                    "Error fetching %s (CIK %s): %s",
                    fund.name,
                    fund.cik,
                    err_msg,
                    exc_info=True,
                )
                st.toast(f"⚠️ {fund.name}: {err_msg}", icon="⚠️")

    return total_processed


def resolve_all_cusips(quarter: date) -> int:
    """Resolve CUSIPs for a quarter from the bundled seed cache.

    No API calls — instant resolution from config/cusip_tickers.json
    which is loaded into SQLite on startup. To resolve new CUSIPs,
    run ``python scripts/resolve_new_cusips.py`` offline.
    """
    store: HoldingsStore = st.session_state.store
    cache: DataCache = st.session_state.cache

    cusips = store.get_unique_cusips_for_quarter(quarter)
    if not cusips:
        return 0

    resolved = cache.get_cusip_tickers(cusips)
    return len(resolved)


def run_analysis(quarter: date) -> tuple[list[FundDiff], CrossFundSignals]:
    """Run the full analysis pipeline for a quarter.

    1. Load holdings for current and prior quarter
    2. Enrich with tickers from CUSIP cache
    3. Compute diffs for each fund
    4. Aggregate cross-fund signals

    Also stores skipped_funds in session state for UI reporting.
    """
    store: HoldingsStore = st.session_state.store
    cache: DataCache = st.session_state.cache
    watchlist = st.session_state.watchlist

    # Get CUSIP→ticker mapping for enrichment
    cusips = store.get_unique_cusips_for_quarter(quarter)
    ticker_map = cache.get_cusip_tickers(cusips)

    # Get sector/float data from cache (if available)
    tickers = list(set(ticker_map.values()))
    sector_map = cache.get_sector_info_bulk(tickers) if tickers else {}

    fund_diffs: list[FundDiff] = []
    skipped: list[dict] = []  # Track which funds were excluded

    for fund in watchlist:
        quarters = store.get_available_quarters(fund.cik)
        if quarter not in quarters:
            skipped.append({
                "name": fund.name, "reason": "no filing",
            })
            continue

        # Find prior quarter
        q_idx = quarters.index(quarter)
        if q_idx + 1 >= len(quarters):
            # No prior quarter available
            skipped.append({
                "name": fund.name,
                "reason": "no prior quarter data",
            })
            continue
        prior_quarter = quarters[q_idx + 1]

        # Load holdings
        current_holdings = store.get_holdings(fund.cik, quarter)
        prior_holdings = store.get_holdings(fund.cik, prior_quarter)

        if not current_holdings or not prior_holdings:
            skipped.append({
                "name": fund.name,
                "reason": "empty holdings",
            })
            continue

        # Enrich with tickers + sectors
        for h in current_holdings + prior_holdings:
            if h.cusip in ticker_map:
                h.ticker = ticker_map[h.cusip]
                info = sector_map.get(h.ticker)
                if info:
                    h.sector = info.get("sector")
                    h.industry = info.get("industry")

        # Build FundHoldings objects
        filing_date = store.get_filing_date(fund.cik, quarter) or quarter
        current_fh = FundHoldings(
            fund=fund,
            quarter_end=quarter,
            filing_date=filing_date,
            report_date=quarter,
            holdings=current_holdings,
        )
        prior_filing_date = store.get_filing_date(fund.cik, prior_quarter) or prior_quarter
        prior_fh = FundHoldings(
            fund=fund,
            quarter_end=prior_quarter,
            filing_date=prior_filing_date,
            report_date=prior_quarter,
            holdings=prior_holdings,
        )

        # Compute diff
        diff = compute_fund_diff(current_fh, prior_fh)

        # Tag themes on all changes
        tag_themes(diff.all_changes, str(settings.themes_path))

        fund_diffs.append(diff)

    # Store skip info so the UI can report it
    st.session_state["skipped_funds"] = skipped

    # Cross-fund aggregation (with dollar-weighted + float metrics)
    signals = aggregate_signals(
        fund_diffs=fund_diffs,
        quarter=quarter,
        min_funds_for_crowd=settings.min_funds_for_crowd,
        min_funds_for_consensus=settings.min_funds_for_consensus,
        sector_data=sector_map,
    )

    # Store sector/float data for use by views
    st.session_state.setdefault("sector_data", {})[quarter] = sector_map

    # Historical baselines for findings scoring
    baselines = compute_fund_baselines(
        store=store,
        fund_ciks=[d.fund.cik for d in fund_diffs],
        current_quarter=quarter,
    )
    st.session_state.setdefault("fund_baselines", {})[quarter] = baselines

    return fund_diffs, signals


def _run_full_pipeline(n_quarters: int) -> None:
    """Run the complete pipeline: fetch → resolve CUSIPs → analyze.

    Called by the single "Run Full Analysis" button.
    """
    # Top-level progress bar visible outside the status expander
    pipeline_bar = st.progress(0, text="Starting pipeline…")

    status = st.status(
        "Running full analysis pipeline…", expanded=True,
    )

    # Step 1 — Fetch
    n_funds = len(st.session_state.watchlist)
    with status:
        st.write("**① Fetching filings from EDGAR…**")
        # Wrap progress to drive both the detail bar and the pipeline bar
        detail_progress = st.progress(0)

        def _dual_progress(frac: float, text: str = "") -> None:
            detail_progress.progress(frac, text=text)
            # Step 1 spans 0–60% of overall pipeline
            pipeline_bar.progress(
                frac * 0.60,
                text=f"① Fetching ({int(frac * n_funds)}/{n_funds})…",
            )

        # Use a thin wrapper so fetch_filings can call .progress()
        class _DualBar:
            @staticmethod
            def progress(frac: float, text: str = "") -> None:
                _dual_progress(frac, text)

        fetch_count = fetch_filings(
            funds=st.session_state.watchlist,
            n_quarters=n_quarters,
            progress_bar=_DualBar,
        )
        detail_progress.empty()
        st.write(f"✓ {fetch_count} new filings processed")

    # Refresh quarters after fetch
    quarters = get_available_quarters()
    if not quarters:
        pipeline_bar.empty()
        with status:
            st.error("No filings found. Check fund CIKs.")
            status.update(
                label="Pipeline failed", state="error",
            )
        return

    # Auto-select the most recent quarter
    quarter = quarters[0]
    st.session_state["selected_quarter"] = quarter

    # Step 2 — Resolve CUSIPs + Analyze
    pipeline_bar.progress(0.70, text="② Resolving CUSIPs…")
    with status:
        st.write(
            f"**② Resolving CUSIPs for {quarter}…**"
        )
        cusip_count = resolve_all_cusips(quarter)
        st.write(f"✓ {cusip_count} CUSIPs resolved")

    pipeline_bar.progress(0.85, text="③ Analyzing…")
    with status:
        st.write("**③ Running analysis engine…**")
        diffs, signals = run_analysis(quarter)
        st.session_state.fund_diffs[quarter] = diffs
        st.session_state.cross_signals[quarter] = signals
        _export_report(quarter, diffs, signals)
        st.write(
            f"✓ {len(diffs)} funds | "
            f"{len(signals.crowded_trades)} crowded trades | "
            f"{len(signals.divergences)} divergences"
        )
        skipped = st.session_state.get("skipped_funds", [])
        if skipped:
            no_prior = [
                s["name"] for s in skipped
                if s["reason"] == "no prior quarter data"
            ]
            no_filing = [
                s["name"] for s in skipped
                if s["reason"] == "no filing"
            ]
            if no_prior:
                st.write(
                    f"⚠️ {len(no_prior)} fund(s) skipped — "
                    f"no prior quarter data: "
                    f"{', '.join(no_prior[:5])}"
                    + (f" +{len(no_prior)-5} more"
                       if len(no_prior) > 5 else "")
                )
            if no_filing:
                st.write(
                    f"⚠️ {len(no_filing)} fund(s) skipped — "
                    f"no filing for {quarter}"
                )

    pipeline_bar.progress(1.0, text="✅ Pipeline complete")
    status.update(
        label=(
            f"Analysis complete — {len(diffs)} funds "
            f"for {quarter}"
        ),
        state="complete",
        expanded=False,
    )
    st.rerun()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------


def render_sidebar() -> str:
    """Render sidebar controls. Returns selected page name."""
    with st.sidebar:
        st.title("📊 Fund Tracker 13F")
        st.caption("13F Filing Analyzer — Hedge Funds & Asset Managers")

        # --- Quarter picker & Analyze (above navigation) ---
        quarters = get_available_quarters()
        has_data = len(quarters) > 0
        q = st.session_state.get("selected_quarter")

        if has_data:
            render_quarter_picker(quarters)

        # Tier filter — under quarter picker, above Analyze
        with st.expander("Fund Tiers", expanded=False):
            render_tier_filter()

        # Data source selector — auto-detects available providers
        with st.expander("Data Source", expanded=False):
            from data.provider_factory import (
                PROVIDER_DESCRIPTIONS,
                available_providers,
                get_provider_safe,
            )

            avail = available_providers()
            current_source = st.session_state.get("data_source", "Yahoo Finance")
            if current_source not in avail:
                current_source = avail[0] if avail else "Yahoo Finance"

            source = st.selectbox(
                "Provider",
                options=avail,
                index=avail.index(current_source) if current_source in avail else 0,
                key="data_source_select",
                help=(
                    "Switch the market data provider. Interactive Brokers "
                    "requires TWS or IB Gateway running locally."
                ),
            )
            if source in PROVIDER_DESCRIPTIONS:
                st.caption(PROVIDER_DESCRIPTIONS[source])

            if source != st.session_state.get("data_source"):
                st.session_state["data_source"] = source
                st.session_state["market_provider"] = get_provider_safe(source)

        if has_data and q:
            already_analyzed = q in st.session_state.get("fund_diffs", {})

            # Always show Analyze — primary if not yet run, secondary for re-run
            btn_label = "▶ Analyze" if not already_analyzed else "🔄 Re-Analyze"
            btn_type = "primary" if not already_analyzed else "secondary"
            if st.button(
                btn_label,
                use_container_width=True,
                type=btn_type,
                help="Compare this quarter to the prior quarter",
            ):
                with st.spinner("Analyzing..."):
                    diffs, signals = run_analysis(q)
                    st.session_state.fund_diffs[q] = diffs
                    st.session_state.cross_signals[q] = signals
                    _export_report(q, diffs, signals)
                st.rerun()

            # Status line
            if already_analyzed:
                n_funds = len(st.session_state.fund_diffs[q])
                st.caption(f"✅ {n_funds} funds analyzed")

        # Fetch/refresh button
        if st.button(
            "▶ Fetch & Analyze" if not has_data else "🔄 Refresh from EDGAR",
            use_container_width=True,
            type="primary" if not has_data else "secondary",
            help=(
                "Re-downloads latest 13F filings from SEC EDGAR "
                "and re-runs the full analysis. Use this when new "
                "quarterly filings become available."
            ),
        ):
            _run_full_pipeline(n_quarters=8)

        st.divider()

        # Page navigation — numbered to match How to Use guide
        _NAV_OPTIONS = {
            "Dashboard": "Visual summary of the quarter",
            "Stock Analysis": "Search a ticker — who's buying & selling",
            "Fund Deep Dive": "Single fund QoQ position analysis",
            "Signal Scanner": "High-conviction signals across all funds",
            "Crowded Trades": "Consensus buys, sells & divergences",
            "Overlap Matrix": "Portfolio similarity heatmap",
            "Export Report": "Markdown report preview & download",
        }

        page = st.radio(
            "Navigate",
            options=list(_NAV_OPTIONS.keys()),
            captions=list(_NAV_OPTIONS.values()),
            format_func=lambda x: (
                f"{list(_NAV_OPTIONS.keys()).index(x) + 1} · {x}"
            ),
            index=0,
        )

        # Fund picker (for deep dive)
        if page == "Fund Deep Dive":
            st.divider()
            filtered_funds = get_filtered_funds()
            render_fund_picker(filtered_funds)

    return page


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _render_app_header() -> None:
    """Render the top-of-page description shown before any view."""
    quarters = get_available_quarters()
    n_funds = len(st.session_state.watchlist)
    q = st.session_state.get("selected_quarter")
    analyzed = q and q in st.session_state.get("fund_diffs", {})

    if analyzed:
        n_analyzed = len(st.session_state.fund_diffs[q])
        skipped = st.session_state.get("skipped_funds", [])
        skip_note = ""
        if skipped:
            skip_note = (
                f" | **{len(skipped)}** skipped "
                f"(missing prior quarter)"
            )
        st.caption(
            f"Tracking **{n_funds}** funds | "
            f"**{len(quarters)}** quarters loaded | "
            f"**{n_analyzed}** funds analyzed for "
            f"**{q}**{skip_note}"
        )
    elif quarters:
        st.caption(
            f"Tracking **{n_funds}** funds | "
            f"**{len(quarters)}** quarters loaded"
        )


def _render_onboarding() -> None:
    """Show onboarding guide when no analysis data is available."""
    quarters = get_available_quarters()
    n_funds = len(st.session_state.watchlist)
    q = st.session_state.get("selected_quarter")
    analyzed = q and q in st.session_state.get("fund_diffs", {})

    if analyzed:
        return  # User has data — let the page render normally

    # --- Welcome + Filing Deadlines on same row ---
    col_welcome, col_deadlines = st.columns([3, 2])

    with col_welcome:
        st.markdown("## Welcome to Fund Tracker 13F")
        st.caption(
            f"Analyzes SEC 13F-HR filings from {n_funds} hedge funds & "
            f"asset managers. Surfaces high-conviction moves — new positions, "
            f"exits, concentrated adds, consensus trades. Ranked by signal "
            f"strength (% share change), not dollar size."
        )

    with col_deadlines:
        _render_filing_deadlines()
        _render_tracked_funds()

    st.divider()

    # --- How to Use: instructions + descriptions ---
    st.markdown("### How to Use")

    _n = (
        "<span style='font-size:1.3rem; font-weight:700; "
        "color:#4A9EFF; margin-right:6px;'>{}</span>"
    )

    if not quarters:
        # No data at all — first-time instructions
        st.info(
            "**First time?** Click **▶ Fetch & Analyze** in the sidebar. "
            "Downloads filings from EDGAR, resolves CUSIPs, and runs the full analysis (1–3 min). "
            "Data cached locally in SQLite."
        )
    else:
        # Has data but hasn't analyzed yet
        st.info(
            f"**{len(quarters)} quarter(s)** cached locally. "
            f"Click **🔄 Refresh from EDGAR** to pull the latest filings, "
            f"then **▶ Analyze** to run the comparison."
        )

    st.markdown("Once the analysis runs, explore the pages:")
    st.markdown(
        f"{_n.format(1)} **Dashboard** — top findings, fund summary, "
        "activity heatmap, concentration shifts<br>"
        f"{_n.format(2)} **Stock Analysis** — search any ticker to see "
        "which funds hold it and net sentiment<br>"
        f"{_n.format(3)} **Fund Deep Dive** — single-fund breakdown "
        "(AUM, position changes, filing lag) or ad-hoc CIK lookup<br>"
        f"{_n.format(4)} **Signal Scanner** — every new position, exit, "
        "significant add/trim across all funds<br>"
        f"{_n.format(5)} **Crowded Trades** — consensus buys/sells "
        "(3+ funds) and divergences<br>"
        f"{_n.format(6)} **Overlap Matrix** — fund-to-fund portfolio "
        "similarity heatmap and shared-holdings Sankey<br>"
        f"{_n.format(7)} **Export Report** — download a Markdown "
        "summary of all signals and fund breakdowns",
        unsafe_allow_html=True,
    )

    st.divider()
    with st.expander("Key Concepts", expanded=False):
        st.markdown(
            "**13F-HR** — SEC filing required quarterly from "
            "institutional investment managers with 100M+ in U.S. "
            "equities. Due 45 days after quarter end.\n\n"
            "**Conviction sizing** — Moves ranked by *percentage "
            "change in shares*, not dollar value. Adds and trims "
            "must also be ≥ 0.25% of AUM to filter out noise from "
            "micro-positions.\n\n"
            "**Filing lag** — Days between quarter end and filing "
            "date. Closer to 45 days = more stale. Most top-tier "
            "funds file within 30 days.\n\n"
            "**HHI (Herfindahl Index)** — Measures portfolio "
            "concentration. Positive change = concentrating into "
            "fewer names. Negative = diversifying.\n\n"
            "**Cosine similarity** — Value-weighted portfolio "
            "overlap. Two funds with the same top holdings at "
            "similar weights score high, even if they share few "
            "small positions. A better measure of correlated "
            "drawdown risk than simple name-count overlap.\n\n"
            "**Float ownership %** — Total shares held by tracked "
            "funds as a percentage of the stock's public float. "
            "Above 5% signals crowding risk — if multiple funds "
            "exit at once, price impact cascades.\n\n"
            "**Dollar-weighted flows** — Aggregate dollar value "
            "flowing into or out of a sector or stock across all "
            "tracked funds, not just a count of funds buying vs. "
            "selling."
        )

    # --- Disclaimer ---
    with st.expander("⚠️ Disclaimer", expanded=False):
        st.markdown(
            "This tool is provided **for informational and educational "
            "purposes only**. It is not investment advice, and nothing "
            "displayed here constitutes a recommendation to buy, sell, "
            "or hold any security.\n\n"
            "13F filings are **backward-looking** — they reflect "
            "positions as of a quarter-end date and are filed up to "
            "45 days later. Fund managers may have materially changed "
            "their holdings between the reporting date and now.\n\n"
            "Data is sourced from SEC EDGAR and yfinance. While "
            "reasonable efforts are made to ensure accuracy, there may "
            "be errors, omissions, or delays. Derived metrics "
            "(conviction scores, crowding risk, sector flows) are "
            "heuristic estimates, not audited figures.\n\n"
            "**Do not rely on this tool for investment decisions.** "
            "Always consult a qualified financial advisor and do your "
            "own due diligence before investing."
        )

    # --- Topic badges ---
    st.markdown(
        "<div style='margin-top:16px; display:flex; flex-wrap:wrap; "
        "gap:6px; justify-content:center;'>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>13f-filings</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>hedge-funds</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>sec-edgar</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>portfolio-analysis</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>institutional-investors</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>streamlit</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>python</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>crowded-trades</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>conviction-analysis</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1f3044; color:#58a6ff;'>float-ownership</span>"
        "</div>"
        "<div style='margin-top:6px; display:flex; flex-wrap:wrap; "
        "gap:6px; justify-content:center;'>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>sqlite</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>plotly</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>pydantic</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>yfinance</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>pandas</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>rest-api</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>data-pipeline</span>"
        "<span style='display:inline-block; padding:4px 12px; "
        "border-radius:24px; font-size:0.72rem; font-weight:500; "
        "background:#1a2f1a; color:#7ee787;'>caching</span>"
        "</div>",
        unsafe_allow_html=True,
    )


def _render_filing_deadlines() -> None:
    """Render SEC filing deadlines reference box."""
    with st.expander(
        "Filing Deadlines Reference", expanded=False,
    ):
        st.markdown(
            "<style>"
            ".deadline-box { font-size:0.78rem; line-height:1.5; } "
            ".deadline-box table { width:100%; "
            "border-collapse:collapse; margin:4px 0 8px; } "
            ".deadline-box th, .deadline-box td "
            "{ padding:3px 6px; text-align:left; "
            "border-bottom:1px solid #444; white-space:nowrap; } "
            ".deadline-box th { font-weight:600; color:#999; } "
            "</style>"
            "<div class='deadline-box'>"
            "<b>SEC (U.S. Domestic)</b>"
            "<table>"
            "<tr><th>Filing</th><th>Deadline</th>"
            "<th>~Q4 '25</th><th>~Q1 '26</th></tr>"
            "<tr><td><b>13F-HR</b></td><td>45 days</td>"
            "<td>Feb 14 '26</td><td>May 15 '26</td></tr>"
            "<tr><td><b>10-K</b></td><td>60–90 days</td>"
            "<td>Mar 1 '26</td><td>—</td></tr>"
            "<tr><td><b>10-Q</b></td><td>40–45 days</td>"
            "<td>—</td><td>May 10 '26</td></tr>"
            "</table>"
            "<b>Foreign / ADR</b>"
            "<table>"
            "<tr><th>Filing</th><th>Equiv.</th>"
            "<th>Deadline</th><th>~Due</th></tr>"
            "<tr><td><b>20-F</b></td><td>10-K</td>"
            "<td>4 months</td><td>Apr 30 '26</td></tr>"
            "<tr><td><b>6-K</b></td><td>8-K/10-Q</td>"
            "<td>Promptly</td><td>Varies</td></tr>"
            "</table>"
            "<span style='color:#888;'>Dates assume Dec 31 "
            "FY-end. 10-K: 60d (Large Accel.) to 90d "
            "(Smaller).</span>"
            "</div>",
            unsafe_allow_html=True,
        )


_TIER_LABELS = {
    "A": "Multi-Strat",
    "B": "Stock Pickers / Tiger Cubs",
    "C": "Event-Driven / Activist",
    "D": "Emerging / Newer",
    "E": "Healthcare Specialists",
}


def _render_tracked_funds() -> None:
    """Render a collapsible list of all watchlist funds grouped by tier."""
    watchlist: list[FundInfo] = st.session_state.get("watchlist", [])
    if not watchlist:
        return

    with st.expander(
        f"Tracked Funds ({len(watchlist)})", expanded=False,
    ):
        # Group by tier
        by_tier: dict[str, list[FundInfo]] = {}
        for f in watchlist:
            by_tier.setdefault(f.tier.value, []).append(f)

        html = (
            "<style>"
            ".fund-list { font-size:0.78rem; line-height:1.6; } "
            ".fund-list b { color:#4A9EFF; } "
            ".fund-list .tier-hdr { font-weight:600; color:#999; "
            "margin:6px 0 2px; } "
            "</style>"
            "<div class='fund-list'>"
        )
        for tier_key in ("A", "B", "C", "D", "E"):
            funds = by_tier.get(tier_key, [])
            if not funds:
                continue
            label = _TIER_LABELS.get(tier_key, tier_key)
            html += (
                f"<div class='tier-hdr'>"
                f"<b>Tier {tier_key}</b> · {label} "
                f"({len(funds)})</div>"
            )
            names = ", ".join(f.name for f in funds)
            html += f"<div>{names}</div>"
        html += "</div>"

        st.markdown(html, unsafe_allow_html=True)


_EXPORT_DIR = Path.home() / "Desktop" / "13F scan"


def _export_report(
    quarter: date,
    diffs: list[FundDiff],
    signals: CrossFundSignals,
) -> None:
    """Save markdown report to ~/Desktop/13F scan/ (local) or skip on cloud."""
    try:
        _EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        q_num = (quarter.month - 1) // 3 + 1
        filename = f"13f_report_Q{q_num}_{quarter.year}.md"
        path = _EXPORT_DIR / filename
        baselines = st.session_state.get(
            "fund_baselines", {},
        ).get(quarter)
        report_md = generate_quarterly_report(
            fund_diffs=diffs,
            signals=signals,
            quarter=quarter,
            baselines=baselines,
        )
        path.write_text(report_md, encoding="utf-8")
        logger.info("Exported report to %s", path)
    except OSError:
        # Streamlit Cloud or other env without ~/Desktop — skip silently
        logger.debug("Desktop export skipped (no writable path)")


def main() -> None:
    page = render_sidebar()

    _render_app_header()

    # Show onboarding guide if no analysis data exists yet
    q = st.session_state.get("selected_quarter")
    has_analysis = q and q in st.session_state.get("fund_diffs", {})

    if not has_analysis:
        # Onboarding includes filing deadlines in its own layout
        _render_onboarding()
        return

    # Reference dropdowns — single row
    ref1, ref2, ref3 = st.columns([1, 1, 1])
    with ref1:
        _render_filing_deadlines()
    with ref2:
        _render_tracked_funds()
    with ref3:
        with st.expander("📖 How to Read", expanded=False):
            st.markdown(
                "**Top Findings** — 5 most actionable cross-fund signals. "
                "Consensus buys and divergences rank highest.\n\n"
                "**Fund Summary** — One row per fund: AUM, "
                "position counts, top-10 weight, HHI change, filing lag.\n\n"
                "**Top Moves** — Largest stock moves across all funds: "
                "initiations (green), exits (red), adds >50% (light green), "
                "trims >60% (light red). Ranked by weight change.\n\n"
                "**Heatmap** — Darker = more activity. Compare rows to "
                "spot active vs. quiet funds.\n\n"
                "**Concentration** — Top-10 weight and HHI change. "
                "Positive HHI = concentrating, negative = diversifying."
            )

    # Ticker lookup — popover below reference row
    with st.popover("🔍 Stock Lookup"):
        st.caption("Type a ticker or company name. Click outside to close.")
        render_ticker_lookup()

    match page:
        case "Dashboard":
            dashboard.render()
        case "Stock Analysis":
            stock_analysis.render()
        case "Fund Deep Dive":
            fund_overview.render()
        case "Signal Scanner":
            signal_scanner.render()
        case "Crowded Trades":
            crowded_trades.render()
        case "Overlap Matrix":
            overlap_matrix.render()
        case "Export Report":
            report_export.render()



main()

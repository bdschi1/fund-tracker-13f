"""Cross-fund signal aggregation.

Takes individual FundDiff objects across all watched funds and surfaces:
- Crowded trades: stocks multiple watched funds all added
- Divergences: Fund A initiated while Fund B exited
- Consensus initiations: stocks 3+ funds initiated for the first time
- Sector flow analysis: net buying/selling by sector
- Historically-aware top findings: z-score vs. fund's own baseline
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import date
from typing import TYPE_CHECKING

import yaml

from core.models import (
    CrossFundSignals,
    CrowdedTrade,
    FundBaseline,
    FundDiff,
    FundDivergence,
    FundInfo,
    Holding,
    PositionDiff,
)

if TYPE_CHECKING:
    from data.store import HoldingsStore

logger = logging.getLogger(__name__)


def aggregate_signals(
    fund_diffs: list[FundDiff],
    quarter: date,
    min_funds_for_crowd: int = 3,
    min_funds_for_consensus: int = 3,
    sector_data: dict[str, dict] | None = None,
) -> CrossFundSignals:
    """Aggregate position changes across all funds into cross-fund signals.

    Args:
        fund_diffs: List of FundDiff for each fund in the same quarter.
        quarter: The quarter being analyzed.
        min_funds_for_crowd: Minimum funds buying for a crowded trade signal.
        min_funds_for_consensus: Minimum funds for consensus initiation.

    Returns:
        CrossFundSignals with crowded trades, divergences, consensus initiations.
    """
    # Step 1: Build per-CUSIP action maps
    cusip_actions: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: {"initiated": [], "added": [], "trimmed": [], "exited": []}
    )
    # Store metadata from the most recent PositionDiff we see for each CUSIP
    cusip_metadata: dict[str, PositionDiff] = {}
    # Track aggregate dollar values and shares per CUSIP
    cusip_values: dict[str, int] = defaultdict(int)   # total value_thousands
    cusip_shares: dict[str, int] = defaultdict(int)    # total shares

    for diff in fund_diffs:
        fund_name = diff.fund.name

        for pos in diff.new_positions:
            cusip_actions[pos.cusip]["initiated"].append(fund_name)
            cusip_metadata[pos.cusip] = pos
            cusip_values[pos.cusip] += pos.current_value_thousands
            cusip_shares[pos.cusip] += pos.current_shares

        for pos in diff.added_positions:
            cusip_actions[pos.cusip]["added"].append(fund_name)
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos
            cusip_values[pos.cusip] += pos.current_value_thousands
            cusip_shares[pos.cusip] += pos.current_shares

        for pos in diff.trimmed_positions:
            cusip_actions[pos.cusip]["trimmed"].append(fund_name)
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos
            cusip_values[pos.cusip] += pos.current_value_thousands
            cusip_shares[pos.cusip] += pos.current_shares

        for pos in diff.exited_positions:
            cusip_actions[pos.cusip]["exited"].append(fund_name)
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos
            # Exited positions have zero current value — use prior
            cusip_values[pos.cusip] += pos.prior_value_thousands

        # Also track unchanged positions for aggregate float calculation
        for pos in diff.unchanged_positions:
            cusip_values[pos.cusip] += pos.current_value_thousands
            cusip_shares[pos.cusip] += pos.current_shares
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos

    # Step 2: Identify signals
    crowded: list[CrowdedTrade] = []
    divergences: list[FundDivergence] = []
    consensus: list[CrowdedTrade] = []

    for cusip, actions in cusip_actions.items():
        meta = cusip_metadata.get(cusip)
        total_buying = len(actions["initiated"]) + len(actions["added"])
        total_selling = len(actions["trimmed"]) + len(actions["exited"])

        # Float ownership calculation
        ticker = meta.ticker if meta else None
        float_shares = None
        float_pct = None
        if ticker and sector_data and ticker in sector_data:
            fs = sector_data[ticker].get("float_shares")
            if fs and fs > 0:
                float_shares = fs
                agg_shares = cusip_shares.get(cusip, 0)
                if agg_shares > 0:
                    float_pct = round(agg_shares / fs * 100, 2)

        trade = CrowdedTrade(
            cusip=cusip,
            ticker=ticker,
            issuer_name=meta.issuer_name if meta else "",
            sector=meta.sector if meta else None,
            themes=meta.themes if meta else [],
            funds_initiated=actions["initiated"],
            funds_added=actions["added"],
            funds_trimmed=actions["trimmed"],
            funds_exited=actions["exited"],
            net_fund_sentiment=total_buying - total_selling,
            aggregate_value_thousands=cusip_values.get(cusip, 0),
            aggregate_shares=cusip_shares.get(cusip, 0),
            float_shares=float_shares,
            float_ownership_pct=float_pct,
        )

        # Crowded trade: N+ funds net buying
        if total_buying >= min_funds_for_crowd:
            crowded.append(trade)

        # Consensus initiation: N+ funds started NEW position
        if len(actions["initiated"]) >= min_funds_for_consensus:
            consensus.append(trade)

        # Divergence: at least 1 initiated AND at least 1 exited
        if actions["initiated"] and actions["exited"]:
            divergences.append(
                FundDivergence(
                    cusip=cusip,
                    ticker=ticker,
                    issuer_name=meta.issuer_name if meta else "",
                    sector=meta.sector if meta else None,
                    initiated_by=actions["initiated"],
                    exited_by=actions["exited"],
                )
            )

    # Sort by signal strength
    crowded.sort(key=lambda t: t.net_fund_sentiment, reverse=True)
    consensus.sort(key=lambda t: len(t.funds_initiated), reverse=True)
    divergences.sort(
        key=lambda d: len(d.initiated_by) + len(d.exited_by), reverse=True
    )

    # Step 3: Sector flow analysis (fund-count and dollar-weighted)
    sector_flows = _compute_sector_flows(fund_diffs)
    sector_dollar_flows = _compute_sector_dollar_flows(fund_diffs)

    # Step 4: Crowding risk — all stocks where tracked funds own >5% of float
    crowding_risks = _compute_crowding_risks(
        cusip_metadata, cusip_values, cusip_shares, sector_data,
    )

    return CrossFundSignals(
        quarter=quarter,
        crowded_trades=crowded,
        divergences=divergences,
        consensus_initiations=consensus,
        sector_flows=sector_flows,
        sector_dollar_flows=sector_dollar_flows,
        crowding_risks=crowding_risks,
        funds_analyzed=len(fund_diffs),
    )


def _compute_sector_flows(
    fund_diffs: list[FundDiff],
) -> dict[str, dict[str, int]]:
    """Compute net fund buying/selling by sector.

    Returns:
        {sector: {"buying": count, "selling": count, "net": count}}
    """
    sector_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"buying": 0, "selling": 0, "net": 0}
    )

    for diff in fund_diffs:
        seen_sectors_buy: set[str] = set()
        seen_sectors_sell: set[str] = set()

        for pos in diff.new_positions + diff.added_positions:
            sector = pos.sector or "Unknown"
            if sector not in seen_sectors_buy:
                sector_counts[sector]["buying"] += 1
                sector_counts[sector]["net"] += 1
                seen_sectors_buy.add(sector)

        for pos in diff.exited_positions + diff.trimmed_positions:
            sector = pos.sector or "Unknown"
            if sector not in seen_sectors_sell:
                sector_counts[sector]["selling"] += 1
                sector_counts[sector]["net"] -= 1
                seen_sectors_sell.add(sector)

    return dict(sector_counts)


def _compute_sector_dollar_flows(
    fund_diffs: list[FundDiff],
) -> dict[str, dict[str, int]]:
    """Compute dollar-weighted sector flows.

    Returns:
        {sector: {"buying_k": int, "selling_k": int, "net_k": int}}
    """
    flows: dict[str, dict[str, int]] = defaultdict(
        lambda: {"buying_k": 0, "selling_k": 0, "net_k": 0}
    )

    for diff in fund_diffs:
        for pos in diff.new_positions + diff.added_positions:
            sector = pos.sector or "Unknown"
            val = pos.current_value_thousands
            flows[sector]["buying_k"] += val
            flows[sector]["net_k"] += val

        for pos in diff.exited_positions:
            sector = pos.sector or "Unknown"
            val = pos.prior_value_thousands
            flows[sector]["selling_k"] += val
            flows[sector]["net_k"] -= val

        for pos in diff.trimmed_positions:
            sector = pos.sector or "Unknown"
            val = abs(pos.value_change_thousands)
            flows[sector]["selling_k"] += val
            flows[sector]["net_k"] -= val

    return dict(flows)


def _compute_crowding_risks(
    cusip_metadata: dict[str, PositionDiff],
    cusip_values: dict[str, int],
    cusip_shares: dict[str, int],
    sector_data: dict[str, dict] | None,
    threshold_pct: float = 5.0,
) -> list[CrowdedTrade]:
    """Find stocks where tracked funds collectively own >threshold% of float.

    Returns CrowdedTrade objects sorted by float_ownership_pct descending.
    """
    if not sector_data:
        return []

    risks: list[CrowdedTrade] = []
    for cusip, meta in cusip_metadata.items():
        ticker = meta.ticker
        if not ticker or ticker not in sector_data:
            continue

        fs = sector_data[ticker].get("float_shares")
        if not fs or fs <= 0:
            continue

        agg_shares = cusip_shares.get(cusip, 0)
        if agg_shares <= 0:
            continue

        float_pct = round(agg_shares / fs * 100, 2)
        if float_pct < threshold_pct:
            continue

        risks.append(CrowdedTrade(
            cusip=cusip,
            ticker=ticker,
            issuer_name=meta.issuer_name,
            sector=meta.sector,
            aggregate_value_thousands=cusip_values.get(cusip, 0),
            aggregate_shares=agg_shares,
            float_shares=fs,
            float_ownership_pct=float_pct,
        ))

    risks.sort(key=lambda t: t.float_ownership_pct or 0, reverse=True)
    return risks


def tag_themes(
    diffs: list[PositionDiff],
    themes_path: str = "config/themes.yaml",
) -> None:
    """Tag position diffs with thematic labels from themes.yaml.

    Modifies diffs in place by setting the `themes` field.
    """
    try:
        with open(themes_path) as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("Themes file not found: %s", themes_path)
        return

    # Build ticker → theme list mapping
    ticker_themes: dict[str, list[str]] = defaultdict(list)
    for theme in data.get("themes", []):
        theme_name = theme["name"]
        for ticker in theme.get("tickers", []):
            ticker_themes[ticker.upper()].append(theme_name)

    # Tag each diff
    for diff in diffs:
        if diff.ticker and diff.ticker.upper() in ticker_themes:
            diff.themes = ticker_themes[diff.ticker.upper()]


# ---------------------------------------------------------------------------
# Shared-holdings analysis for Sankey visualization
# ---------------------------------------------------------------------------


def compute_most_widely_held(
    all_holdings: dict[str, list[Holding]],
    fund_lookup: dict[str, FundInfo],
    top_n: int = 20,
) -> list[dict]:
    """Identify the most widely held positions across funds.

    Args:
        all_holdings: {cik: [Holding, ...]} for one quarter.
        fund_lookup: {cik: FundInfo} for name resolution.
        top_n: How many top stocks to return.

    Returns:
        List of dicts sorted by fund_count desc:
        [{
            "cusip": str,
            "issuer_name": str,
            "fund_count": int,
            "funds": [{"name": str, "weight_pct": float,
                        "value_thousands": int}],
            "total_value_thousands": int,
        }]
    """
    # Aggregate across funds: cusip → list of (fund_name, holding, total_aum)
    cusip_map: dict[str, dict] = {}

    for cik, holdings in all_holdings.items():
        fund = fund_lookup.get(cik)
        if not fund:
            continue
        total_val = sum(h.value_thousands for h in holdings)
        if total_val == 0:
            continue

        for h in holdings:
            if h.is_option:
                continue  # Only equity
            key = h.cusip
            if key not in cusip_map:
                cusip_map[key] = {
                    "cusip": h.cusip,
                    "issuer_name": h.issuer_name,
                    "ticker": h.ticker,
                    "funds": [],
                    "total_value_thousands": 0,
                }
            # Prefer a non-None ticker if we find one
            if h.ticker and not cusip_map[key].get("ticker"):
                cusip_map[key]["ticker"] = h.ticker
            weight = h.value_thousands / total_val * 100
            cusip_map[key]["funds"].append({
                "name": fund.name,
                "weight_pct": round(weight, 2),
                "value_thousands": h.value_thousands,
            })
            cusip_map[key]["total_value_thousands"] += h.value_thousands

    # Count funds per stock, sort
    for entry in cusip_map.values():
        entry["fund_count"] = len(entry["funds"])

    results = sorted(
        cusip_map.values(),
        key=lambda x: x["fund_count"],
        reverse=True,
    )
    return results[:top_n]


# ---------------------------------------------------------------------------
# Top Findings — auto-extracted headline signals
# ---------------------------------------------------------------------------


def _fmt_val(thousands: int) -> str:
    """Format $thousands into readable string."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    return f"${dollars / 1_000:.0f}K"


def compute_top_findings(
    fund_diffs: list[FundDiff],
    signals: CrossFundSignals | None,
    n: int = 5,
    baselines: dict[str, FundBaseline] | None = None,
) -> list[dict]:
    """Compute the top N most interesting findings for the quarter.

    Each finding is a dict with keys:
        category: str  — one of the _FINDING_ICONS keys
        headline: str  — short bold label
        detail: str    — one-sentence explanation
        score: float   — internal ranking score (higher = more important)

    When *baselines* is provided, per-fund findings (activity,
    new-position sizing, concentration) are scored relative to the
    fund's own historical behaviour.  Cross-fund findings (consensus,
    crowded trades, divergences) are unaffected.

    Returns findings sorted by score descending, capped at n.
    """
    candidates: list[dict] = []

    if not fund_diffs:
        return []

    # --- 1. Consensus initiations (highest priority) ---
    if signals and signals.consensus_initiations:
        for ct in signals.consensus_initiations[:3]:
            n_funds = len(ct.funds_initiated)
            label = ct.display_label
            names = ", ".join(ct.funds_initiated[:4])
            if n_funds > 4:
                names += f" +{n_funds - 4}"
            candidates.append({
                "category": "crowded_buy",
                "headline": f"{label}: {n_funds} funds initiated",
                "detail": f"New consensus position opened by {names}.",
                "score": 100 + n_funds * 10,
                "ticker": ct.ticker,
            })

    # --- 2. Biggest crowded trade by net sentiment ---
    if signals and signals.crowded_trades:
        for ct in signals.crowded_trades[:2]:
            # Skip if already covered as consensus
            if signals.consensus_initiations and ct.cusip in {
                c.cusip for c in signals.consensus_initiations[:3]
            }:
                continue
            label = ct.display_label
            candidates.append({
                "category": "crowded_buy",
                "headline": f"{label}: {ct.total_funds_buying} funds buying",
                "detail": (
                    f"Net sentiment {ct.net_fund_sentiment:+d} "
                    f"({ct.total_funds_buying} buying vs. "
                    f"{ct.total_funds_selling} selling)."
                ),
                "score": 80 + ct.net_fund_sentiment * 5,
                "ticker": ct.ticker,
            })

    # --- 3. Top divergence ---
    if signals and signals.divergences:
        div = signals.divergences[0]
        label = div.display_label
        init_names = ", ".join(div.initiated_by[:2])
        exit_names = ", ".join(div.exited_by[:2])
        candidates.append({
            "category": "divergence",
            "headline": f"{label}: funds disagree",
            "detail": (
                f"Initiated by {init_names}; exited by {exit_names}. "
                "Funds are split on this name."
            ),
            "score": 75 + len(div.initiated_by) + len(div.exited_by),
            "ticker": div.ticker,
        })

    # --- 4. Most active fund (baseline-adjusted) ---
    activity = [
        (
            d,
            len(d.new_positions) + len(d.exited_positions)
            + sum(1 for p in d.added_positions if p.is_significant_add)
            + sum(1 for p in d.trimmed_positions if p.is_significant_trim),
        )
        for d in fund_diffs
    ]
    activity.sort(key=lambda x: x[1], reverse=True)
    if activity and activity[0][1] > 0:
        d, count = activity[0]
        base_score = 40 + min(count, 20)
        detail = (
            f"{len(d.new_positions)} new, "
            f"{len(d.exited_positions)} exits, "
            f"AUM {_fmt_val(d.current_aum_thousands)}."
        )

        if baselines and d.fund.cik in baselines:
            bl = baselines[d.fund.cik]
            z = bl.activity_zscore(count)
            base_score *= _baseline_multiplier(z)
            if z >= 2.0:
                detail += (
                    f" Unusually active "
                    f"({z:.1f}σ above avg of "
                    f"{bl.activity_mean:.0f} moves)."
                )
            elif z < 0.5:
                detail += (
                    f" Typical for this fund "
                    f"(avg {bl.activity_mean:.0f} moves/quarter)."
                )

        candidates.append({
            "category": "activity",
            "headline": f"{d.fund.name} most active ({count} moves)",
            "detail": detail,
            "score": base_score,
            "ticker": None,
        })

    # --- 5. Biggest single new position by weight (baseline-adjusted) ---
    best_new: tuple[FundDiff, PositionDiff] | None = None
    best_new_wt = 0.0
    for d in fund_diffs:
        for p in d.new_positions:
            if p.current_weight_pct > best_new_wt:
                best_new_wt = p.current_weight_pct
                best_new = (d, p)
    if best_new and best_new_wt >= 1.0:
        d, p = best_new
        base_score = 55 + best_new_wt * 5
        detail = (
            f"New {_fmt_val(p.current_value_thousands)} position — "
            f"high conviction sizing."
        )

        if baselines and d.fund.cik in baselines:
            bl = baselines[d.fund.cik]
            z = bl.new_position_zscore(best_new_wt)
            base_score *= _baseline_multiplier(z)
            if z >= 2.0:
                detail += (
                    f" Unusually large "
                    f"(typically sizes new positions at "
                    f"{bl.max_new_weight_mean:.1f}%)."
                )
            elif z < 0.5:
                detail += (
                    f" Normal sizing for this fund "
                    f"(avg {bl.max_new_weight_mean:.1f}%)."
                )

        candidates.append({
            "category": "new_position",
            "headline": (
                f"{d.fund.name} initiated {p.display_label} "
                f"at {p.current_weight_pct:.1f}%"
            ),
            "detail": detail,
            "score": base_score,
            "ticker": p.ticker,
        })

    # --- 6. Biggest concentration shift (baseline-adjusted) ---
    conc_shifts = sorted(
        fund_diffs,
        key=lambda d: abs(d.hhi_change),
        reverse=True,
    )
    if conc_shifts and abs(conc_shifts[0].hhi_change) > 0.005:
        d = conc_shifts[0]
        direction = "concentrating" if d.hhi_change > 0 else "diversifying"
        base_score = 35 + min(abs(d.hhi_change) * 5000, 20)
        detail = (
            f"Top-10 weight: {d.prior_top10_weight:.1%} → "
            f"{d.current_top10_weight:.1%}. "
            f"HHI moved {d.hhi_change * 10000:+.0f} bps."
        )

        if baselines and d.fund.cik in baselines:
            bl = baselines[d.fund.cik]
            z = bl.hhi_zscore(d.hhi_change)
            base_score *= _baseline_multiplier(z)
            if z >= 2.0:
                detail += (
                    f" Unusual shift "
                    f"(HHI typically moves ±"
                    f"{bl.hhi_change_mean * 10000:.0f} bps)."
                )
            elif z < 0.5:
                detail += (
                    f" Normal for this fund "
                    f"(avg ±{bl.hhi_change_mean * 10000:.0f} bps)."
                )

        candidates.append({
            "category": "concentration",
            "headline": f"{d.fund.name} {direction}",
            "detail": detail,
            "score": base_score,
            "ticker": None,
        })

    # Sort by score and return top N
    candidates.sort(key=lambda f: f["score"], reverse=True)
    return candidates[:n]


# ---------------------------------------------------------------------------
# Historical baseline computation
# ---------------------------------------------------------------------------


def _baseline_multiplier(zscore: float) -> float:
    """Convert z-score to a finding-score multiplier.

    Penalises "expected" behaviour (z < 0.5) and boosts genuinely
    unusual quarters (z > 1.5).
    """
    if zscore < 0.5:
        return 0.5
    if zscore < 1.0:
        return 0.8
    if zscore < 1.5:
        return 1.0
    if zscore < 2.0:
        return 1.3
    return 1.6


def compute_fund_baselines(
    store: HoldingsStore,
    fund_ciks: list[str],
    current_quarter: date,
    min_quarters: int = 3,
) -> dict[str, FundBaseline]:
    """Compute historical baselines for funds from SQLite data.

    For each fund, queries past quarter-pairs to establish typical
    activity counts, HHI shifts, and new-position sizing.  Funds
    with fewer than *min_quarters* historical pairs are omitted
    (they keep absolute scoring in compute_top_findings).

    Returns ``{cik: FundBaseline}`` for funds with enough history.
    """
    baselines: dict[str, FundBaseline] = {}

    for cik in fund_ciks:
        history = store.get_cross_quarter_activity(
            cik, exclude_quarter=current_quarter,
        )

        if len(history) < min_quarters:
            continue

        activity_vals = [
            h["new_positions"] + h["exited_positions"]
            for h in history
        ]
        hhi_vals = [abs(h["hhi_change"]) for h in history]
        new_wt_vals = [h["max_new_weight_pct"] for h in history]

        baselines[cik] = FundBaseline(
            cik=cik,
            quarters_available=len(history),
            activity_mean=statistics.mean(activity_vals),
            activity_std=(
                statistics.stdev(activity_vals)
                if len(activity_vals) >= 2 else 0.0
            ),
            hhi_change_mean=statistics.mean(hhi_vals),
            hhi_change_std=(
                statistics.stdev(hhi_vals)
                if len(hhi_vals) >= 2 else 0.0
            ),
            max_new_weight_mean=statistics.mean(new_wt_vals),
            max_new_weight_std=(
                statistics.stdev(new_wt_vals)
                if len(new_wt_vals) >= 2 else 0.0
            ),
        )

    return baselines

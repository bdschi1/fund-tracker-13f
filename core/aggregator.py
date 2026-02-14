"""Cross-fund signal aggregation.

Takes individual FundDiff objects across all watched funds and surfaces:
- Crowded trades: stocks multiple watched funds all added
- Divergences: Fund A initiated while Fund B exited
- Consensus initiations: stocks 3+ funds initiated for the first time
- Sector flow analysis: net buying/selling by sector
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date

import yaml

from core.models import (
    CrossFundSignals,
    CrowdedTrade,
    FundDiff,
    FundDivergence,
    FundInfo,
    Holding,
    PositionDiff,
)

logger = logging.getLogger(__name__)


def aggregate_signals(
    fund_diffs: list[FundDiff],
    quarter: date,
    min_funds_for_crowd: int = 3,
    min_funds_for_consensus: int = 3,
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

    for diff in fund_diffs:
        fund_name = diff.fund.name

        for pos in diff.new_positions:
            cusip_actions[pos.cusip]["initiated"].append(fund_name)
            cusip_metadata[pos.cusip] = pos

        for pos in diff.added_positions:
            cusip_actions[pos.cusip]["added"].append(fund_name)
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos

        for pos in diff.trimmed_positions:
            cusip_actions[pos.cusip]["trimmed"].append(fund_name)
            if pos.cusip not in cusip_metadata:
                cusip_metadata[pos.cusip] = pos

        for pos in diff.exited_positions:
            cusip_actions[pos.cusip]["exited"].append(fund_name)
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

        trade = CrowdedTrade(
            cusip=cusip,
            ticker=meta.ticker if meta else None,
            issuer_name=meta.issuer_name if meta else "",
            sector=meta.sector if meta else None,
            themes=meta.themes if meta else [],
            funds_initiated=actions["initiated"],
            funds_added=actions["added"],
            funds_trimmed=actions["trimmed"],
            funds_exited=actions["exited"],
            net_fund_sentiment=total_buying - total_selling,
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
                    ticker=meta.ticker if meta else None,
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

    # Step 3: Sector flow analysis
    sector_flows = _compute_sector_flows(fund_diffs)

    return CrossFundSignals(
        quarter=quarter,
        crowded_trades=crowded,
        divergences=divergences,
        consensus_initiations=consensus,
        sector_flows=sector_flows,
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
) -> list[dict]:
    """Compute the top N most interesting findings for the quarter.

    Each finding is a dict with keys:
        category: str  — one of the _FINDING_ICONS keys
        headline: str  — short bold label
        detail: str    — one-sentence explanation
        score: float   — internal ranking score (higher = more important)

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
        })

    # --- 4. Most active fund ---
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
        candidates.append({
            "category": "activity",
            "headline": f"{d.fund.name} most active ({count} moves)",
            "detail": (
                f"{len(d.new_positions)} new, "
                f"{len(d.exited_positions)} exits, "
                f"AUM {_fmt_val(d.current_aum_thousands)}."
            ),
            "score": 40 + min(count, 20),
        })

    # --- 6. Biggest single new position by weight ---
    best_new: tuple[FundDiff, PositionDiff] | None = None
    best_new_wt = 0.0
    for d in fund_diffs:
        for p in d.new_positions:
            if p.current_weight_pct > best_new_wt:
                best_new_wt = p.current_weight_pct
                best_new = (d, p)
    if best_new and best_new_wt >= 1.0:
        d, p = best_new
        candidates.append({
            "category": "new_position",
            "headline": (
                f"{d.fund.name} initiated {p.display_label} "
                f"at {p.current_weight_pct:.1f}%"
            ),
            "detail": (
                f"New {_fmt_val(p.current_value_thousands)} position — "
                f"high conviction sizing."
            ),
            "score": 55 + best_new_wt * 5,
        })

    # --- 7. Biggest concentration shift ---
    conc_shifts = sorted(
        fund_diffs,
        key=lambda d: abs(d.hhi_change),
        reverse=True,
    )
    if conc_shifts and abs(conc_shifts[0].hhi_change) > 0.005:
        d = conc_shifts[0]
        direction = "concentrating" if d.hhi_change > 0 else "diversifying"
        candidates.append({
            "category": "concentration",
            "headline": f"{d.fund.name} {direction}",
            "detail": (
                f"Top-10 weight: {d.prior_top10_weight:.1%} → "
                f"{d.current_top10_weight:.1%}. "
                f"HHI moved {d.hhi_change * 10000:+.0f} bps."
            ),
            "score": 35 + min(abs(d.hhi_change) * 5000, 20),
        })

    # Sort by score and return top N
    candidates.sort(key=lambda f: f["score"], reverse=True)
    return candidates[:n]

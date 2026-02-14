"""Markdown report generator for 13F analysis.

Produces structured markdown from FundDiff and CrossFundSignals objects.
No UI dependencies — can be used from CLI or Streamlit export.
"""

from __future__ import annotations

from datetime import date, datetime

from core.aggregator import compute_top_findings
from core.models import (
    CrossFundSignals,
    FundBaseline,
    FundDiff,
)


def generate_quarterly_report(
    fund_diffs: list[FundDiff],
    signals: CrossFundSignals,
    quarter: date,
    include_fund_details: bool = True,
    max_positions_per_section: int = 15,
    baselines: dict[str, FundBaseline] | None = None,
) -> str:
    """Generate a full markdown report for one quarter.

    Sections:
    1. Executive Summary
    2. Cross-Fund Signals (consensus, crowded, divergences)
    3. Sector Flows
    4. Individual Fund Summaries (optional)
    5. Data Quality Notes
    """
    lines: list[str] = []

    # Header
    q_label = _quarter_label(quarter)
    lines.append(f"# 13F Fund Tracker Report — {q_label}")
    lines.append("")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("")

    # Executive Summary
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- **Quarter**: {q_label} (ending {quarter})")
    lines.append(f"- **Funds Analyzed**: {signals.funds_analyzed}")
    stale_count = sum(1 for d in fund_diffs if d.is_stale)
    if stale_count:
        lines.append(f"- **Stale Filings**: {stale_count} filed 50+ days after quarter end")
    lines.append(f"- **Consensus Initiations**: {len(signals.consensus_initiations)}")
    lines.append(f"- **Crowded Trades**: {len(signals.crowded_trades)}")
    lines.append(f"- **Divergences**: {len(signals.divergences)}")
    lines.append("")

    # Top Findings
    _FINDING_ICONS = {
        "crowded_buy": "🟢",
        "crowded_sell": "🔴",
        "divergence": "🔀",
        "concentration": "📊",
        "new_position": "🆕",
        "exit": "🚪",
        "activity": "⚡",
    }
    findings = compute_top_findings(
        fund_diffs, signals, n=5, baselines=baselines,
    )
    if findings:
        lines.append("### 🔍 Top Findings")
        lines.append("")
        for i, f in enumerate(findings, 1):
            icon = _FINDING_ICONS.get(f["category"], "•")
            lines.append(f"{i}. {icon} **{f['headline']}** — {f['detail']}")
        lines.append("")

    # Cross-Fund Signals
    lines.append("---")
    lines.append("")
    lines.append("## Cross-Fund Signals")
    lines.append("")

    # Consensus Initiations
    if signals.consensus_initiations:
        lines.append("### Consensus New Positions")
        lines.append("")
        lines.append("Stocks that 3+ watched funds initiated for the first time this quarter.")
        lines.append("")
        lines.append("| Stock | Funds Initiated | Funds Added | Sector | Themes |")
        lines.append("|-------|----------------|-------------|--------|--------|")
        for ct in signals.consensus_initiations[:max_positions_per_section]:
            stock = ct.display_label
            init_count = len(ct.funds_initiated)
            init_names = ", ".join(ct.funds_initiated[:5])
            if len(ct.funds_initiated) > 5:
                init_names += f" +{len(ct.funds_initiated) - 5}"
            added = len(ct.funds_added)
            sector = ct.sector or "—"
            themes = ", ".join(ct.themes[:2]) if ct.themes else "—"
            row = (
                f"| **{stock}** | {init_count} ({init_names}) "
                f"| {added} | {sector} | {themes} |"
            )
            lines.append(row)
        lines.append("")

    # Crowded Trades
    if signals.crowded_trades:
        lines.append("### Crowded Trades (3+ Funds Buying)")
        lines.append("")
        lines.append("| Stock | Buying | Selling | Net | Sector |")
        lines.append("|-------|--------|---------|-----|--------|")
        for ct in signals.crowded_trades[:max_positions_per_section]:
            stock = ct.display_label
            lines.append(
                f"| **{stock}** | {ct.total_funds_buying} | "
                f"{ct.total_funds_selling} | {ct.net_fund_sentiment:+d} | "
                f"{ct.sector or '—'} |"
            )
        lines.append("")

    # Divergences
    if signals.divergences:
        lines.append("### Divergences (One In, One Out)")
        lines.append("")
        lines.append("| Stock | Initiated By | Exited By | Sector |")
        lines.append("|-------|-------------|-----------|--------|")
        for div in signals.divergences[:max_positions_per_section]:
            stock = div.display_label
            init = ", ".join(div.initiated_by[:3])
            exit_ = ", ".join(div.exited_by[:3])
            lines.append(f"| **{stock}** | {init} | {exit_} | {div.sector or '—'} |")
        lines.append("")

    # Crowding Risk
    if signals.crowding_risks:
        lines.append("---")
        lines.append("")
        lines.append("## 🚨 Crowding Risk (Float Ownership)")
        lines.append("")
        lines.append(
            "Stocks where tracked funds collectively own ≥ 5% of the public float. "
            "High float ownership creates liquidation risk."
        )
        lines.append("")
        lines.append("| Stock | Float % | Agg. Value | Sector |")
        lines.append("|-------|---------|-----------|--------|")
        for ct in signals.crowding_risks[:max_positions_per_section]:
            stock = ct.display_label
            fp = f"{ct.float_ownership_pct:.1f}%" if ct.float_ownership_pct else "—"
            val = _fmt_value(ct.aggregate_value_thousands)
            lines.append(
                f"| **{stock}** | {fp} | {val} | {ct.sector or '—'} |"
            )
        lines.append("")

    # Sector Flows
    if signals.sector_flows:
        lines.append("---")
        lines.append("")
        lines.append("## Sector Flows")
        lines.append("")
        lines.append("| Sector | Funds Buying | Funds Selling | Net |")
        lines.append("|--------|-------------|---------------|-----|")
        sorted_sectors = sorted(
            signals.sector_flows.items(),
            key=lambda x: abs(x[1]["net"]),
            reverse=True,
        )
        for sector, counts in sorted_sectors:
            if sector == "Unknown":
                continue
            net = counts["net"]
            arrow = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
            lines.append(
                f"| {sector} | {counts['buying']} | {counts['selling']} | "
                f"{arrow} {net:+d} |"
            )
        lines.append("")

    # Dollar-weighted sector flows
    if signals.sector_dollar_flows:
        lines.append("### Dollar-Weighted Sector Flows")
        lines.append("")
        lines.append("| Sector | Buying | Selling | Net Flow |")
        lines.append("|--------|--------|---------|----------|")
        sorted_dollar = sorted(
            signals.sector_dollar_flows.items(),
            key=lambda x: abs(x[1]["net_k"]),
            reverse=True,
        )
        for sector, counts in sorted_dollar:
            if sector == "Unknown":
                continue
            net = counts["net_k"]
            arrow = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
            lines.append(
                f"| {sector} | {_fmt_value(counts['buying_k'])} | "
                f"{_fmt_value(counts['selling_k'])} | "
                f"{arrow} {_fmt_value(net)} |"
            )
        lines.append("")

    # Individual Fund Summaries
    if include_fund_details and fund_diffs:
        lines.append("---")
        lines.append("")
        lines.append("## Individual Fund Summaries")
        lines.append("")

        for diff in sorted(fund_diffs, key=lambda d: d.fund.name):
            lines.append(f"### {diff.fund.name} ({diff.fund.tier.value})")
            lines.append("")
            lines.append(
                f"- **AUM**: {_fmt_value(diff.current_aum_thousands)}"
            )
            lines.append(
                f"- **Filing Lag**: {diff.filing_lag_days} days"
                + (" ⚠️ STALE" if diff.is_stale else "")
            )
            lines.append(
                f"- **Top-10 Concentration**: {diff.current_top10_weight:.1%} "
                f"(was {diff.prior_top10_weight:.1%})"
            )
            lines.append(
                f"- **Positions**: {len(diff.new_positions)} new, "
                f"{len(diff.exited_positions)} exited, "
                f"{len(diff.added_positions)} added, "
                f"{len(diff.trimmed_positions)} trimmed"
            )
            lines.append("")

            # New positions
            if diff.new_positions:
                lines.append("**New Positions:**")
                lines.append("")
                for pos in diff.new_positions[:10]:
                    lines.append(
                        f"- {pos.display_label}: {_fmt_value(pos.current_value_thousands)} "
                        f"({pos.current_weight_pct:.1f}% of AUM)"
                    )
                lines.append("")

            # Exited positions
            if diff.exited_positions:
                lines.append("**Exited Positions:**")
                lines.append("")
                for pos in diff.exited_positions[:10]:
                    lines.append(
                        f"- {pos.display_label}: was {_fmt_value(pos.prior_value_thousands)} "
                        f"({pos.prior_weight_pct:.1f}% of AUM)"
                    )
                lines.append("")

            # Top adds by %
            sig_adds = [p for p in diff.added_positions if p.is_significant_add]
            if sig_adds:
                lines.append("**Significant Adds (50%+ increase):**")
                lines.append("")
                for pos in sig_adds[:10]:
                    lines.append(
                        f"- {pos.display_label}: {_fmt_pct(pos.shares_change_pct)} shares "
                        f"({_fmt_value(pos.prior_value_thousands)} → "
                        f"{_fmt_value(pos.current_value_thousands)}, "
                        f"weight {pos.prior_weight_pct:.1f}% → {pos.current_weight_pct:.1f}%)"
                    )
                lines.append("")

            # Top trims by %
            sig_trims = [p for p in diff.trimmed_positions if p.is_significant_trim]
            if sig_trims:
                lines.append("**Significant Trims (60%+ decrease):**")
                lines.append("")
                for pos in sig_trims[:10]:
                    lines.append(
                        f"- {pos.display_label}: {_fmt_pct(pos.shares_change_pct)} shares "
                        f"({_fmt_value(pos.prior_value_thousands)} → "
                        f"{_fmt_value(pos.current_value_thousands)})"
                    )
                lines.append("")

            lines.append("---")
            lines.append("")

    # Data Quality Notes
    stale_filings = [d for d in fund_diffs if d.is_stale]
    if stale_filings:
        lines.append("## Data Quality Notes")
        lines.append("")
        lines.append("**Stale Filings (50+ days after quarter end):**")
        lines.append("")
        for d in stale_filings:
            lines.append(
                f"- {d.fund.name}: filed {d.filing_lag_days} days late "
                f"(filed {d.filing_date})"
            )
        lines.append("")

    return "\n".join(lines)


def generate_single_fund_report(diff: FundDiff) -> str:
    """Generate markdown for a single fund's quarter analysis."""
    lines: list[str] = []
    lines.append(f"# {diff.fund.name} — {_quarter_label(diff.current_quarter)}")
    lines.append("")
    lines.append(f"**AUM**: {_fmt_value(diff.current_aum_thousands)}")
    lines.append(f"**Filing Date**: {diff.filing_date} "
                 f"({diff.filing_lag_days} days after quarter end)")
    lines.append(f"**Top-10 Weight**: {diff.current_top10_weight:.1%}")
    lines.append("")

    sections = [
        ("New Positions", diff.new_positions),
        ("Exited Positions", diff.exited_positions),
        ("Added Positions", diff.added_positions),
        ("Trimmed Positions", diff.trimmed_positions),
    ]

    for title, positions in sections:
        if not positions:
            continue
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| Stock | Value | Weight | Share Δ% | Weight Δ |")
        lines.append("|-------|-------|--------|---------|----------|")
        for pos in positions[:20]:
            val = pos.current_value_thousands or pos.prior_value_thousands
            lines.append(
                f"| {pos.display_label} | {_fmt_value(val)} | "
                f"{pos.current_weight_pct:.1f}% | "
                f"{_fmt_pct(pos.shares_change_pct)} | "
                f"{pos.weight_change_pct:+.1f}pp |"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_value(thousands: int) -> str:
    """Format $thousands into human-readable: $1.2B, $345.6M, $12.3M, etc."""
    dollars = thousands * 1000
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if abs(dollars) >= 1_000:
        return f"${dollars / 1_000:.0f}K"
    return f"${dollars:.0f}"


def _fmt_pct(value: float) -> str:
    """Format as percentage with sign."""
    return f"{value:+.1%}"


def _quarter_label(d: date) -> str:
    """Convert date to quarter label like 'Q4 2025'."""
    quarter = (d.month - 1) // 3 + 1
    return f"Q{quarter} {d.year}"

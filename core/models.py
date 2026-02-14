"""Domain models for 13F filing analysis.

Pure data structures with computed properties. All business logic
lives in core/diff_engine.py, core/options_filter.py, core/aggregator.py.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


def _shorten_issuer(name: str) -> str:
    """Strip common corporate suffixes from SEC issuer names.

    Turns 'MOLINA HEALTHCARE INC' → 'MOLINA HEALTHCARE',
    'NVIDIA CORP' → 'NVIDIA', etc.  Falls through gracefully
    if the name is already short.
    """
    import re

    # Common suffixes found in 13F issuer names (order matters — longer first)
    _SUFFIXES = (
        r"\bFORMERLY\b.*$",
        r"\bHOLDINGS?\b",
        r"\bHLDGS?\b",
        r"\bGROUP\b",
        r"\bINCORPORATED\b",
        r"\bCORPORATION\b",
        r"\bINC\.?\b",
        r"\bCORP\.?\b",
        r"\bLTD\.?\b",
        r"\bLLC\.?\b",
        r"\bL\.?P\.?\b",
        r"\bPLC\.?\b",
        r"\bN\.?V\.?\b",
        r"\bS\.?A\.?\b",
        r"\bCO\.?\b",
        r"\bTECHNOLOGIES\b",
        r"\bTECH\b",
        r"\bENTERPRISES?\b",
        r"\bINTERNATIONAL\b",
        r"\bINTL\b",
        r"\bSOLUTIONS?\b",
        r"\bSYSTEMS?\b",
        r"\bSERVICES?\b",
        r"\bCOMMS?\b",
        r"\bCOMMUNICATIONS?\b",
        r"\bCL [A-Z]$",       # "CL A", "CL B" share class
        r"\bCLASS [A-Z]$",    # "CLASS A"
        r"\bSHS\b",           # "SHS" (shares)
        r"\bCOM\b",           # "COM" (common)
        r"\bNEW\b",
        r"[/-]+\s*$",         # trailing slashes/dashes
    )
    result = name.strip()
    for pat in _SUFFIXES:
        result = re.sub(pat, "", result, flags=re.IGNORECASE).strip()
    # Collapse multiple spaces and strip trailing punctuation
    result = re.sub(r"\s{2,}", " ", result).strip()
    result = result.rstrip(" .,;:-/")
    # If we stripped everything, fall back to original
    return result if result else name.strip()


class Tier(str, Enum):
    """Fund classification tier."""

    A = "A"  # Multi-Strat (filter to top positions)
    B = "B"  # Stock Pickers / Tiger Cubs
    C = "C"  # Event-Driven / Activist
    D = "D"  # Emerging / Newer
    E = "E"  # Healthcare Specialists


class PositionChangeType(str, Enum):
    """How a position changed quarter-over-quarter."""

    NEW = "NEW"              # Initiated this quarter
    EXITED = "EXITED"        # Sold to zero
    ADDED = "ADDED"          # Increased shares
    TRIMMED = "TRIMMED"      # Decreased shares
    UNCHANGED = "UNCHANGED"  # Same share count


# ---------------------------------------------------------------------------
# Fund Metadata
# ---------------------------------------------------------------------------


class FundInfo(BaseModel):
    """Static fund metadata from watchlist.yaml."""

    name: str
    cik: str
    tier: Tier
    aliases: list[str] = Field(default_factory=list)

    @property
    def cik_padded(self) -> str:
        """10-digit zero-padded CIK for EDGAR URLs."""
        return self.cik.zfill(10)


# ---------------------------------------------------------------------------
# Holdings
# ---------------------------------------------------------------------------


class Holding(BaseModel):
    """A single position from a 13F filing."""

    cusip: str
    issuer_name: str
    title_of_class: str
    value_thousands: int              # Value in $1000s as reported
    shares_or_prn_amt: int            # Share count or principal amount
    sh_prn_type: Literal["SH", "PRN"] = "SH"
    put_call: Literal["PUT", "CALL"] | None = None
    investment_discretion: str = "SOLE"

    @field_validator("investment_discretion", mode="before")
    @classmethod
    def normalize_discretion(cls, v: str) -> str:
        """Normalize SEC investment discretion abbreviations."""
        mapping = {
            "SOLE": "SOLE",
            "SHARED": "SHARED",
            "DEFINED": "DEFINED",
            "DFND": "DEFINED",  # Common abbreviation in filings
            "OTR": "OTHER",
        }
        return mapping.get(v.upper().strip(), v.upper().strip()) if v else "SOLE"
    voting_authority_sole: int = 0
    voting_authority_shared: int = 0
    voting_authority_none: int = 0

    # Enrichment fields (populated after parsing)
    ticker: str | None = None
    sector: str | None = None
    industry: str | None = None

    @property
    def value_dollars(self) -> int:
        """Market value in actual dollars."""
        return self.value_thousands * 1000

    @property
    def is_option(self) -> bool:
        """True if this is a PUT or CALL position."""
        return self.put_call is not None

    @property
    def is_equity(self) -> bool:
        """True if this is a plain equity (non-option, share-based) position."""
        return self.put_call is None and self.sh_prn_type == "SH"

    @property
    def issuer_cusip_prefix(self) -> str:
        """First 6 chars of CUSIP — identifies the issuer across equity/options."""
        return self.cusip[:6]

    @property
    def display_label(self) -> str:
        """Human-readable label: TICKER or shortened ISSUER_NAME, with [PUT]/[CALL] if option."""
        base = self.ticker or _shorten_issuer(self.issuer_name)
        if self.put_call:
            return f"{base} [{self.put_call}]"
        return base


class FundHoldings(BaseModel):
    """All holdings for one fund in one quarter."""

    fund: FundInfo
    quarter_end: date
    filing_date: date
    report_date: date
    holdings: list[Holding]
    total_value_thousands: int = 0

    @model_validator(mode="after")
    def compute_total(self) -> FundHoldings:
        if self.total_value_thousands == 0 and self.holdings:
            self.total_value_thousands = sum(h.value_thousands for h in self.holdings)
        return self

    @property
    def total_value_dollars(self) -> int:
        return self.total_value_thousands * 1000

    @property
    def filing_lag_days(self) -> int:
        """Days between quarter end and filing date."""
        return (self.filing_date - self.quarter_end).days

    @property
    def position_count(self) -> int:
        return len(self.holdings)

    @property
    def equity_holdings(self) -> list[Holding]:
        """Only equity (non-option) positions."""
        return [h for h in self.holdings if h.is_equity]

    @property
    def option_holdings(self) -> list[Holding]:
        """Only options positions."""
        return [h for h in self.holdings if h.is_option]

    def get_holding_by_cusip(
        self, cusip: str, put_call: str | None = None
    ) -> Holding | None:
        """Find a specific holding by CUSIP and optionally put/call type."""
        for h in self.holdings:
            if h.cusip == cusip and h.put_call == put_call:
                return h
        return None

    def portfolio_weight(self, holding: Holding) -> float:
        """Position weight as fraction of total AUM (0.0 to 1.0)."""
        if self.total_value_thousands == 0:
            return 0.0
        return holding.value_thousands / self.total_value_thousands

    def holdings_by_issuer(self, cusip_prefix: str) -> list[Holding]:
        """All holdings for an issuer (equity + options) by CUSIP prefix."""
        return [h for h in self.holdings if h.issuer_cusip_prefix == cusip_prefix]


# ---------------------------------------------------------------------------
# Position Diffs
# ---------------------------------------------------------------------------


class PositionDiff(BaseModel):
    """Quarter-over-quarter change for a single position."""

    cusip: str
    ticker: str | None = None
    issuer_name: str
    put_call: Literal["PUT", "CALL"] | None = None
    sector: str | None = None
    themes: list[str] = Field(default_factory=list)

    # Current quarter values
    current_shares: int = 0
    current_value_thousands: int = 0
    current_weight_pct: float = 0.0      # % of AUM (e.g., 3.5 = 3.5%)

    # Prior quarter values
    prior_shares: int = 0
    prior_value_thousands: int = 0
    prior_weight_pct: float = 0.0

    # Computed change metrics
    change_type: PositionChangeType
    shares_change: int = 0               # Absolute share change
    shares_change_pct: float = 0.0       # % change in shares (e.g., 1.0 = doubled)
    value_change_thousands: int = 0
    weight_change_pct: float = 0.0       # Change in portfolio weight (percentage points)

    # Price enrichment (populated later)
    quarter_end_price: float | None = None
    current_price: float | None = None
    price_change_since_quarter: float | None = None  # % change

    # Context flags
    is_options_position: bool = False
    options_filter_action: Literal["INCLUDE", "EXCLUDE", "FLAG"] = "FLAG"

    @property
    def display_label(self) -> str:
        """Human-readable label with optional [PUT]/[CALL]."""
        base = self.ticker or _shorten_issuer(self.issuer_name)
        if self.put_call:
            return f"{base} [{self.put_call}]"
        return base

    @property
    def is_significant_add(self) -> bool:
        """Position increased by 50%+ in shares AND is ≥ 0.25% of AUM.

        The weight gate filters out micro-positions: a $10B fund doubling
        a $1M position (0.01% weight) is noise, not conviction.
        """
        return (
            self.change_type == PositionChangeType.ADDED
            and self.shares_change_pct >= 0.50
            and self.current_weight_pct >= 0.25
        )

    @property
    def is_significant_trim(self) -> bool:
        """Position decreased by 60%+ in shares AND was ≥ 0.25% of AUM.

        Requires the *prior* weight to be meaningful — cutting a tiny
        position by 80% is not informative.
        """
        return (
            self.change_type == PositionChangeType.TRIMMED
            and self.shares_change_pct <= -0.60
            and self.prior_weight_pct >= 0.25
        )

    @property
    def current_value_dollars(self) -> int:
        return self.current_value_thousands * 1000

    @property
    def prior_value_dollars(self) -> int:
        return self.prior_value_thousands * 1000

    @property
    def value_change_dollars(self) -> int:
        return self.value_change_thousands * 1000


class FundDiff(BaseModel):
    """Complete quarter-over-quarter diff for one fund."""

    fund: FundInfo
    current_quarter: date
    prior_quarter: date
    filing_date: date
    filing_lag_days: int

    # AUM context
    current_aum_thousands: int
    prior_aum_thousands: int
    aum_change_pct: float

    # Categorized position changes (sorted by signal strength)
    new_positions: list[PositionDiff]         # Sorted by value desc
    exited_positions: list[PositionDiff]      # Sorted by prior value desc
    added_positions: list[PositionDiff]       # Sorted by shares_change_pct desc
    trimmed_positions: list[PositionDiff]     # Sorted by shares_change_pct asc
    unchanged_positions: list[PositionDiff]

    # Concentration metrics
    current_hhi: float
    prior_hhi: float
    hhi_change: float
    current_top10_weight: float
    prior_top10_weight: float

    @property
    def all_changes(self) -> list[PositionDiff]:
        """All positions that changed (excludes unchanged)."""
        return (
            self.new_positions
            + self.exited_positions
            + self.added_positions
            + self.trimmed_positions
        )

    @property
    def is_stale(self) -> bool:
        """Filing is 50+ days after quarter end (unusually late)."""
        return self.filing_lag_days > 50

    @property
    def total_new_value_thousands(self) -> int:
        return sum(p.current_value_thousands for p in self.new_positions)

    @property
    def total_exited_value_thousands(self) -> int:
        return sum(p.prior_value_thousands for p in self.exited_positions)


# ---------------------------------------------------------------------------
# Cross-Fund Signals
# ---------------------------------------------------------------------------


class CrowdedTrade(BaseModel):
    """A stock that multiple watched funds acted on in the same direction."""

    cusip: str
    ticker: str | None = None
    issuer_name: str
    sector: str | None = None
    themes: list[str] = Field(default_factory=list)

    funds_initiated: list[str] = Field(default_factory=list)
    funds_added: list[str] = Field(default_factory=list)
    funds_trimmed: list[str] = Field(default_factory=list)
    funds_exited: list[str] = Field(default_factory=list)
    net_fund_sentiment: int = 0  # (initiated + added) - (trimmed + exited)

    # Dollar-weighted metrics
    aggregate_value_thousands: int = 0  # Total $ across all tracked funds
    aggregate_shares: int = 0           # Total shares across all tracked funds

    # Float ownership (populated when sector data available)
    float_shares: int | None = None
    float_ownership_pct: float | None = None  # % of float held by tracked funds

    @property
    def total_funds_buying(self) -> int:
        return len(self.funds_initiated) + len(self.funds_added)

    @property
    def total_funds_selling(self) -> int:
        return len(self.funds_trimmed) + len(self.funds_exited)

    @property
    def display_label(self) -> str:
        return self.ticker or _shorten_issuer(self.issuer_name)

    @property
    def aggregate_value_dollars(self) -> int:
        return self.aggregate_value_thousands * 1000

    @property
    def is_crowding_risk(self) -> bool:
        """True if tracked funds collectively own >5% of float."""
        return self.float_ownership_pct is not None and self.float_ownership_pct >= 5.0


class FundDivergence(BaseModel):
    """A stock where one fund initiated and another exited same quarter."""

    cusip: str
    ticker: str | None = None
    issuer_name: str
    sector: str | None = None
    initiated_by: list[str]
    exited_by: list[str]

    @property
    def display_label(self) -> str:
        return self.ticker or _shorten_issuer(self.issuer_name)


class ConvictionTrack(BaseModel):
    """Historical conviction tracking for a fund-position pair."""

    fund_name: str
    cusip: str
    ticker: str | None = None
    issuer_name: str
    quarters_held: int
    consecutive_adds: int = 0
    consecutive_trims: int = 0
    weight_history: list[float] = Field(default_factory=list)
    shares_history: list[int] = Field(default_factory=list)

    @property
    def conviction_score(self) -> float:
        """Higher = more conviction. Based on quarters held + consistent adding."""
        return self.quarters_held + (self.consecutive_adds * 1.5)


class FundBaseline(BaseModel):
    """Historical baseline stats for one fund, computed from past quarters.

    Used by compute_top_findings() to penalize expected behavior
    and boost genuinely surprising activity.
    """

    cik: str
    quarters_available: int  # How many historical quarter-pairs this is based on

    # Activity baseline (new + exits per quarter)
    activity_mean: float
    activity_std: float

    # HHI change baseline (absolute magnitude per quarter)
    hhi_change_mean: float
    hhi_change_std: float

    # New position sizing baseline (max new position weight % per quarter)
    max_new_weight_mean: float
    max_new_weight_std: float

    def activity_zscore(self, current_activity: int) -> float:
        """Z-score for this quarter's activity count."""
        if self.activity_std == 0:
            return 0.0
        return (current_activity - self.activity_mean) / self.activity_std

    def hhi_zscore(self, current_hhi_change: float) -> float:
        """Z-score for this quarter's absolute HHI change."""
        if self.hhi_change_std == 0:
            return 0.0
        return (abs(current_hhi_change) - self.hhi_change_mean) / self.hhi_change_std

    def new_position_zscore(self, current_max_weight: float) -> float:
        """Z-score for this quarter's largest new position weight."""
        if self.max_new_weight_std == 0:
            return 0.0
        return (current_max_weight - self.max_new_weight_mean) / self.max_new_weight_std


class CrossFundSignals(BaseModel):
    """Aggregated cross-fund signals for one quarter."""

    quarter: date
    crowded_trades: list[CrowdedTrade]
    divergences: list[FundDivergence]
    consensus_initiations: list[CrowdedTrade]
    sector_flows: dict[str, dict[str, int]] = Field(default_factory=dict)
    # Dollar-weighted sector flows: {sector: {"buying_k": int, "selling_k": int, "net_k": int}}
    sector_dollar_flows: dict[str, dict[str, int]] = Field(default_factory=dict)
    # Crowding risk flags (stocks with >5% float owned by tracked funds)
    crowding_risks: list[CrowdedTrade] = Field(default_factory=list)
    funds_analyzed: int
    generated_at: datetime = Field(default_factory=datetime.now)

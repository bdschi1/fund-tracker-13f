"""SQLite persistence layer for 13F holdings data.

Stores historical holdings, fund metadata, CUSIP mappings, sector data,
prices, and filing index. Single database file for all data.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path

from core.models import FundHoldings, FundInfo, Holding, Tier

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data_cache/fund_tracker.db")

SCHEMA_SQL = """
-- Fund metadata (populated from watchlist.yaml)
CREATE TABLE IF NOT EXISTS funds (
    cik TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    last_filing_date TEXT,
    last_quarter TEXT
);

-- Raw 13F holdings (one row per position per quarter per fund)
CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL,
    quarter_end TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT NOT NULL,
    title_of_class TEXT NOT NULL,
    value_thousands INTEGER NOT NULL,
    shares_or_prn_amt INTEGER NOT NULL,
    sh_prn_type TEXT NOT NULL DEFAULT 'SH',
    put_call TEXT,
    investment_discretion TEXT DEFAULT 'SOLE',
    voting_sole INTEGER DEFAULT 0,
    voting_shared INTEGER DEFAULT 0,
    voting_none INTEGER DEFAULT 0,
    fetched_at TEXT NOT NULL,
    UNIQUE(cik, quarter_end, cusip, put_call)
);

CREATE INDEX IF NOT EXISTS idx_holdings_cik_quarter
    ON holdings (cik, quarter_end);
CREATE INDEX IF NOT EXISTS idx_holdings_cusip
    ON holdings (cusip);
CREATE INDEX IF NOT EXISTS idx_holdings_quarter
    ON holdings (quarter_end);

-- CUSIP to ticker mapping (populated via OpenFIGI)
CREATE TABLE IF NOT EXISTS cusip_map (
    cusip TEXT PRIMARY KEY,
    ticker TEXT,
    name TEXT,
    exchange TEXT,
    market_sector TEXT,
    fetched_at TEXT NOT NULL
);

-- Sector/industry enrichment (populated via yfinance)
CREATE TABLE IF NOT EXISTS sector_map (
    ticker TEXT PRIMARY KEY,
    sector TEXT,
    industry TEXT,
    market_cap REAL,
    shares_outstanding INTEGER,
    float_shares INTEGER,
    fetched_at TEXT NOT NULL
);

-- Price cache (quarter-end and current prices)
CREATE TABLE IF NOT EXISTS prices (
    ticker TEXT NOT NULL,
    price_date TEXT NOT NULL,
    close_price REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (ticker, price_date)
);

-- Price performance cache (1w, 1m, YTD, 1yr returns)
CREATE TABLE IF NOT EXISTS price_performance (
    ticker TEXT PRIMARY KEY,
    current_price REAL,
    return_1w REAL,
    return_1m REAL,
    return_ytd REAL,
    return_1yr REAL,
    fetched_at TEXT NOT NULL
);

-- Filing metadata (tracks which filings have been processed)
CREATE TABLE IF NOT EXISTS filing_index (
    cik TEXT NOT NULL,
    accession_number TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    report_date TEXT NOT NULL,
    quarter_end TEXT NOT NULL,
    form_type TEXT NOT NULL,
    primary_doc TEXT NOT NULL,
    processed_at TEXT,
    holdings_count INTEGER DEFAULT 0,
    total_value_thousands INTEGER DEFAULT 0,
    PRIMARY KEY (cik, accession_number)
);
"""


class HoldingsStore:
    """SQLite persistence for 13F holdings data."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_db()

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Fund metadata
    # ------------------------------------------------------------------

    def upsert_fund(self, fund: FundInfo) -> None:
        """Insert or update fund metadata."""
        self._conn.execute(
            """INSERT INTO funds (cik, name, tier)
               VALUES (?, ?, ?)
               ON CONFLICT(cik) DO UPDATE SET name=excluded.name, tier=excluded.tier""",
            (fund.cik, fund.name, fund.tier.value),
        )
        self._conn.commit()

    def upsert_funds(self, funds: list[FundInfo]) -> None:
        """Bulk insert/update fund metadata."""
        self._conn.executemany(
            """INSERT INTO funds (cik, name, tier)
               VALUES (?, ?, ?)
               ON CONFLICT(cik) DO UPDATE SET name=excluded.name, tier=excluded.tier""",
            [(f.cik, f.name, f.tier.value) for f in funds],
        )
        self._conn.commit()

    def get_fund(self, cik: str) -> FundInfo | None:
        """Get fund metadata by CIK."""
        row = self._conn.execute(
            "SELECT cik, name, tier FROM funds WHERE cik = ?", (cik,)
        ).fetchone()
        if row is None:
            return None
        return FundInfo(cik=row["cik"], name=row["name"], tier=Tier(row["tier"]))

    # ------------------------------------------------------------------
    # Holdings
    # ------------------------------------------------------------------

    def store_holdings(self, fund_holdings: FundHoldings) -> int:
        """Store all holdings from a filing. Returns count of rows inserted.

        Uses INSERT OR REPLACE to handle re-processing of the same filing.
        """
        now = datetime.now().isoformat()
        rows = [
            (
                fund_holdings.fund.cik,
                fund_holdings.quarter_end.isoformat(),
                fund_holdings.filing_date.isoformat(),
                h.cusip,
                h.issuer_name,
                h.title_of_class,
                h.value_thousands,
                h.shares_or_prn_amt,
                h.sh_prn_type,
                h.put_call,
                h.investment_discretion,
                h.voting_authority_sole,
                h.voting_authority_shared,
                h.voting_authority_none,
                now,
            )
            for h in fund_holdings.holdings
        ]
        self._conn.executemany(
            """INSERT OR REPLACE INTO holdings
               (cik, quarter_end, filing_date, cusip, issuer_name, title_of_class,
                value_thousands, shares_or_prn_amt, sh_prn_type, put_call,
                investment_discretion, voting_sole, voting_shared, voting_none,
                fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        # Update fund's last filing/quarter info
        self._conn.execute(
            """UPDATE funds SET last_filing_date = ?, last_quarter = ?
               WHERE cik = ?""",
            (
                fund_holdings.filing_date.isoformat(),
                fund_holdings.quarter_end.isoformat(),
                fund_holdings.fund.cik,
            ),
        )
        self._conn.commit()
        logger.info(
            "Stored %d holdings for %s Q%s",
            len(rows),
            fund_holdings.fund.name,
            fund_holdings.quarter_end,
        )
        return len(rows)

    def get_holdings(self, cik: str, quarter_end: date) -> list[Holding]:
        """Get all holdings for a fund in a specific quarter."""
        rows = self._conn.execute(
            """SELECT cusip, issuer_name, title_of_class, value_thousands,
                      shares_or_prn_amt, sh_prn_type, put_call,
                      investment_discretion, voting_sole, voting_shared, voting_none
               FROM holdings
               WHERE cik = ? AND quarter_end = ?
               ORDER BY value_thousands DESC""",
            (cik, quarter_end.isoformat()),
        ).fetchall()
        return [
            Holding(
                cusip=r["cusip"],
                issuer_name=r["issuer_name"],
                title_of_class=r["title_of_class"],
                value_thousands=r["value_thousands"],
                shares_or_prn_amt=r["shares_or_prn_amt"],
                sh_prn_type=r["sh_prn_type"],
                put_call=r["put_call"],
                investment_discretion=r["investment_discretion"] or "SOLE",
                voting_authority_sole=r["voting_sole"],
                voting_authority_shared=r["voting_shared"],
                voting_authority_none=r["voting_none"],
            )
            for r in rows
        ]

    def get_available_quarters(self, cik: str) -> list[date]:
        """Get sorted list of quarters with data for a fund."""
        rows = self._conn.execute(
            """SELECT DISTINCT quarter_end FROM holdings
               WHERE cik = ? ORDER BY quarter_end DESC""",
            (cik,),
        ).fetchall()
        return [date.fromisoformat(r["quarter_end"]) for r in rows]

    def get_all_available_quarters(self) -> list[date]:
        """Get all quarters that have any data across all funds."""
        rows = self._conn.execute(
            "SELECT DISTINCT quarter_end FROM holdings ORDER BY quarter_end DESC"
        ).fetchall()
        return [date.fromisoformat(r["quarter_end"]) for r in rows]

    def get_latest_quarter(self, cik: str) -> date | None:
        """Get the most recent quarter with data for a fund."""
        row = self._conn.execute(
            """SELECT MAX(quarter_end) as q FROM holdings WHERE cik = ?""",
            (cik,),
        ).fetchone()
        if row and row["q"]:
            return date.fromisoformat(row["q"])
        return None

    def has_holdings(self, cik: str, quarter_end: date) -> bool:
        """Check if holdings exist for a fund/quarter."""
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM holdings WHERE cik = ? AND quarter_end = ?",
            (cik, quarter_end.isoformat()),
        ).fetchone()
        return row["cnt"] > 0

    def get_holdings_count(self, cik: str, quarter_end: date) -> int:
        """Get count of holdings for a fund/quarter."""
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM holdings WHERE cik = ? AND quarter_end = ?",
            (cik, quarter_end.isoformat()),
        ).fetchone()
        return row["cnt"]

    def get_filing_date(self, cik: str, quarter_end: date) -> date | None:
        """Get the filing date for a fund/quarter."""
        row = self._conn.execute(
            """SELECT DISTINCT filing_date FROM holdings
               WHERE cik = ? AND quarter_end = ? LIMIT 1""",
            (cik, quarter_end.isoformat()),
        ).fetchone()
        if row:
            return date.fromisoformat(row["filing_date"])
        return None

    def get_all_holdings_for_quarter(
        self, quarter_end: date
    ) -> dict[str, list[Holding]]:
        """Get holdings for ALL funds in a given quarter.

        Returns: {cik: [Holding, ...]}
        """
        rows = self._conn.execute(
            """SELECT cik, cusip, issuer_name, title_of_class, value_thousands,
                      shares_or_prn_amt, sh_prn_type, put_call,
                      investment_discretion, voting_sole, voting_shared, voting_none
               FROM holdings
               WHERE quarter_end = ?
               ORDER BY cik, value_thousands DESC""",
            (quarter_end.isoformat(),),
        ).fetchall()

        result: dict[str, list[Holding]] = {}
        for r in rows:
            cik = r["cik"]
            if cik not in result:
                result[cik] = []
            result[cik].append(
                Holding(
                    cusip=r["cusip"],
                    issuer_name=r["issuer_name"],
                    title_of_class=r["title_of_class"],
                    value_thousands=r["value_thousands"],
                    shares_or_prn_amt=r["shares_or_prn_amt"],
                    sh_prn_type=r["sh_prn_type"],
                    put_call=r["put_call"],
                    investment_discretion=r["investment_discretion"] or "SOLE",
                    voting_authority_sole=r["voting_sole"],
                    voting_authority_shared=r["voting_shared"],
                    voting_authority_none=r["voting_none"],
                )
            )
        return result

    def get_holding_history(
        self, cik: str, cusip: str, n_quarters: int = 8
    ) -> list[tuple[date, Holding]]:
        """Get historical holdings for a specific fund+CUSIP pair.

        Returns list of (quarter_end, Holding) tuples, most recent first.
        """
        rows = self._conn.execute(
            """SELECT quarter_end, cusip, issuer_name, title_of_class,
                      value_thousands, shares_or_prn_amt, sh_prn_type, put_call,
                      investment_discretion, voting_sole, voting_shared, voting_none
               FROM holdings
               WHERE cik = ? AND cusip = ?
               ORDER BY quarter_end DESC
               LIMIT ?""",
            (cik, cusip, n_quarters),
        ).fetchall()
        return [
            (
                date.fromisoformat(r["quarter_end"]),
                Holding(
                    cusip=r["cusip"],
                    issuer_name=r["issuer_name"],
                    title_of_class=r["title_of_class"],
                    value_thousands=r["value_thousands"],
                    shares_or_prn_amt=r["shares_or_prn_amt"],
                    sh_prn_type=r["sh_prn_type"],
                    put_call=r["put_call"],
                    investment_discretion=r["investment_discretion"] or "SOLE",
                    voting_authority_sole=r["voting_sole"],
                    voting_authority_shared=r["voting_shared"],
                    voting_authority_none=r["voting_none"],
                ),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # CUSIP mapping
    # ------------------------------------------------------------------

    def get_cusip_ticker(self, cusip: str) -> str | None:
        """Look up ticker for a CUSIP from cache."""
        row = self._conn.execute(
            "SELECT ticker FROM cusip_map WHERE cusip = ?", (cusip,)
        ).fetchone()
        return row["ticker"] if row else None

    def get_cusip_tickers_bulk(self, cusips: list[str]) -> dict[str, str]:
        """Bulk lookup of CUSIP→ticker mappings."""
        if not cusips:
            return {}
        placeholders = ",".join("?" * len(cusips))
        rows = self._conn.execute(
            f"SELECT cusip, ticker FROM cusip_map WHERE cusip IN ({placeholders})",
            cusips,
        ).fetchall()
        return {r["cusip"]: r["ticker"] for r in rows if r["ticker"]}

    def store_cusip_mapping(
        self,
        cusip: str,
        ticker: str | None,
        name: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Store a CUSIP→ticker mapping."""
        self._conn.execute(
            """INSERT OR REPLACE INTO cusip_map
               (cusip, ticker, name, exchange, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            (cusip, ticker, name, exchange, datetime.now().isoformat()),
        )
        self._conn.commit()

    def seed_cusip_cache(self, seed_path: Path) -> int:
        """Pre-populate cusip_map from a bundled JSON seed file.

        Only inserts CUSIPs that are **not already** in the table
        (i.e. won't overwrite fresh OpenFIGI results).

        The seed file format is::

            {"60855R100": {"ticker": "MOH", "name": "...", "exchange": "US"}, ...}

        Returns the number of new entries seeded.
        """
        import json

        if not seed_path.exists():
            logger.debug("No CUSIP seed file at %s", seed_path)
            return 0

        with open(seed_path) as f:
            seed_data: dict[str, dict] = json.load(f)

        if not seed_data:
            return 0

        # Find which CUSIPs are already in the table
        existing = set(
            r[0] for r in self._conn.execute(
                "SELECT cusip FROM cusip_map",
            ).fetchall()
        )

        now = datetime.now().isoformat()
        new_rows = []
        for cusip, info in seed_data.items():
            if cusip in existing:
                continue
            new_rows.append((
                cusip,
                info.get("ticker"),
                info.get("name"),
                info.get("exchange"),
                "Equity",
                now,
            ))

        if new_rows:
            self._conn.executemany(
                """INSERT OR IGNORE INTO cusip_map
                   (cusip, ticker, name, exchange,
                    market_sector, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                new_rows,
            )
            self._conn.commit()

        logger.info(
            "Seeded %d CUSIP mappings from %s (%d already existed)",
            len(new_rows), seed_path.name, len(existing),
        )
        return len(new_rows)

    def export_cusip_seed(self, output_path: Path) -> int:
        """Export cusip_map to a JSON seed file for bundling.

        Returns the number of entries exported.
        """
        import json

        rows = self._conn.execute(
            "SELECT cusip, ticker, name, exchange "
            "FROM cusip_map WHERE ticker IS NOT NULL",
        ).fetchall()

        seed = {
            r["cusip"]: {
                "ticker": r["ticker"],
                "name": r["name"],
                "exchange": r["exchange"],
            }
            for r in rows
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(seed, f, indent=2, sort_keys=True)

        logger.info(
            "Exported %d CUSIP mappings to %s",
            len(seed), output_path,
        )
        return len(seed)

    # ------------------------------------------------------------------
    # Sector mapping
    # ------------------------------------------------------------------

    def get_sector_info(self, ticker: str) -> dict | None:
        """Get sector/industry info for a ticker."""
        row = self._conn.execute(
            """SELECT sector, industry, market_cap, shares_outstanding, float_shares
               FROM sector_map WHERE ticker = ?""",
            (ticker,),
        ).fetchone()
        if row:
            return dict(row)
        return None

    def get_sector_info_bulk(self, tickers: list[str]) -> dict[str, dict]:
        """Bulk lookup of sector info."""
        if not tickers:
            return {}
        placeholders = ",".join("?" * len(tickers))
        rows = self._conn.execute(
            f"""SELECT ticker, sector, industry, market_cap, shares_outstanding, float_shares
                FROM sector_map WHERE ticker IN ({placeholders})""",
            tickers,
        ).fetchall()
        return {r["ticker"]: dict(r) for r in rows}

    def store_sector_info(
        self,
        ticker: str,
        sector: str | None,
        industry: str | None,
        market_cap: float | None = None,
        shares_outstanding: int | None = None,
        float_shares: int | None = None,
    ) -> None:
        """Store sector/industry info for a ticker."""
        self._conn.execute(
            """INSERT OR REPLACE INTO sector_map
               (ticker, sector, industry, market_cap, shares_outstanding,
                float_shares, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                ticker, sector, industry, market_cap,
                shares_outstanding, float_shares,
                datetime.now().isoformat(),
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Price cache
    # ------------------------------------------------------------------

    def get_price(self, ticker: str, price_date: date) -> float | None:
        """Get cached price for a ticker on a specific date."""
        row = self._conn.execute(
            "SELECT close_price FROM prices WHERE ticker = ? AND price_date = ?",
            (ticker, price_date.isoformat()),
        ).fetchone()
        return row["close_price"] if row else None

    def get_prices_bulk(
        self, tickers: list[str], price_date: date
    ) -> dict[str, float]:
        """Bulk lookup of prices on a date."""
        if not tickers:
            return {}
        placeholders = ",".join("?" * len(tickers))
        rows = self._conn.execute(
            f"""SELECT ticker, close_price FROM prices
                WHERE ticker IN ({placeholders}) AND price_date = ?""",
            [*tickers, price_date.isoformat()],
        ).fetchall()
        return {r["ticker"]: r["close_price"] for r in rows}

    def store_prices(self, prices: dict[str, float], price_date: date) -> None:
        """Store prices for multiple tickers on a date."""
        now = datetime.now().isoformat()
        self._conn.executemany(
            """INSERT OR REPLACE INTO prices
               (ticker, price_date, close_price, fetched_at)
               VALUES (?, ?, ?, ?)""",
            [
                (ticker, price_date.isoformat(), price, now)
                for ticker, price in prices.items()
            ],
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Price performance cache
    # ------------------------------------------------------------------

    def get_price_performance(
        self, ticker: str, max_age_hours: int = 12,
    ) -> dict | None:
        """Get cached price performance if fresh enough.

        Returns dict with current_price, return_1w, return_1m,
        return_ytd, return_1yr, or None if stale/missing.
        """
        row = self._conn.execute(
            "SELECT * FROM price_performance WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if not row:
            return None

        # Staleness check
        fetched = datetime.fromisoformat(row["fetched_at"])
        age_hours = (datetime.now() - fetched).total_seconds() / 3600
        if age_hours > max_age_hours:
            return None

        return {
            "ticker": row["ticker"],
            "current_price": row["current_price"],
            "return_1w": row["return_1w"],
            "return_1m": row["return_1m"],
            "return_ytd": row["return_ytd"],
            "return_1yr": row["return_1yr"],
        }

    def get_price_performance_bulk(
        self, tickers: list[str], max_age_hours: int = 12,
    ) -> dict[str, dict]:
        """Bulk lookup of cached price performance.

        Returns {ticker: {current_price, return_1w, ...}} for fresh entries.
        """
        if not tickers:
            return {}
        placeholders = ",".join("?" * len(tickers))
        rows = self._conn.execute(
            f"SELECT * FROM price_performance WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchall()

        result: dict[str, dict] = {}
        now = datetime.now()
        for row in rows:
            fetched = datetime.fromisoformat(row["fetched_at"])
            age_hours = (now - fetched).total_seconds() / 3600
            if age_hours > max_age_hours:
                continue
            result[row["ticker"]] = {
                "ticker": row["ticker"],
                "current_price": row["current_price"],
                "return_1w": row["return_1w"],
                "return_1m": row["return_1m"],
                "return_ytd": row["return_ytd"],
                "return_1yr": row["return_1yr"],
            }
        return result

    def store_price_performance(
        self,
        ticker: str,
        current_price: float | None,
        return_1w: float | None,
        return_1m: float | None,
        return_ytd: float | None,
        return_1yr: float | None,
    ) -> None:
        """Store price performance for a ticker."""
        now = datetime.now().isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO price_performance
               (ticker, current_price, return_1w, return_1m,
                return_ytd, return_1yr, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ticker, current_price, return_1w, return_1m,
             return_ytd, return_1yr, now),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Filing index
    # ------------------------------------------------------------------

    def is_filing_processed(self, cik: str, accession_number: str) -> bool:
        """Check if a filing has already been processed."""
        row = self._conn.execute(
            """SELECT processed_at FROM filing_index
               WHERE cik = ? AND accession_number = ?""",
            (cik, accession_number),
        ).fetchone()
        return row is not None and row["processed_at"] is not None

    def store_filing_index(
        self,
        cik: str,
        accession_number: str,
        filing_date: str,
        report_date: str,
        quarter_end: str,
        form_type: str,
        primary_doc: str,
        holdings_count: int = 0,
        total_value_thousands: int = 0,
    ) -> None:
        """Store filing metadata and mark as processed."""
        self._conn.execute(
            """INSERT OR REPLACE INTO filing_index
               (cik, accession_number, filing_date, report_date, quarter_end,
                form_type, primary_doc, processed_at, holdings_count,
                total_value_thousands)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cik, accession_number, filing_date, report_date,
                quarter_end, form_type, primary_doc,
                datetime.now().isoformat(), holdings_count,
                total_value_thousands,
            ),
        )
        self._conn.commit()

    def get_latest_filing(self, cik: str) -> dict | None:
        """Get the most recent filing metadata for a CIK."""
        row = self._conn.execute(
            """SELECT * FROM filing_index
               WHERE cik = ? ORDER BY quarter_end DESC LIMIT 1""",
            (cik,),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_unique_cusips_for_quarter(self, quarter_end: date) -> list[str]:
        """Get all unique CUSIPs across all funds for a quarter."""
        rows = self._conn.execute(
            "SELECT DISTINCT cusip FROM holdings WHERE quarter_end = ?",
            (quarter_end.isoformat(),),
        ).fetchall()
        return [r["cusip"] for r in rows]

    def get_holdings_count_by_quarter(self, quarter_end: date) -> int:
        """Count how many distinct funds have holdings for a quarter."""
        row = self._conn.execute(
            "SELECT COUNT(DISTINCT cik) as cnt FROM holdings WHERE quarter_end = ?",
            (quarter_end.isoformat(),),
        ).fetchone()
        return row["cnt"] if row else 0

    def get_fund_quarter_map(self) -> dict[str, date]:
        """Get the most recent quarter with data for each fund.

        Returns: {cik: latest_quarter_end_date}
        """
        rows = self._conn.execute(
            """SELECT cik, MAX(quarter_end) as latest_q
               FROM holdings GROUP BY cik"""
        ).fetchall()
        return {r["cik"]: date.fromisoformat(r["latest_q"]) for r in rows}

    def get_fund_quarter_detail(
        self, quarter_end: date
    ) -> dict[str, dict]:
        """Get per-fund filing details for a quarter.

        Returns: {cik: {"quarter_end": date, "filing_date": str, ...}}
        """
        rows = self._conn.execute(
            """SELECT fi.cik, fi.quarter_end, fi.filing_date, fi.holdings_count,
                      fi.total_value_thousands, f.name, f.tier
               FROM filing_index fi
               JOIN funds f ON fi.cik = f.cik
               WHERE fi.quarter_end = ?""",
            (quarter_end.isoformat(),),
        ).fetchall()
        return {
            r["cik"]: {
                "quarter_end": date.fromisoformat(r["quarter_end"]),
                "filing_date": r["filing_date"],
                "holdings_count": r["holdings_count"],
                "total_value_thousands": r["total_value_thousands"],
                "name": r["name"],
                "tier": r["tier"],
            }
            for r in rows
        }

    def get_cross_quarter_activity(
        self,
        cik: str,
        exclude_quarter: date | None = None,
    ) -> list[dict]:
        """Compute per-quarter-pair activity metrics for historical baseline.

        For each consecutive quarter pair, computes:
        - new_positions: CUSIPs in current quarter but not prior (equity only)
        - exited_positions: CUSIPs in prior quarter but not current
        - hhi_current / hhi_prior: sum-of-squared weights for each quarter
        - max_new_weight_pct: weight of largest new position (% of AUM)

        Returns list of dicts sorted by quarter_end DESC, excluding
        the specified quarter if given.
        """
        # Get all quarters for this fund
        quarters = self.get_available_quarters(cik)
        if exclude_quarter and exclude_quarter in quarters:
            quarters = [q for q in quarters if q != exclude_quarter]

        if len(quarters) < 2:
            return []

        results: list[dict] = []

        for i in range(len(quarters) - 1):
            current_q = quarters[i]      # More recent
            prior_q = quarters[i + 1]    # Older

            # Load equity positions for both quarters
            cur_rows = self._conn.execute(
                """SELECT cusip, value_thousands
                   FROM holdings
                   WHERE cik = ? AND quarter_end = ? AND put_call IS NULL""",
                (cik, current_q.isoformat()),
            ).fetchall()

            pri_rows = self._conn.execute(
                """SELECT cusip, value_thousands
                   FROM holdings
                   WHERE cik = ? AND quarter_end = ? AND put_call IS NULL""",
                (cik, prior_q.isoformat()),
            ).fetchall()

            cur_cusips = {r["cusip"] for r in cur_rows}
            pri_cusips = {r["cusip"] for r in pri_rows}

            new_cusips = cur_cusips - pri_cusips
            exited_cusips = pri_cusips - cur_cusips

            # AUM for current quarter
            cur_total = sum(r["value_thousands"] for r in cur_rows)
            pri_total = sum(r["value_thousands"] for r in pri_rows)

            # HHI for both quarters
            hhi_cur = 0.0
            if cur_total > 0:
                hhi_cur = sum(
                    (r["value_thousands"] / cur_total) ** 2
                    for r in cur_rows
                )

            hhi_pri = 0.0
            if pri_total > 0:
                hhi_pri = sum(
                    (r["value_thousands"] / pri_total) ** 2
                    for r in pri_rows
                )

            # Max new position weight
            max_new_weight = 0.0
            if new_cusips and cur_total > 0:
                for r in cur_rows:
                    if r["cusip"] in new_cusips:
                        w = r["value_thousands"] / cur_total * 100
                        if w > max_new_weight:
                            max_new_weight = w

            results.append({
                "quarter_end": current_q,
                "prior_quarter": prior_q,
                "new_positions": len(new_cusips),
                "exited_positions": len(exited_cusips),
                "hhi_current": hhi_cur,
                "hhi_prior": hhi_pri,
                "hhi_change": hhi_cur - hhi_pri,
                "max_new_weight_pct": max_new_weight,
            })

        return results

    def vacuum(self) -> None:
        """Reclaim disk space after large deletes."""
        self._conn.execute("VACUUM")

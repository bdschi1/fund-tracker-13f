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

    def vacuum(self) -> None:
        """Reclaim disk space after large deletes."""
        self._conn.execute("VACUUM")

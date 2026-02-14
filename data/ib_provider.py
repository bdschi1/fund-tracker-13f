"""Interactive Brokers data provider.

Requires ``ib_insync`` and a running TWS or IB Gateway instance.
Install with::

    pip install -e ".[ib]"

Connection defaults to TWS paper-trading on localhost:7497.
Override with environment variables or Settings fields::

    FT13F_IB_HOST=127.0.0.1
    FT13F_IB_PORT=7497
    FT13F_IB_CLIENT_ID=10
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from data.provider import MarketDataProvider

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Optional import — fails gracefully when ib_insync is not installed
# ------------------------------------------------------------------

try:
    from ib_insync import IB, Stock
    _HAS_IB = True
except ImportError:
    IB = None  # type: ignore[assignment,misc]
    Stock = None  # type: ignore[assignment,misc]
    _HAS_IB = False

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 7497
_DEFAULT_CLIENT_ID = 10


def is_available() -> bool:
    """Return True if ib_insync is importable."""
    return _HAS_IB


# ------------------------------------------------------------------
# Connection manager
# ------------------------------------------------------------------

class _IBConnection:
    """Managed IB connection with auto-connect and reconnect."""

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        client_id: int = _DEFAULT_CLIENT_ID,
        timeout: int = 15,
        readonly: bool = True,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self.readonly = readonly
        self._ib: Any = None

        if not _HAS_IB:
            raise ImportError(
                "ib_insync is not installed. Install with: pip install -e '.[ib]'"
            )

    def connect(self) -> Any:
        """Return connected IB instance, reconnecting if needed."""
        if self._ib is not None and self._ib.isConnected():
            return self._ib

        ib = IB()
        try:
            ib.connect(
                host=self.host,
                port=self.port,
                clientId=self.client_id,
                timeout=self.timeout,
                readonly=self.readonly,
            )
        except Exception as exc:
            raise ConnectionError(
                f"Cannot connect to IB on {self.host}:{self.port}. "
                f"Ensure TWS/Gateway is running. Error: {exc}"
            ) from exc

        logger.info("IB connected to %s:%d", self.host, self.port)
        self._ib = ib
        return ib

    def disconnect(self) -> None:
        """Disconnect from IB."""
        if self._ib is not None:
            try:
                self._ib.disconnect()
            except Exception:
                pass
            self._ib = None


# ------------------------------------------------------------------
# Helper: IB duration string
# ------------------------------------------------------------------

def _ib_duration(start: date, end: date) -> str:
    """Convert a date range to an IB-compatible duration string."""
    delta = (end - start).days + 1
    if delta <= 365:
        return f"{delta} D"
    years = (delta // 365) + 1
    return f"{years} Y"


# ------------------------------------------------------------------
# Provider implementation
# ------------------------------------------------------------------

class IBProvider(MarketDataProvider):
    """Interactive Brokers provider — real-time, requires TWS/Gateway."""

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        client_id: int = _DEFAULT_CLIENT_ID,
        timeout: int = 15,
    ):
        self._conn = _IBConnection(host, port, client_id, timeout, readonly=True)

    @property
    def name(self) -> str:
        return "Interactive Brokers"

    # ------------------------------------------------------------------
    # Price history
    # ------------------------------------------------------------------

    def fetch_price_history(
        self,
        ticker: str,
        days: int = 400,
    ) -> list[dict]:
        """Fetch daily OHLCV bars from IB using reqHistoricalData."""
        try:
            ib = self._conn.connect()
        except (ConnectionError, ImportError) as exc:
            logger.error("IB connection failed: %s", exc)
            return []

        today = date.today()
        start = today - timedelta(days=days)
        contract = Stock(ticker, "SMART", "USD")

        try:
            ib.qualifyContracts(contract)
        except Exception:
            logger.warning("IB cannot qualify contract for %s", ticker)
            return []

        duration = _ib_duration(start, today)
        end_dt = datetime(today.year, today.month, today.day, 23, 59, 59)

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=duration,
                barSizeSetting="1 day",
                whatToShow="ADJUSTED_LAST",
                useRTH=True,
                formatDate=1,
                keepUpToDate=False,
            )
        except Exception as exc:
            logger.warning("IB historical data failed for %s: %s", ticker, exc)
            return []

        if not bars:
            logger.warning("No IB data returned for %s", ticker)
            return []

        rows: list[dict] = []
        for bar in bars:
            bar_date = bar.date
            if isinstance(bar_date, str):
                bar_date = datetime.strptime(bar_date, "%Y%m%d").date()
            elif isinstance(bar_date, datetime):
                bar_date = bar_date.date()

            if bar_date < start or bar_date > today:
                continue

            rows.append({
                "date": bar_date,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            })

        ib.sleep(0.5)  # pacing to avoid rate limiting
        return rows

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def fetch_ticker_info(self, ticker: str) -> dict:
        """Fetch sector/industry from IB contract details + fundamentals."""
        defaults: dict = {
            "sector": None,
            "industry": None,
            "market_cap": None,
            "shares_outstanding": None,
            "float_shares": None,
        }

        try:
            ib = self._conn.connect()
        except (ConnectionError, ImportError) as exc:
            logger.error("IB connection failed: %s", exc)
            return defaults

        contract = Stock(ticker, "SMART", "USD")

        try:
            ib.qualifyContracts(contract)
        except Exception:
            logger.warning("IB cannot qualify contract for %s", ticker)
            return defaults

        # Contract details → sector/industry
        try:
            details_list = ib.reqContractDetails(contract)
            if details_list:
                details = details_list[0]
                defaults["sector"] = getattr(details, "category", None) or None
                defaults["industry"] = getattr(details, "industry", None) or None
        except Exception as exc:
            logger.warning("IB contract details failed for %s: %s", ticker, exc)

        # Fundamental data (requires subscription) — parse XML
        try:
            fundamentals = ib.reqFundamentalData(contract, "ReportSnapshot")
            if fundamentals:
                defaults = _parse_ib_fundamentals(fundamentals, defaults)
        except Exception as exc:
            logger.debug(
                "IB fundamental data unavailable for %s (may need subscription): %s",
                ticker, exc,
            )

        ib.sleep(0.5)
        return defaults

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def disconnect(self) -> None:
        """Disconnect from IB."""
        self._conn.disconnect()


# ------------------------------------------------------------------
# XML parser for IB fundamentals
# ------------------------------------------------------------------

def _parse_ib_fundamentals(xml_str: str, defaults: dict) -> dict:
    """Parse IB's ReportSnapshot XML into standard dict."""
    try:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(xml_str)

        for ratio in root.iter("Ratio"):
            field = ratio.get("FieldName", "")
            val = ratio.text
            if not val:
                continue
            try:
                if field == "MKTCAP":
                    defaults["market_cap"] = float(val) * 1e6
                elif field == "SHARESOUT":
                    defaults["shares_outstanding"] = int(float(val) * 1e6)
            except (ValueError, TypeError):
                continue
    except Exception as exc:
        logger.debug("Failed to parse IB fundamentals XML: %s", exc)

    return defaults

"""SEC EDGAR client for fetching 13F-HR filings.

Rate limited to stay under SEC's 10 req/sec fair access policy.
Requires User-Agent header with name and email.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

SEC_DATA_URL = "https://data.sec.gov"
SEC_WWW_URL = "https://www.sec.gov"
SUBMISSIONS_URL = f"{SEC_DATA_URL}/submissions/CIK{{cik}}.json"
ARCHIVES_URL = f"{SEC_WWW_URL}/Archives/edgar/data"


@dataclass
class FilingReference:
    """Reference to a specific 13F filing on EDGAR."""

    cik: str  # Raw CIK (may or may not be zero-padded)
    accession_number: str
    filing_date: str
    report_date: str
    primary_doc: str
    form_type: str = "13F-HR"

    @property
    def cik_raw(self) -> str:
        """CIK without zero-padding (for archive URLs)."""
        return self.cik.lstrip("0") or "0"

    @property
    def accession_path(self) -> str:
        """Accession number formatted for URL (no dashes)."""
        return self.accession_number.replace("-", "")

    @property
    def filing_base_url(self) -> str:
        """Base URL for this filing's documents on www.sec.gov."""
        return f"{ARCHIVES_URL}/{self.cik_raw}/{self.accession_path}"

    @property
    def index_url(self) -> str:
        """URL to the filing index page (HTML)."""
        return f"{self.filing_base_url}/{self.accession_number}-index.htm"

    @property
    def quarter_end(self) -> date:
        """Derive the calendar quarter-end from the report date."""
        rd = date.fromisoformat(self.report_date)
        # Quarter ends: 03-31, 06-30, 09-30, 12-31
        quarter_ends = {
            1: date(rd.year, 3, 31),
            2: date(rd.year, 3, 31),
            3: date(rd.year, 3, 31),
            4: date(rd.year, 6, 30),
            5: date(rd.year, 6, 30),
            6: date(rd.year, 6, 30),
            7: date(rd.year, 9, 30),
            8: date(rd.year, 9, 30),
            9: date(rd.year, 9, 30),
            10: date(rd.year, 12, 31),
            11: date(rd.year, 12, 31),
            12: date(rd.year, 12, 31),
        }
        return quarter_ends[rd.month]


class EdgarClient:
    """HTTP client for SEC EDGAR with rate limiting and retry."""

    def __init__(
        self,
        user_agent: str,
        rate_limit_rps: float = 8.0,
    ) -> None:
        self._user_agent = user_agent
        self._min_interval = 1.0 / rate_limit_rps
        self._client = httpx.Client(
            headers={
                "User-Agent": user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
        )
        self._last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        """Enforce minimum interval between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((
            httpx.HTTPStatusError,
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _get(self, url: str) -> httpx.Response:
        """Rate-limited GET request with retry."""
        self._rate_limit()
        logger.debug("GET %s", url)
        resp = self._client.get(url)
        resp.raise_for_status()
        return resp

    def get_submissions(self, cik: str) -> dict:
        """Fetch the submissions JSON for a CIK.

        Returns the full submissions object including recent filings
        and any overflow filing history files.
        """
        url = SUBMISSIONS_URL.format(cik=cik.zfill(10))
        resp = self._get(url)
        data = resp.json()

        # Handle overflow: SEC splits filing history into separate JSON files
        # for companies with >1000 filings
        recent = data.get("filings", {}).get("recent", {})
        overflow_files = data.get("filings", {}).get("files", [])

        for overflow in overflow_files:
            overflow_url = f"{SEC_DATA_URL}/submissions/{overflow['name']}"
            try:
                overflow_resp = self._get(overflow_url)
                overflow_data = overflow_resp.json()
                # Merge overflow data into recent
                for key in recent:
                    if key in overflow_data:
                        recent[key].extend(overflow_data[key])
            except Exception:
                logger.warning("Failed to fetch overflow file: %s", overflow["name"])

        return data

    def find_13f_filings(
        self, cik: str, n_quarters: int = 2
    ) -> list[FilingReference]:
        """Find the most recent 13F-HR filings for a CIK.

        Args:
            cik: The CIK number (will be zero-padded to 10 digits).
            n_quarters: Number of most recent filings to return.

        Returns:
            List of FilingReference objects, most recent first.
        """
        submissions = self.get_submissions(cik)
        recent = submissions.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        report_dates = recent.get("reportDate", [])

        # Collect all 13F filings, then pick the best per quarter
        candidates: dict[str, FilingReference] = {}  # quarter_key → best filing

        for i, form in enumerate(forms):
            if form not in ("13F-HR", "13F-HR/A"):
                continue

            report_date = report_dates[i] if i < len(report_dates) else ""
            if not report_date:
                continue

            quarter_key = report_date[:7]  # YYYY-MM

            ref = FilingReference(
                cik=cik.zfill(10),
                accession_number=accessions[i],
                filing_date=dates[i],
                report_date=report_date,
                primary_doc=primary_docs[i] if i < len(primary_docs) else "",
                form_type=form,
            )

            if quarter_key not in candidates:
                candidates[quarter_key] = ref
            elif form == "13F-HR/A":
                # Amendments override original filings for same quarter
                candidates[quarter_key] = ref

            # Stop scanning once we have enough quarters
            if len(candidates) > n_quarters * 2:
                break

        # Sort by report_date descending and take the most recent n_quarters
        filings = sorted(
            candidates.values(),
            key=lambda f: f.report_date,
            reverse=True,
        )[:n_quarters]

        if not filings:
            entity_name = submissions.get("name", "UNKNOWN")
            all_forms = set(forms[:50])  # Inspect first 50 for debugging
            logger.warning(
                "No 13F filings found for CIK %s (%s). "
                "Recent form types: %s",
                cik,
                entity_name,
                ", ".join(sorted(all_forms)[:10]) if all_forms else "NONE",
            )
        else:
            logger.info(
                "Found %d 13F filings for CIK %s",
                len(filings),
                cik,
            )
        return filings

    def fetch_info_table_xml(self, filing: FilingReference) -> str:
        """Fetch the 13F information table XML for a filing.

        The primary_doc from submissions often points to the rendered HTML
        form (xslForm13F_X02/primary_doc.xml), not the raw info table XML.
        We scrape the filing index HTML to find the correct XML document.
        """
        import re

        base_url = filing.filing_base_url

        # Step 1: Fetch the filing index page to find all document links
        try:
            index_resp = self._get(filing.index_url)
            index_html = index_resp.text
        except Exception:
            logger.warning(
                "Could not fetch filing index for %s",
                filing.accession_number,
            )
            index_html = ""

        # Step 2: Find info table XML from the index page
        xml_doc = None
        if index_html:
            # Extract all XML file links from the index page
            xml_links = re.findall(
                r'href="[^"]*?/([^/"]+\.xml)"', index_html
            )
            # De-duplicate preserving order
            seen = set()
            xml_files = []
            for f in xml_links:
                if f.lower() not in seen:
                    seen.add(f.lower())
                    xml_files.append(f)

            logger.debug(
                "XML files in filing %s: %s",
                filing.accession_number,
                xml_files,
            )

            # Priority 1: Look for *infotable* or *information* in filename
            for f in xml_files:
                fl = f.lower()
                if ("infotable" in fl or "information" in fl) and fl.endswith(
                    ".xml"
                ):
                    xml_doc = f
                    break

            # Priority 2: Any XML that is NOT primary_doc.xml and NOT
            # inside xslForm13F path (those are rendered views)
            if xml_doc is None:
                for f in xml_files:
                    fl = f.lower()
                    if fl == "primary_doc.xml":
                        continue
                    if fl.endswith(".xml"):
                        xml_doc = f
                        break

        # Priority 3: Fallback — use primary_doc but strip xsl path prefix
        if xml_doc is None:
            pdoc = filing.primary_doc
            # xslForm13F_X02/primary_doc.xml → primary_doc.xml
            if "/" in pdoc:
                pdoc = pdoc.split("/")[-1]
            xml_doc = pdoc
            logger.warning(
                "Using fallback doc for %s: %s",
                filing.accession_number,
                xml_doc,
            )

        xml_url = f"{base_url}/{xml_doc}"
        logger.debug("Fetching info table XML: %s", xml_url)
        resp = self._get(xml_url)
        return resp.text

    def lookup_entity(self, cik: str) -> dict | None:
        """Validate a CIK and return entity name.

        Returns {"cik": cik, "name": entity_name} or None if invalid.
        Lightweight — only fetches submissions metadata, not filings.
        """
        try:
            submissions = self.get_submissions(cik)
            name = submissions.get("name", "").strip()
            if not name:
                return None
            return {"cik": cik, "name": name}
        except Exception:
            logger.warning("CIK lookup failed for %s", cik)
            return None

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> EdgarClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()

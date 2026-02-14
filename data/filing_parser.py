"""Parse 13F information table XML into Holding objects.

Handles the standard SEC 13F XML namespace and common variations.
"""

from __future__ import annotations

import logging
import re
from datetime import date

from lxml import etree

from core.models import FundHoldings, FundInfo, Holding

logger = logging.getLogger(__name__)

# 13F XML namespaces — SEC uses several variants
# The newer namespace reports value in DOLLARS; the older in THOUSANDS.
NS_NEW = {"ns": "http://www.sec.gov/edgar/document/thirteenf/informationtable"}
NS_OLD = {"ns": "http://www.sec.gov/edgar/13Fform"}
NAMESPACES = [NS_NEW, NS_OLD]

# The newer schema (thirteenf/informationtable) stores value in dollars.
# The older schema (13Fform) stores value in thousands.
# We normalize everything to thousands for consistency.
NS_VALUE_IN_DOLLARS = {
    NS_NEW["ns"],  # newer schema = dollars
}


def parse_info_table_xml(
    xml_text: str,
    fund: FundInfo,
    quarter_end: date,
    filing_date: date,
    report_date: date,
) -> FundHoldings:
    """Parse 13F information table XML into a FundHoldings object.

    Tries multiple XML namespace variants since SEC filings aren't
    perfectly consistent.

    Args:
        xml_text: Raw XML string of the information table.
        fund: The fund this filing belongs to.
        quarter_end: Calendar quarter end date.
        filing_date: Date the filing was submitted.
        report_date: Period of report date.

    Returns:
        FundHoldings with all positions parsed.

    Raises:
        ValueError: If the XML cannot be parsed with any known namespace.
    """
    # Clean up common XML issues
    xml_text = _clean_xml(xml_text)

    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError as e:
        logger.error("XML parse error for %s Q%s: %s", fund.name, quarter_end, e)
        raise ValueError(f"Cannot parse XML: {e}") from e

    # Try each namespace variant
    holdings: list[Holding] = []
    for ns in NAMESPACES:
        entries = root.findall(".//ns:infoTable", ns)
        if not entries:
            # Try without namespace prefix
            entries = root.findall(".//{%s}infoTable" % ns["ns"])
        if entries:
            value_in_dollars = ns["ns"] in NS_VALUE_IN_DOLLARS
            holdings = _parse_entries(entries, ns, value_in_dollars)
            if value_in_dollars:
                logger.debug(
                    "Using newer schema (value in dollars) for %s Q%s",
                    fund.name,
                    quarter_end,
                )
            break

    # Fallback: try without any namespace (older, value in thousands)
    if not holdings:
        entries = root.findall(".//infoTable")
        if entries:
            holdings = _parse_entries_no_ns(entries)

    if not holdings:
        logger.warning(
            "No holdings parsed for %s Q%s (XML may have unexpected format)",
            fund.name,
            quarter_end,
        )

    logger.info(
        "Parsed %d holdings for %s Q%s",
        len(holdings),
        fund.name,
        quarter_end,
    )

    return FundHoldings(
        fund=fund,
        quarter_end=quarter_end,
        filing_date=filing_date,
        report_date=report_date,
        holdings=holdings,
    )


def _parse_entries(
    entries: list[etree._Element],
    ns: dict[str, str],
    value_in_dollars: bool = False,
) -> list[Holding]:
    """Parse infoTable entries with a specific namespace."""
    holdings: list[Holding] = []

    for entry in entries:
        try:
            issuer = _text(entry, "ns:nameOfIssuer", ns)
            title = _text(entry, "ns:titleOfClass", ns)
            cusip = _text(entry, "ns:cusip", ns).upper().strip()
            raw_value = _int(entry, "ns:value", ns)
            # Normalize: newer schema reports dollars, convert to thousands
            value = raw_value // 1000 if value_in_dollars else raw_value

            shrs_elem = entry.find("ns:shrsOrPrnAmt", ns)
            if shrs_elem is None:
                shrs_elem = entry.find("ns:shrsorprnamt", ns)
            shares = _int(shrs_elem, "ns:sshPrnamt", ns) if shrs_elem is not None else 0
            sh_prn_type = (
                _text(shrs_elem, "ns:sshPrnamtType", ns)
                if shrs_elem is not None
                else "SH"
            )

            put_call_raw = _text(entry, "ns:putCall", ns)
            put_call = put_call_raw.upper() if put_call_raw else None
            if put_call and put_call not in ("PUT", "CALL"):
                put_call = None

            discretion = _text(entry, "ns:investmentDiscretion", ns) or "SOLE"

            voting = entry.find("ns:votingAuthority", ns)
            vote_sole = _int(voting, "ns:Sole", ns) if voting is not None else 0
            vote_shared = _int(voting, "ns:Shared", ns) if voting is not None else 0
            vote_none = _int(voting, "ns:None", ns) if voting is not None else 0

            if not cusip or value == 0:
                continue

            holdings.append(
                Holding(
                    cusip=cusip,
                    issuer_name=issuer,
                    title_of_class=title,
                    value_thousands=value,
                    shares_or_prn_amt=shares,
                    sh_prn_type=sh_prn_type.upper() if sh_prn_type else "SH",
                    put_call=put_call,
                    investment_discretion=discretion.upper(),
                    voting_authority_sole=vote_sole,
                    voting_authority_shared=vote_shared,
                    voting_authority_none=vote_none,
                )
            )
        except Exception as e:
            logger.debug("Skipping entry due to parse error: %s", e)
            continue

    return holdings


def _parse_entries_no_ns(entries: list[etree._Element]) -> list[Holding]:
    """Parse infoTable entries without XML namespace."""
    holdings: list[Holding] = []

    for entry in entries:
        try:
            issuer = _text_no_ns(entry, "nameOfIssuer")
            title = _text_no_ns(entry, "titleOfClass")
            cusip = _text_no_ns(entry, "cusip").upper().strip()
            value = _int_no_ns(entry, "value")

            shrs_elem = entry.find("shrsOrPrnAmt")
            shares = _int_no_ns(shrs_elem, "sshPrnamt") if shrs_elem is not None else 0
            sh_prn_type = (
                _text_no_ns(shrs_elem, "sshPrnamtType")
                if shrs_elem is not None
                else "SH"
            )

            put_call_raw = _text_no_ns(entry, "putCall")
            put_call = put_call_raw.upper() if put_call_raw else None
            if put_call and put_call not in ("PUT", "CALL"):
                put_call = None

            discretion = _text_no_ns(entry, "investmentDiscretion") or "SOLE"

            voting = entry.find("votingAuthority")
            vote_sole = _int_no_ns(voting, "Sole") if voting is not None else 0
            vote_shared = _int_no_ns(voting, "Shared") if voting is not None else 0
            vote_none = _int_no_ns(voting, "None") if voting is not None else 0

            if not cusip or value == 0:
                continue

            holdings.append(
                Holding(
                    cusip=cusip,
                    issuer_name=issuer,
                    title_of_class=title,
                    value_thousands=value,
                    shares_or_prn_amt=shares,
                    sh_prn_type=sh_prn_type.upper() if sh_prn_type else "SH",
                    put_call=put_call,
                    investment_discretion=discretion.upper(),
                    voting_authority_sole=vote_sole,
                    voting_authority_shared=vote_shared,
                    voting_authority_none=vote_none,
                )
            )
        except Exception as e:
            logger.debug("Skipping entry (no-ns) due to parse error: %s", e)
            continue

    return holdings


def _clean_xml(xml_text: str) -> str:
    """Clean up common XML issues in SEC filings."""
    # Remove XML declaration if it has encoding issues
    xml_text = re.sub(r"<\?xml[^?]*\?>", "", xml_text, count=1)
    # Add XML declaration back
    xml_text = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_text.strip()
    return xml_text


def _text(elem: etree._Element, path: str, ns: dict[str, str]) -> str:
    """Extract text from an element using namespace-aware path."""
    child = elem.find(path, ns)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _int(elem: etree._Element | None, path: str, ns: dict[str, str]) -> int:
    """Extract integer from an element using namespace-aware path."""
    if elem is None:
        return 0
    txt = _text(elem, path, ns)
    try:
        return int(txt) if txt else 0
    except ValueError:
        return 0


def _text_no_ns(elem: etree._Element, tag: str) -> str:
    """Extract text without namespace."""
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _int_no_ns(elem: etree._Element | None, tag: str) -> int:
    """Extract integer without namespace."""
    if elem is None:
        return 0
    txt = _text_no_ns(elem, tag)
    try:
        return int(txt) if txt else 0
    except ValueError:
        return 0

"""Ticker Lookup — small search box for quick stock info.

Appears at the top of every page. Searches holdings by ticker or
issuer name and shows: ticker, company name, sector, market cap.
All data from local SQLite — no external API calls.
"""

from __future__ import annotations

import streamlit as st


def render_ticker_lookup() -> None:
    """Render a compact ticker/name search box.

    Display format per result:
        **TICKER** · Company Name · Sector · $Market Cap
    """
    store = st.session_state.get("store")
    if not store:
        return

    query = st.text_input(
        "Stock lookup",
        placeholder="Ticker or company name …",
        key="ticker_lookup_input",
        label_visibility="collapsed",
    )

    if not query or len(query.strip()) < 2:
        return

    # Easter egg
    if query.strip().lower() == "garcia":
        st.markdown(
            "🎸 [What a long, strange trip it's been…]"
            "(https://en.wikipedia.org/wiki/Jerry_Garcia)",
            unsafe_allow_html=True,
        )
        return

    q = query.strip().upper()

    # Search in holdings for matching issuer_name or ticker
    conn = store._conn
    rows = conn.execute(
        """SELECT DISTINCT h.cusip, h.issuer_name,
                  cm.ticker, cm.name AS figi_name, cm.exchange,
                  sm.sector, sm.industry, sm.market_cap
           FROM holdings h
           LEFT JOIN cusip_map cm ON h.cusip = cm.cusip
           LEFT JOIN sector_map sm ON cm.ticker = sm.ticker
           WHERE UPPER(h.issuer_name) LIKE ?
              OR UPPER(cm.ticker) = ?
           LIMIT 8""",
        (f"%{q}%", q),
    ).fetchall()

    if not rows:
        st.caption(f"No matches for '{query.strip()}'")
        return

    # Deduplicate by cusip (holdings may have variant names)
    seen: dict[str, dict] = {}
    for r in rows:
        cusip = r["cusip"]
        if cusip not in seen:
            seen[cusip] = dict(r)
        else:
            # Keep the row with a ticker if we don't already have one
            if not seen[cusip].get("ticker") and r["ticker"]:
                seen[cusip] = dict(r)

    for info in list(seen.values())[:5]:
        ticker = info.get("ticker") or None
        name = info.get("issuer_name") or info.get("figi_name") or "—"
        sector = info.get("sector") or ""
        mcap = info.get("market_cap")

        # Format market cap
        mcap_str = ""
        if mcap and mcap > 0:
            if mcap >= 1e12:
                mcap_str = f"${mcap / 1e12:.1f}T"
            elif mcap >= 1e9:
                mcap_str = f"${mcap / 1e9:.1f}B"
            elif mcap >= 1e6:
                mcap_str = f"${mcap / 1e6:.0f}M"

        # Build: **TICKER** · Company Name · Sector · $Cap
        parts: list[str] = []
        if ticker:
            parts.append(f"<b>{ticker}</b>")
        parts.append(name)

        meta: list[str] = []
        if sector:
            meta.append(sector)
        if mcap_str:
            meta.append(mcap_str)

        line1 = " · ".join(parts)
        line2 = " · ".join(meta) if meta else ""

        html = (
            f"<div style='font-size:0.85em; line-height:1.5; "
            f"padding:4px 0; border-bottom:1px solid #333;'>"
            f"{line1}"
        )
        if line2:
            html += f"<br><span style='color:#888;'>{line2}</span>"
        html += "</div>"

        st.markdown(html, unsafe_allow_html=True)

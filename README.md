# Fund Tracker 13F

SEC 13F-HR filing analyzer tracking 52 hedge funds and asset managers across 5 tiers. Surfaces high-conviction moves — new positions, full exits, concentrated adds, and consensus trades — ranked by signal strength (% share change), not dollar size.

## Pages

| # | Page | What it shows |
|---|------|---------------|
| 1 | **Dashboard** | Top findings (historically-aware), fund summary table, top moves chart, activity heatmap, concentration shifts |
| 2 | **Stock Analysis** | Search any ticker — which funds hold it, who initiated/exited, net sentiment |
| 3 | **Fund Deep Dive** | Single fund QoQ breakdown: every position change, AUM, concentration, filing lag |
| 4 | **Signal Scanner** | All position changes across all funds: new, exits, adds >50%, trims >60% |
| 5 | **Crowded Trades** | Consensus buys/sells (3+ funds), divergences (one buying what another sells) |
| 6 | **Overlap Matrix** | Fund-to-fund portfolio similarity heatmap, shared holdings Sankey |
| 7 | **Export Report** | Markdown report preview and download |

## Key Features

- **Historically-aware findings** — per-fund z-score baselines (activity, concentration, position sizing) so the Top Findings surface genuinely surprising behavior, not just funds that are always active
- **Bundled CUSIP-to-ticker mapping** — 4,500+ CUSIP mappings ship with the repo (`config/cusip_tickers.json`) covering Russell 1000/2000, major ETFs, and crypto ETFs. Instant ticker resolution on new installs without API calls
- **Fallback CUSIP resolution** — unknown CUSIPs are resolved via OpenFIGI API and cached in SQLite permanently
- **Price performance tags** — Top Findings show 1w / 1m / YTD / 1yr returns next to each ticker
- **Pod-shop risk metrics** — crowding risk, float ownership analysis, sector concentration flows
- **9 quarters of historical data** — QoQ analysis spans back to Q1 2024 for robust baselines

## Setup

```bash
pip install -r requirements.txt
```

Copy the environment template and set your EDGAR user agent:

```bash
cp .env.example .env
```

Edit `.env`:

```
FT13F_EDGAR_USER_AGENT="YourName your@email.com"
```

Optional — add an [OpenFIGI](https://www.openfigi.com/api) API key for faster CUSIP resolution:

```
FT13F_OPENFIGI_API_KEY="your-key"
```

Without a key, the bundled `config/cusip_tickers.json` covers ~99.7% of holdings by dollar value. The API key is only needed to resolve edge-case CUSIPs (SPACs, obscure trusts, etc.).

## Run

```bash
streamlit run app/main.py
```

Click **Fetch & Analyze** in the sidebar. This downloads filings from SEC EDGAR, resolves CUSIPs, and runs the analysis (~1-3 min first time). Data is cached locally in SQLite.

## CUSIP Seed File

The bundled `config/cusip_tickers.json` maps CUSIPs to tickers so new installs get instant resolution. Sources:

- SEC company_tickers.json (name matching)
- OpenFIGI API (direct CUSIP lookup)
- iShares Russell 1000/2000 ETF holdings
- JPM 2024 Global ETF Handbook (US-listed ETFs with AUM > $2B)

To update the seed after resolving new CUSIPs:

```bash
python scripts/export_cusip_seed.py
```

## Stack

- **UI**: Streamlit, Plotly
- **Data**: Pydantic, SQLite, httpx, lxml
- **Sources**: SEC EDGAR (13F-HR filings), Yahoo Finance (prices), OpenFIGI (CUSIP resolution)

## Fund Tiers

| Tier | Category | Count |
|------|----------|-------|
| A | Multi-Strat | 5 |
| B | Stock Pickers / Tiger Cubs | 21 |
| C | Event-Driven / Activist | 7 |
| D | Emerging / Newer | 7 |
| E | Healthcare Specialists | 12 |

## Project Structure

```
app/
  main.py              Streamlit entry point, sidebar, analysis orchestration
  state/session.py     Centralized session state management
  views/               One module per page (dashboard, stock_analysis, etc.)
  components/          Reusable chart/table components
core/
  models.py            Pydantic models (FundDiff, CrowdedTrade, FundBaseline, etc.)
  aggregator.py        Cross-fund signal computation + baseline scoring
  diff_engine.py       Quarter-over-quarter position change detection
  report.py            Markdown report generation
data/
  store.py             SQLite persistence (holdings, funds, cusip_map, prices)
  edgar_client.py      SEC EDGAR 13F-HR filing downloader
  filing_parser.py     XML/HTML filing parser
  cusip_resolver.py    OpenFIGI CUSIP-to-ticker resolution
  performance_provider.py  Price performance (1w/1m/YTD/1yr returns)
config/
  watchlist.yaml       Fund watchlist (52 funds, 5 tiers)
  cusip_tickers.json   Bundled CUSIP-to-ticker seed (4,500+ mappings)
  settings.py          App configuration
scripts/
  export_cusip_seed.py Update bundled CUSIP seed from database
tests/
  42 tests covering models, diff engine, aggregator, store
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/
ruff check .
```

## License

MIT

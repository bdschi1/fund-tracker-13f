# Fund Tracker 13F

SEC 13F-HR filing analyzer tracking 52 hedge funds across 5 tiers. Surfaces high-conviction moves — new positions, full exits, concentrated adds, and consensus trades — ranked by signal strength (% share change), not dollar size.

## Pages

| # | Page | What it shows |
|---|------|---------------|
| 1 | **Dashboard** | Top findings, fund summary table, top moves chart, activity heatmap, concentration shifts |
| 2 | **Stock Analysis** | Search any ticker — which funds hold it, who initiated/exited, net sentiment |
| 3 | **Fund Deep Dive** | Single fund QoQ breakdown: every position change, AUM, concentration, filing lag |
| 4 | **Signal Scanner** | All position changes across all funds: new, exits, adds >50%, trims >60% |
| 5 | **Crowded Trades** | Consensus buys/sells (3+ funds), divergences (one buying what another sells) |
| 6 | **Overlap Matrix** | Fund-to-fund portfolio similarity heatmap, shared holdings Sankey |
| 7 | **Export Report** | Markdown report preview and download |

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

Optional — add an [OpenFIGI](https://www.openfigi.com/api) API key for CUSIP-to-ticker resolution:

```
FT13F_OPENFIGI_API_KEY="your-key"
```

## Run

```bash
streamlit run app/main.py
```

Click **Fetch & Analyze** in the sidebar. This downloads filings from SEC EDGAR, resolves CUSIPs, and runs the analysis (~1-3 min first time). Data is cached locally in SQLite.

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

## Development

```bash
pip install -e ".[dev]"
pytest tests/
ruff check .
```

## License

MIT

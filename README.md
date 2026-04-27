# MarketWatch

A modular premarket monitoring pipeline that ingests cross-asset price data, computes equity signals, tracks options volatility, reads Reddit sentiment, and emails a structured daily report before the US market open.

---

## Overview

MarketWatch runs every morning as a single orchestrated job (`jobs/premarket.py`). It fetches live data, computes signals, writes structured text artifacts, and emails a PDF report — all before the 9:30am open.

The pipeline is designed around four lanes that run in sequence:

1. **Reddit / ApeWisdom** — updates the dynamic ticker universe
2. **Equity** — prices, returns, volatility signals, sector overview
3. **Options** — IV spikes and put/call skew
4. **News** — GDELT headline clustering

---

## Pipeline

```
premarket.py
  ├─ 1. Reddit / ApeWisdom     → updates tickers.json (hot / candidates)
  ├─ 2. Equity                 → Polygon prices → signals → sector overview
  ├─ 3. Options                → Polygon options → IV spike flags
  ├─ 4. Claude                 → concludes and organizes artifacts into a report
  └─ 5. Email                  → PDF report via SMTP
```

---

## Features

### Prices — Polygon API
- Daily OHLC ingestion with incremental updates (backfill from 2024-01-01)
- Dividend-adjusted OHLC: `adj_open`, `adj_high`, `adj_low`, `adj_close` via total-return factor
- Adjustment factor (`adj_factor_total_return`) applied on ex-dividend date
- Cache-first: skips API call if CSV is already up to date

### Premarket Return — Schwab Developer API
- At run time, fetches live premarket price from Schwab for every ticker
- Computes `r_pc = (p_pre - prev_close) / prev_close` using raw close
- Replaces stale `ret_d` (yesterday's close-to-close) with the live premarket move
- `z_last_21` recomputed with `r_pc` as the current return against the historical baseline
- Graceful fallback to `r_cc[-1]` if Schwab is unavailable or outside premarket hours
- OAuth 2.0 refresh-token flow; refresh token auto-rotates on every use and is written back to `.env`

### Equity Signal Engine
Per-ticker daily metrics across the full universe:

| Metric | Description |
|--------|-------------|
| `ret_d` | Today's return — live premarket if available, else yesterday's close |
| `ret_w` / `ret_m` | 5-day / 21-day cumulative log return |
| `sigma_21` | 21-day rolling daily volatility |
| `ewma_sigma` | RiskMetrics EWMA volatility (λ = 0.94) |
| `ratio_ewma_vs_21` | EWMA vol / 21-day vol — regime change signal |
| `ratio_ewma_vs_spy` | EWMA vol / SPY EWMA vol — cross-sectional signal |
| `z_last_21` | Z-score of today's return vs 21-day baseline |
| `gap_mode` / `gap_ret` / `gap_z_21` | Overnight or intraday gap with z-score |
| `flag_vol_spike` | Vol regime shift flag |
| `flag_recent_abnormal` | Abnormal move flag |

**Flags:**
- `flag_vol_spike`: `ratio_ewma_vs_21 ≥ 1.5` AND `ratio_ewma_vs_spy ≥ 1.3`
- `flag_recent_abnormal`: `|z_last_21| ≥ 3.0` OR `|gap_z_21| ≥ 2.5`

### Sector & Macro Snapshot
Cross-asset daily return snapshot across equities, rates, credit, commodities, crypto, and volatility. Includes a derived Treasury–HY spread row (TLT − HYG). Appended to `data/sector.csv` as a wide-format time series.

### Options — IV Spike Detection
- ATM implied volatility series per ticker via Polygon options chains
- EWMA-based z-score of IV vs 21-day baseline
- IV spike flag: `z_iv_21 ≥ 2.5` AND `IV/SPY_IV ≥ 1.2`
- Cache-first with noop detection; rate-limited at 60s/request on Polygon free tier

### Reddit Sentiment — ApeWisdom
- Fetches top-100 mentions from ApeWisdom (WSB + all subreddits)
- 30-day rolling hot list / 60-day candidate list with decay logic
- Writes `tickers.json` with canonical keys: `benchmark`, `sector`, `core`, `hot`, `candidates`, `blocked`
- Universe used for equity + options pipeline is refreshed from disk after this step

### Ticker Universe — `data/config/tickers.json`
```json
{
  "benchmark": ["SPY"],
  "sector":    ["SPY", "QQQ", "GLD", "USO", "TLT", "LQD", "HYG", "UUP", "IBIT", "SMH", "XLF", "XLE", "VXX"],
  "core":      [...],
  "hot":       [...],
  "candidates": [...],
  "blocked":   [...]
}
```
`hot` and `candidates` are updated daily by the Reddit pipeline. `blocked` prevents common English words from being treated as tickers.

### Email Delivery
- SMTP with TLS (Gmail default)
- `jobs/send_report.py` — standalone script that finds the latest PDF in `data/morning_report/` and emails it
- Configurable recipients via `SMF_EMAIL_TO` env var

---

## Project Structure

```
market_watch/
├─ jobs/
│  ├─ premarket.py          # main pipeline entry point
│  └─ send_report.py        # standalone email sender
├─ src/
│  ├─ common/
│  │  └─ env.py             # .env loader, getenv_required
│  ├─ prices/
│  │  ├─ polygon_client.py  # Polygon OHLC + dividend fetch
│  │  ├─ schwab_client.py   # Schwab OAuth + premarket price
│  │  └─ smf_process.py     # signal engine (returns, vol, flags)
│  ├─ options/
│  │  ├─ options_client.py  # Polygon options chain fetch
│  │  └─ options_process.py # IV series + spike detection
│  ├─ reddit/
│  │  ├─ apewisdom.py       # ApeWisdom API client
│  │  └─ wisdomprocess.py   # ticker state + tickers.json writer
│  ├─ processor/            # report assembly and email payload
│  └─ utility/
│     ├─ constant.py        # paths, ticker lists, SMTP config
│     └─ emailer.py         # send_report / send_payload
├─ data/
│  ├─ config/
│  │  └─ tickers.json       # ticker universe (git-ignored)
│  ├─ prices/               # per-ticker OHLC CSVs (git-ignored)
│  ├─ options/              # per-ticker IV series CSVs (git-ignored)
│  ├─ reports/              # daily text artifacts (git-ignored)
│  └─ morning_report/       # PDF reports for email (git-ignored)
├─ .env.example
├─ requirements.txt
└─ README.md
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
```
Fill in `.env`:

| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io API key (free tier works) |
| `SCHWAB_CLIENT_ID` | Schwab Developer App Key |
| `SCHWAB_CLIENT_SECRET` | Schwab Developer App Secret |
| `SCHWAB_REFRESH_TOKEN` | Obtained via one-time OAuth browser flow (see below) |
| `REDDIT_CLIENT_ID` | Reddit app client ID |
| `REDDIT_CLIENT_SECRET` | Reddit app client secret |
| `SMF_SMTP_USER` | Gmail address for sending reports |
| `SMF_SMTP_PASS` | Gmail app password |
| `SMF_EMAIL_TO` | Comma-separated recipient list |
| `ANTHROPIC_API_KEY` | Used by Claude to conclude and organize the final report |

### 3. Schwab OAuth (one-time)
Register a free app at [developer.schwab.com](https://developer.schwab.com) to obtain your client credentials, then complete the one-time OAuth browser flow to get an initial refresh token. Save it as `SCHWAB_REFRESH_TOKEN` in `.env` — it auto-rotates on every run.

### 4. Run
```bash
python jobs/premarket.py
```

---

## Sample Output

[View Sample File](./report_20260416_121505.html)

---

## Notes

- All data, CSVs, and generated reports are excluded from the repository via `.gitignore`
- API keys and secrets are managed through environment variables only
- Polygon free tier: ~5 req/min — the pipeline applies 12s spacing between calls
- The Schwab premarket fetch happens per-ticker at analysis time; the access token is cached in-process and shared across all tickers in a single run

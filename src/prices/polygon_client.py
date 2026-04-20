# src/prices/polygon_client.py
# Minimal client for Polygon that supports:
# - Initial backfill to CSV (from hard start 2024-01-01) when a ticker has no CSV yet
# - Incremental updates for existing tickers based on each ticker's latest CSV date
# - Dividend-aware total return adjustment (all tickers, all runs)
#
# Uses market_date as the "end" date (premarket-safe; usually last trading day).

import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from typing import Optional, Iterable

import httpx
import pandas as pd
import numpy as np

from src.utility.constant import (
    SMF_TICKERS,
    LOOKBACK_DAYS,
    PRICES_DIR,
    RATE_LIMIT_SECS,
)
from src.common.env import getenv_required

BASE = "https://api.polygon.io"
DEFAULT_INIT_START = "2024-01-01"  # ISO format


# ---------- filesystem helpers ----------

def _ensure_data_dir():
    Path(PRICES_DIR).mkdir(parents=True, exist_ok=True)

def _csv_path(ticker: str) -> Path:
    return Path(PRICES_DIR) / f"{ticker.upper()}.csv"


# ---------- rate limit + HTTP ----------

_last_call = 0.0

def _rate_limit():
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < RATE_LIMIT_SECS:
        time.sleep(RATE_LIMIT_SECS - elapsed)
    _last_call = time.time()

def _get(path: str, params: dict):
    p = dict(params or {})
    api_key = getenv_required("POLYGON_API_KEY").strip().strip('"').strip("'")
    p["apiKey"] = api_key
    _rate_limit()
    with httpx.Client(timeout=30) as client:
        r = client.get(f"{BASE}{path}", params=p)
        if r.status_code == 429:
            time.sleep(15)
            r = client.get(f"{BASE}{path}", params=p)
        r.raise_for_status()
        return r.json()


# ---------- transforms ----------

def _rows_to_df(rows: list[dict], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close"])

    def one(r):
        d = datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat()
        return {
            "ticker": ticker.upper(),
            "date": d,
            "open": float(r["o"]),
            "high": float(r["h"]),
            "low":  float(r["l"]),
            "close":float(r["c"]),
        }

    return pd.DataFrame([one(r) for r in rows])


# ---------- dividend helpers ----------

def fetch_range_dividends(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch dividend history from Polygon /v3/reference/dividends endpoint.
    Returns DataFrame with [date, dividend] columns.
    Filters results to date range and sums dividends by ex_dividend_date.
    """
    rows = []
    path = "/v3/reference/dividends"
    params = {
        "ticker": ticker.upper(),
        "limit": 1000,
        "order": "asc",
        "sort": "ex_dividend_date",
    }

    while True:
        try:
            j = _get(path, params)
        except Exception as e:
            print(f"[warn] {ticker}: dividend fetch failed, continuing with 0 dividends: {e}")
            break

        results = j.get("results") or []

        for r in results:
            ex_date = r.get("ex_dividend_date")
            cash = r.get("cash_amount")
            if ex_date is None or cash is None:
                continue
            rows.append({
                "date": str(ex_date),
                "dividend": float(cash),
            })

        next_url = j.get("next_url")
        if not next_url:
            break

        # Parse next_url into path and params
        if next_url.startswith(BASE):
            path = next_url[len(BASE):]
        else:
            path = next_url

        params = {}

    if not rows:
        return pd.DataFrame(columns=["date", "dividend"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df = df.dropna(subset=["date"])
    df = df.groupby("date", as_index=False)["dividend"].sum()

    # Filter to date range
    df = df[(df["date"] >= str(start)) & (df["date"] <= str(end))].reset_index(drop=True)
    return df


def apply_total_return_adjustment(price_df: pd.DataFrame, dividend_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Enrich price_df with dividend and total-return-adjusted OHLC columns.
    All numeric values rounded to 4 decimals (0.0001 precision).
    Assumes price_df has: ticker, date, open, high, low, close.
    Returns full DataFrame with added: dividend, adj_factor_total_return, adj_open,
                adj_high, adj_low, adj_close
    """
    df = price_df.copy()

    if df.empty:
        for col in [
            "dividend",
            "adj_factor_total_return",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
        ]:
            if col not in df.columns:
                df[col] = pd.Series(dtype=float)
        return df

    # Normalize date format
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df = df.sort_values("date").reset_index(drop=True)

    # Ensure numeric types and round OHLC to 4 decimals upfront
    numeric_cols = ["open", "high", "low", "close"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    # Prepare dividend DataFrame
    if dividend_df is None or dividend_df.empty:
        div = pd.DataFrame(columns=["date", "dividend"])
    else:
        div = dividend_df.copy()
        div["date"] = pd.to_datetime(div["date"], errors="coerce").dt.date.astype(str)
        div = div.dropna(subset=["date"])
        div["dividend"] = pd.to_numeric(div["dividend"], errors="coerce").fillna(0.0).round(4)
        div = div.groupby("date", as_index=False)["dividend"].sum()

    # Merge dividends by date
    df = df.merge(div, on="date", how="left")
    df["dividend"] = df["dividend"].fillna(0.0).astype(float).round(4)

    # Compute forward cumulative total-return series
    adj_close_vals = []
    adj_factor_vals = []

    first_close = float(df.loc[0, "close"]) if pd.notna(df.loc[0, "close"]) else float("nan")
    adj_close_vals.append(first_close)
    adj_factor_vals.append(1.0 if pd.notna(first_close) and first_close != 0 else float("nan"))

    for i in range(1, len(df)):
        prev_close = df.loc[i - 1, "close"]
        curr_close = df.loc[i, "close"]
        curr_div = float(df.loc[i, "dividend"])

        # Safe total return: (close + div) / prev_close
        if pd.isna(prev_close) or prev_close <= 0 or pd.isna(curr_close):
            gross_ret = 1.0
        else:
            gross_ret = (float(curr_close) + curr_div) / float(prev_close)

        # Apply to cumulative adjusted close
        prev_adj_close = adj_close_vals[-1]
        if pd.isna(prev_adj_close):
            curr_adj_close = float(curr_close) if pd.notna(curr_close) else float("nan")
        else:
            curr_adj_close = prev_adj_close * gross_ret

        adj_close_vals.append(curr_adj_close)

        # Compute factor for this bar
        if pd.notna(curr_close) and curr_close != 0:
            adj_factor_vals.append(curr_adj_close / float(curr_close))
        else:
            adj_factor_vals.append(float("nan"))

    df["adj_close"] = adj_close_vals
    df["adj_close"] = df["adj_close"].round(4)

    df["adj_factor_total_return"] = adj_factor_vals
    df["adj_factor_total_return"] = df["adj_factor_total_return"].round(4)

    # Apply factor to all OHLC
    df["adj_open"] = (df["open"] * df["adj_factor_total_return"]).round(4)
    df["adj_high"] = (df["high"] * df["adj_factor_total_return"]).round(4)
    df["adj_low"] = (df["low"] * df["adj_factor_total_return"]).round(4)
    df["adj_close"] = (df["close"] * df["adj_factor_total_return"]).round(4)

    return df


def _one_year_range(market_date: Optional[date] = None) -> tuple[str, str]:
    end = market_date or date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start.isoformat(), end.isoformat()


# ---------- core fetchers ----------

def fetch_initial(ticker: str, start: Optional[str] = None, market_date: Optional[date] = None) -> pd.DataFrame:
    """
    Initial backfill for 'ticker' from DEFAULT_INIT_START (or custom start) through market_date.
    Fetches OHLC, enriches with dividends, writes CSV and returns the DataFrame saved.
    """
    _ensure_data_dir()
    start = start or DEFAULT_INIT_START
    end = (market_date or date.today()).isoformat()

    path = f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    j = _get(path, {"adjusted": "true", "sort": "asc", "limit": 50000})
    df = _rows_to_df(j.get("results") or [], ticker)

    # Enrich with dividends and adjusted columns
    if not df.empty:
        div_df = fetch_range_dividends(ticker, start, end)
        df = apply_total_return_adjustment(df, div_df)

    _write_csv_init(ticker, df)

    if df.empty:
        print(f"[warn] {ticker}: init returned 0 rows ({start}..{end})")
    else:
        print(f"[init-ok] {ticker}: wrote {len(df)} rows [{start}..{end}]")
    return df


def fetch_range_ohlc(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Pull daily bars for an explicit [start, end] inclusive range (ISO dates).
    Does NOT write; just returns a DataFrame.
    """
    path = f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    j = _get(path, {"adjusted": "true", "sort": "asc", "limit": 50000})
    return _rows_to_df(j.get("results") or [], ticker)


def fetch_recent_ohlc(tickers: list[str], market_date: Optional[date] = None):
    """
    Incremental update (PER-TICKER):
    - For each ticker:
        - If CSV missing -> initial backfill to market_date
        - Else -> fetch [latest+1 .. market_date] and merge
    Returns: dict {TICKER: {"mode": "init"|"update"|"noop"|"err", "rows": int, "range": "start..end"}}
    """
    if not tickers:
        return {}

    tickers = [t.upper() for t in tickers]
    results: dict[str, dict] = {}

    end_dt = market_date or date.today()
    end_date = end_dt.isoformat()

    for t in tickers:
        try:
            p = _csv_path(t)

            # init if missing
            if not p.exists():
                df_init = fetch_initial(t, start=DEFAULT_INIT_START, market_date=end_dt)
                results[t] = {"mode": "init", "rows": int(len(df_init)), "range": f"{DEFAULT_INIT_START}..{end_date}"}
                continue

            latest = _latest_csv_date(t)
            if latest is None:
                df_init = fetch_initial(t, start=DEFAULT_INIT_START, market_date=end_dt)
                results[t] = {"mode": "init", "rows": int(len(df_init)), "range": f"{DEFAULT_INIT_START}..{end_date}"}
                continue

            start_date = (latest + timedelta(days=1)).isoformat()

            if start_date > end_date:
                print(f"[noop] {t}: up to date (latest={latest})")
                results[t] = {"mode": "noop", "rows": 0, "range": ""}
                continue

            df_new = fetch_range_ohlc(t, start_date, end_date)
            _merge_csv_update(t, df_new)
            n = int(len(df_new))

            print(f"[ok] {t}: appended {n} rows [{start_date}..{end_date}]")
            results[t] = {"mode": "update", "rows": n, "range": f"{start_date}..{end_date}"}

        except httpx.HTTPStatusError as e:
            print(f"[HTTP {e.response.status_code}] {t}: {e}")
            results[t] = {"mode": "err", "rows": 0, "range": "", "error": f"HTTP {e.response.status_code}"}
        except Exception as e:
            print(f"[err] {t}: {e}")
            results[t] = {"mode": "err", "rows": 0, "range": "", "error": str(e)}

    return results


# ---------- CSV IO ----------

def _write_csv_init(ticker: str, df: pd.DataFrame):
    """Write initial CSV with dividend and adjusted columns."""
    _ensure_data_dir()
    p = _csv_path(ticker)
    if not df.empty:
        df = df.sort_values("date")
    df.to_csv(p, index=False)


def _merge_csv_update(ticker: str, df_new: pd.DataFrame):
    """Merge new OHLC rows into existing CSV and recompute all adjusted columns."""
    _ensure_data_dir()
    p = _csv_path(ticker)

    if not p.exists():
        _write_csv_init(ticker, df_new)
        return

    if df_new is None or df_new.empty:
        return

    # Read existing CSV
    df_old = pd.read_csv(p, dtype={"ticker": str})

    # Normalize dates
    if "date" in df_old.columns:
        df_old["date"] = pd.to_datetime(df_old["date"]).dt.date.astype(str)
    if "date" in df_new.columns:
        df_new["date"] = pd.to_datetime(df_new["date"]).dt.date.astype(str)

    # Merge histories
    df = pd.concat([df_old, df_new], ignore_index=True)
    df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Recalculate adjusted columns for entire history to ensure consistency
    # Extract OHLC and drop any prior adjusted columns to recompute fresh
    ohlc_cols = ["ticker", "date", "open", "high", "low", "close"]
    df_ohlc = df[ohlc_cols].copy() if all(c in df.columns for c in ohlc_cols) else df

    # Infer date range and fetch all dividends
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce")
        min_date = dates.min()
        max_date = dates.max()
        if pd.notna(min_date) and pd.notna(max_date):
            div_df = fetch_range_dividends(ticker, min_date.strftime("%Y-%m-%d"), max_date.strftime("%Y-%m-%d"))
            df_ohlc = apply_total_return_adjustment(df_ohlc, div_df)
        else:
            df_ohlc = apply_total_return_adjustment(df_ohlc, None)
    else:
        df_ohlc = apply_total_return_adjustment(df_ohlc, None)

    df_ohlc.to_csv(p, index=False)

def _latest_csv_date(ticker: str) -> Optional[date]:
    p = _csv_path(ticker)
    if not p.exists():
        return None
    df = pd.read_csv(p, usecols=["date"])
    if df.empty:
        return None
    d = pd.to_datetime(df["date"], errors="coerce").dropna()
    if d.empty:
        return None
    return d.max().date()


# ---------- public entrypoint ----------

def save_universe_excel(tickers: Optional[Iterable[str]] = None, market_date: Optional[date] = None):
    """
    For each ticker:
      - If CSV exists: append recent data up to market_date.
      - If CSV missing: initial backfill up to market_date.
    """
    tickers = list(tickers) if tickers is not None else list(SMF_TICKERS)
    return fetch_recent_ohlc(tickers, market_date=market_date)


if __name__ == "__main__":
    save_universe_excel()
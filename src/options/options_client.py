# options_client.py
# Build/append a daily series of *constant-maturity 30D ATM close IV* per ticker.
#
# Simplified approach:
# - Single API call: fetch options contracts filtered by both expiry + strike range
# - Post-process: find 2 closest-to-30D expiries, extract ATM bracket (below/above spot)
# - Compute: average IVs within expiry, interpolate across expiries for 30D constant-maturity
#
# Output per ticker: options/<TICKER>_atm_iv.csv
#   date, ticker, iv_cm_30d, method, expiry_lower, dte_lower, iv_lower, expiry_upper, dte_upper, iv_upper, weight

from __future__ import annotations
import math
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Iterable, List, Dict, Tuple

import httpx
import numpy as np
import pandas as pd

from src.utility.constant import SMF_TICKERS, PRICES_DIR, OPTIONS_DIR
from src.common.env import getenv_required

OPTIONS_DEFAULT_INIT_START = "2026-04-15"  # fallback start date for initial IV backfill

BASE = "https://api.polygon.io"
MASSIVE_BASE = "https://api.massive.com"

# Constants
TARGET_CM_DAYS = 30
EXP_LO_BUMP = 20
EXP_HI_BUMP = 50
MIN_DTE = 2
STRIKE_BAND = 0.05  # ±5% around spot
MIN_BAR_VOLUME = 10

# ======================== HTTP Helpers ========================

def _client() -> httpx.Client:
    return httpx.Client(timeout=30.0)

def _get(path: str, params: dict) -> dict:
    qp = dict(params or {})
    api_key = getenv_required("POLYGON_API_KEY").strip().strip('"').strip("'")
    qp["apiKey"] = api_key

    with _client() as c:
        r = c.get(f"{BASE}{path}", params=qp)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            print("HTTP ERROR")
            print("status:", r.status_code)
            print("url:", str(r.url))
            print("text:", r.text[:1000])
            raise
        return r.json()

def _get_massive(path: str, params: dict) -> dict:
    qp = dict(params or {})
    api_key = getenv_required("POLYGON_API_KEY").strip().strip('"').strip("'")
    qp["apiKey"] = api_key

    with _client() as c:
        r = c.get(f"{MASSIVE_BASE}{path}", params=qp)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            print("HTTP ERROR")
            print("status:", r.status_code)
            print("url:", str(r.url))
            print("text:", r.text[:1000])
            raise
        return r.json()

# ======================== File Path Helpers ========================

def _spot_path(ticker: str) -> Path:
    """Load spot price from prices directory."""
    return Path(PRICES_DIR) / f"{ticker.upper()}.csv"

def _iv_path(ticker: str) -> Path:
    """Save/load IV series from options directory."""
    return Path(OPTIONS_DIR) / f"{ticker.upper()}_atm_iv.csv"

def _ensure_data_dir():
    Path(OPTIONS_DIR).mkdir(parents=True, exist_ok=True)

# ======================== Spot Price Loading ========================

def _load_spot_series(ticker: str) -> pd.DataFrame:
    """Load per-ticker OHLC CSV and return df with ['date','spot_close'] sorted ascending."""
    p = _spot_path(ticker)
    if not p.exists():
        raise FileNotFoundError(f"Missing spot CSV for {ticker}: {p}")
    df = pd.read_csv(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df[["date", "close"]].rename(columns={"close": "spot_close"})

def _iv_series_last_date(ticker: str) -> Optional[date]:
    """Return most recent saved IV date from options/<TICKER>_atm_iv.csv, else None."""
    p = _iv_path(ticker)
    if not p.exists():
        return None
    df = pd.read_csv(p, usecols=["date"])
    if df.empty:
        return None
    d = pd.to_datetime(df["date"], errors="coerce").dropna()
    if d.empty:
        return None
    return d.max().date()

# ======================== Options Contracts (Single API Call) ========================

def _fetch_atm_contracts(
    ticker: str, 
    asof: date, 
    spot: float,
) -> pd.DataFrame:
    """
    Single API call to fetch call contracts with:
      - expiration: [asof+20d, asof+50d]
      - strike: [spot*0.95, spot*1.05]
    Returns df with columns: option_symbol, strike, type, expiration
    """
    exp_lo = (asof + timedelta(days=EXP_LO_BUMP)).isoformat()
    exp_hi = (asof + timedelta(days=EXP_HI_BUMP)).isoformat()
    strike_lo = spot * (1.0 - STRIKE_BAND)
    strike_hi = spot * (1.0 + STRIKE_BAND)

    j = _get("/v3/reference/options/contracts", {
        "underlying_ticker": ticker.upper(),
        "as_of": asof.isoformat(),
        "expiration_date.gte": exp_lo,
        "expiration_date.lte": exp_hi,
        "strike_price.gte": strike_lo,
        "strike_price.lte": strike_hi,
        "contract_type": "call",
        "limit": 100,
    })

    rows = []
    for item in (j.get("results") or []):
        try:
            rows.append({
                "option_symbol": item["ticker"],
                "strike": float(item["strike_price"]),
                "type": item["contract_type"],
                "expiration": pd.to_datetime(item["expiration_date"]).date(),
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["expiration"] >= asof]  # sanity check
    return df

def _option_daily_close(option_symbol: str, asof: date) -> Tuple[Optional[float], Optional[float]]:
    """Fetch the option's daily bar close price and volume for 'asof' date. Returns (close_price, volume)."""
    path = f"/v1/open-close/{option_symbol}/{asof.isoformat()}"
    try:
        j = _get_massive(path, {"adjusted": "true"})
    except httpx.HTTPStatusError as e:
        print(f"[option-close-error] symbol={option_symbol} date={asof} status={e.response.status_code}")
        print(e.response.text[:1000])
        return (None, None)
    except httpx.HTTPError as e:
        print(f"[option-close-network-error] symbol={option_symbol} date={asof} err={e}")
        return (None, None)
    if j.get("status") != "OK":
        return (None, None)
    close = float(j["close"]) if j.get("close") is not None else None
    vol = float(j["volume"]) if j.get("volume") is not None else None
    return (close, vol)

# ======================== Black-Scholes Helpers ========================

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _bs_price(is_call: bool, S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return float("nan")
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)

def _implied_vol_bisection(
    is_call: bool, S: float, K: float, T: float, r: float, q: float, price: float,
    tol: float = 1e-6, max_iter: int = 100, low: float = 1e-4, high: float = 5.0
) -> Optional[float]:
    """Solve for IV using bisection."""
    if price is None or price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
    upper_bound = S if is_call else K
    if price < intrinsic - 1e-8 or price > upper_bound:
        return None

    lo, hi = low, high
    f_lo = _bs_price(is_call, S, K, T, r, q, lo) - price
    f_hi = _bs_price(is_call, S, K, T, r, q, hi) - price

    tries = 0
    while f_lo * f_hi > 0 and tries < 10:
        hi *= 2.0
        f_hi = _bs_price(is_call, S, K, T, r, q, hi) - price
        tries += 1
        if hi > 100.0:
            break

    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        f_mid = _bs_price(is_call, S, K, T, r, q, mid) - price
        if abs(f_mid) < tol:
            return float(mid)
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return float(0.5 * (lo + hi))

def _risk_free_rate(asof: date, T_years: float) -> float:
    """Flat proxy for risk-free rate."""
    return 0.03

# ======================== ATM IV Computation ========================

def _atm_iv_for_expiry(
    ticker: str, 
    expiry: date, 
    spot: float, 
    asof: date,
    contracts_df: pd.DataFrame
) -> Optional[Tuple[float, dict]]:
    """
    Compute ATM IV for a specific expiry.
    - Find strikes immediately below & above spot
    - Fetch daily close for both
    - Solve for IVs, average them
    Returns (iv_atm, trace) or None
    """
    # Filter contracts for this expiry
    meta = contracts_df[contracts_df["expiration"] == expiry]
    if meta.empty:
        return None

    strikes = sorted(meta["strike"].unique())
    lo_strike = max([k for k in strikes if k <= spot], default=None)
    hi_strike = min([k for k in strikes if k >= spot], default=None)

    if lo_strike is None and hi_strike is None:
        return None

    # Collect IVs from available strikes
    ivs = []
    info_dict = {}

    for strike, name in [(lo_strike, "lo"), (hi_strike, "hi")]:
        if strike is None:
            continue
        row = meta[meta["strike"] == strike]
        if row.empty:
            continue
        sym = row["option_symbol"].iloc[0]
        px, vol = _option_daily_close(sym, asof)
        if px is None or (vol is not None and vol < MIN_BAR_VOLUME):
            continue

        T = max((expiry - asof).days, MIN_DTE) / 365.0
        r = _risk_free_rate(asof, T)
        q = 0.0

        iv = _implied_vol_bisection(is_call=True, S=float(spot), K=float(strike), T=T, r=r, q=q, price=float(px))
        if iv is None or not (0 < iv < 5.0):
            continue

        ivs.append(iv)
        info_dict[f"{name}_strike"] = strike
        info_dict[f"{name}_iv"] = iv

    if not ivs:
        return None

    # Average the IVs (if both exist, else just the one)
    iv_avg = float(np.mean(ivs))
    trace = {
        "method": "bracket_avg" if len(ivs) == 2 else "single",
        **info_dict
    }
    return iv_avg, trace

def _constant_maturity_atm_iv(
    ticker: str,
    asof: date,
    spot: float,
    contracts_df: pd.DataFrame
) -> Optional[Tuple[float, dict]]:
    """
    Compute 30D ATM IV. Find the two expiries closest to 30D (one ≤30d, one ≥30d).
    Try to compute ATM IV for both; if only one side works, return it as-is.
    If both work, average the two IVs.
    """
    expiries = sorted([d for d in contracts_df["expiration"].unique() if (d - asof).days >= MIN_DTE])
    if not expiries:
        return None

    def dte(d):
        return (d - asof).days

    # Find lower (≤30d) and upper (≥30d) expiries closest to target
    lower = None
    upper = None
    for d in expiries:
        if dte(d) <= TARGET_CM_DAYS:
            lower = d
        if dte(d) >= TARGET_CM_DAYS and upper is None:
            upper = d

    # Fallback: if all expiries are on one side, just pick closest
    if lower is None and upper is not None:
        lower = upper
    if upper is None and lower is not None:
        upper = lower

    if lower is None:
        return None

    # Compute ATM IV for both expiries
    result_l = _atm_iv_for_expiry(ticker, lower, spot, asof, contracts_df) if lower else None
    result_u = _atm_iv_for_expiry(ticker, upper, spot, asof, contracts_df) if upper != lower else None

    # Both failed
    if result_l is None and result_u is None:
        return None

    # Only one side available — return it directly, no interpolation
    if result_l is None:
        iv_u, tr_u = result_u
        return iv_u, {"method": "single_expiry", "expiry": upper.isoformat(), "dte": dte(upper), "iv": iv_u, **tr_u}

    if result_u is None:
        iv_l, tr_l = result_l
        return iv_l, {"method": "single_expiry", "expiry": lower.isoformat(), "dte": dte(lower), "iv": iv_l, **tr_l}

    # Both available — average them
    iv_l, tr_l = result_l
    iv_u, tr_u = result_u
    iv_avg = float(np.mean([iv_l, iv_u]))

    trace = {
        "method": "two_expiry_avg",
        "expiry_lower": lower.isoformat(),
        "dte_lower": dte(lower),
        "iv_lower": iv_l,
        "expiry_upper": upper.isoformat(),
        "dte_upper": dte(upper),
        "iv_upper": iv_u,
        **{f"lo_{k}": v for k, v in tr_l.items()},
        **{f"up_{k}": v for k, v in tr_u.items()},
    }
    return iv_avg, trace

# ======================== CSV IO ========================

def _write_iv_csv(ticker: str, df: pd.DataFrame):
    """Write IV DataFrame to CSV (init path)."""
    _ensure_data_dir()
    p = _iv_path(ticker)
    if not df.empty:
        df = df.sort_values("date")
    df.to_csv(p, index=False)


def _merge_iv_update(ticker: str, df_new: pd.DataFrame):
    """Merge new IV rows into existing CSV: concat, dedup on date, sort, save."""
    _ensure_data_dir()
    p = _iv_path(ticker)
    if not p.exists():
        _write_iv_csv(ticker, df_new)
        return
    existing = pd.read_csv(p)
    merged = pd.concat([existing, df_new], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    merged.to_csv(p, index=False)


# ======================== Core Range Fetcher ========================

def fetch_range_atm_iv(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch ATM 30D CM IV for each market date in [start, end] (ISO strings).
    Reads spot prices from the prices CSV; skips dates with no spot data.
    Returns a DataFrame of computed IV rows (no file I/O).
    """
    t = ticker.upper()
    try:
        spot_df = _load_spot_series(t)
    except FileNotFoundError as e:
        print(f"[spot-missing] {t}: {e}")
        return pd.DataFrame()

    # Vectorized filter: one pass, eliminates O(n²) per-date lookup
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    filtered = spot_df[
        (spot_df["date"] >= start_ts) & (spot_df["date"] <= end_ts)
    ].reset_index(drop=True)

    if filtered.empty:
        return pd.DataFrame()

    out_rows = []
    for row in filtered.itertuples(index=False):
        d = row.date.date()
        S = float(row.spot_close)

        try:
            contracts = _fetch_atm_contracts(t, asof=d, spot=S)
        except Exception as e:
            print(f"[contracts-error] {t} on {d}: {e}")
            continue

        if contracts.empty:
            continue

        cm = _constant_maturity_atm_iv(t, asof=d, spot=S, contracts_df=contracts)
        if cm is None:
            continue

        iv_val, trace = cm
        out_rows.append({
            "date": d.isoformat(),
            "ticker": t,
            "iv_cm_30d": iv_val,
            **trace,
        })

    return pd.DataFrame(out_rows)


# ======================== Public Entrypoints ========================

def fetch_initial_atm_iv(
    ticker: str,
    start: Optional[str] = None,
    market_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Full backfill for ticker from OPTIONS_DEFAULT_INIT_START (or custom start)
    through market_date. Writes CSV and returns the saved DataFrame.
    """
    _ensure_data_dir()
    start = start or OPTIONS_DEFAULT_INIT_START
    end = (market_date or date.today()).isoformat()
    df = fetch_range_atm_iv(ticker, start, end)
    _write_iv_csv(ticker, df)
    t = ticker.upper()
    if df.empty:
        print(f"[warn] {t}: init returned 0 rows ({start}..{end})")
    else:
        print(f"[init-ok] {t}: wrote {len(df)} rows [{start}..{end}]")
    return df


def fetch_recent_atm_iv(
    tickers: Optional[Iterable[str]] = None,
    market_date: Optional[date] = None,
) -> Dict[str, dict]:
    """
    Incremental updater (mirrors fetch_recent_ohlc):
    - CSV missing or empty  -> full backfill from OPTIONS_DEFAULT_INIT_START
    - CSV exists            -> fetch only [last_saved+1 .. market_date]
    Returns {TICKER: {"mode": "init"|"update"|"noop"|"err", "rows": int, "range": str}}
    """
    tickers = list(tickers) if tickers is not None else list(SMF_TICKERS)
    _ensure_data_dir()
    results: Dict[str, dict] = {}

    end_dt = market_date or date.today()
    end_date = end_dt.isoformat()

    for i, ticker in enumerate(map(str.upper, tickers)):
        # Pause every 3 tickers to avoid rate-limit on free tier
        if i > 0:
            print(f"\n=== [Rate-limit pause] {i}/{len(tickers)} tickers done — waiting 60s ===")
            for remaining in range(60, 0, -10):
                print(f"  ...{remaining}s remaining")
                time.sleep(10)
            print("  ...resuming")

        try:
            p = _iv_path(ticker)

            # init if missing
            if not p.exists():
                df_init = fetch_initial_atm_iv(ticker, start=OPTIONS_DEFAULT_INIT_START, market_date=end_dt)
                results[ticker] = {"mode": "init", "rows": len(df_init), "range": f"{OPTIONS_DEFAULT_INIT_START}..{end_date}"}
                continue

            latest = _iv_series_last_date(ticker)
            if latest is None:
                # file exists but is empty / unparseable -> re-init
                df_init = fetch_initial_atm_iv(ticker, start=OPTIONS_DEFAULT_INIT_START, market_date=end_dt)
                results[ticker] = {"mode": "init", "rows": len(df_init), "range": f"{OPTIONS_DEFAULT_INIT_START}..{end_date}"}
                continue

            start_date = (latest + timedelta(days=1)).isoformat()

            if start_date > end_date:
                print(f"[noop] {ticker}: up to date (latest={latest})")
                results[ticker] = {"mode": "noop", "rows": 0, "range": ""}
                continue

            df_new = fetch_range_atm_iv(ticker, start_date, end_date)
            if not df_new.empty:
                _merge_iv_update(ticker, df_new)
            n = len(df_new)
            print(f"[iv-ok] {ticker}: appended {n} rows [{start_date}..{end_date}]")
            results[ticker] = {"mode": "update", "rows": n, "range": f"{start_date}..{end_date}"}

        except httpx.HTTPStatusError as e:
            print(f"[HTTP {e.response.status_code}] {ticker}: {e}")
            results[ticker] = {"mode": "err", "rows": 0, "range": "", "error": f"HTTP {e.response.status_code}"}
        except Exception as e:
            print(f"[err] {ticker}: {e}")
            results[ticker] = {"mode": "err", "rows": 0, "range": "", "error": str(e)}

    return results


# Backward-compatible alias
def update_atm_iv_series(
    tickers: Optional[Iterable[str]] = None,
    market_date: Optional[date] = None,
) -> Dict[str, dict]:
    """Alias for fetch_recent_atm_iv (backward compatibility)."""
    return fetch_recent_atm_iv(tickers, market_date=market_date)

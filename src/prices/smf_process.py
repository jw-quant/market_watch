# smf_process.py
# Build a minimal abnormal-volatility report using daily data only.
# All units are DAILY (not annualized).
#
# Columns per ticker:
#   symbol, asof, ret_d, ret_w, ret_m, sigma_21, ewma_sigma,
#   ratio_ewma_vs_21, ratio_ewma_vs_spy,
#   r_last, z_last_21, gap_mode, gap_ret, gap_z_21,
#   flag_vol_spike, flag_recent_abnormal
#
# Flags (tunable):
#   flag_vol_spike: (ratio_ewma_vs_21 >= 1.5) AND (ratio_ewma_vs_spy >= 1.3)
#   flag_recent_abnormal: (abs(z_last_21) >= 3.0) OR (abs(gap_z_21) >= 2.5)

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple, List, Dict

import numpy as np
import pandas as pd
from datetime import date

from src.utility.constant import SMF_TICKERS, BENCHMARK, SECTOR_TICKERS, PRICES_DIR, REPORTS_DIR, DATA_DIR, SECTOR_TICKER_NAME_MAP

LAM = 0.94  # EWMA lambda (RiskMetrics-ish)
W_W, W_M = 5, 21            # week, month in trading days
BASELINE_W = 21             # baseline window for z-scores and ratios

@dataclass
class TickerData:
    symbol: str
    df: pd.DataFrame  # columns: date (datetime64[ns]), open, high, low, close
    r_cc: pd.Series   # close->close daily log returns (aligned to df dates)
    r_oc: pd.Series   # overnight gap log returns (open / prev close)
    r_co: pd.Series   # intraday move log returns (close / open)
    r_pc: float | None = None  # live premarket simple return vs raw prev close; None if unavailable

def _csv_path(symbol: str) -> Path:
    return Path(PRICES_DIR) / f"{symbol.upper()}.csv"

def _load_one(symbol: str) -> TickerData:
    p = _csv_path(symbol)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found for {symbol}: {p}")
    df = pd.read_csv(p, dtype={"ticker": str})
    # Parse and sort by date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Build daily log-returns
    # Use dividend-adjusted close if available, otherwise use raw close
    px_col = "adj_close" if "adj_close" in df.columns else "close"
    c = df[px_col].astype(float)
    px_col = "adj_open" if "adj_open" in df.columns else "open"
    o = df[px_col].astype(float)

    # close-to-close (align to current day)
    r_cc = np.log(c / c.shift(1))
    # overnight: open vs previous close (align to current day)
    r_oc = np.log(o / c.shift(1))
    # intraday: close vs open (same day)
    r_co = np.log(c / o)

    r_pc: float | None = None
    try:
        today = date.today()
        last_csv_date = df["date"].iloc[-1].date()
        if today.weekday() < 5 and today > last_csv_date:
            from src.prices.schwab_client import fetch_premarket_price
            p_pre = fetch_premarket_price(symbol)
            if p_pre is not None:
                prev_close = float(df["close"].iloc[-1])
                if prev_close > 0:
                    r_pc = (p_pre - prev_close) / prev_close
    except Exception as exc:
        print(f"[schwab] r_pc failed for {symbol}: {exc}")
        r_pc = None

    return TickerData(symbol=symbol.upper(), df=df, r_cc=r_cc, r_oc=r_oc, r_co=r_co, r_pc=r_pc)

def _ewma_sigma_daily(r: pd.Series, lam: float = LAM) -> float | np.nan:
    if r.dropna().empty:
        return np.nan
    # EWMA variance of squared returns; adjust=False for recursion-like behavior
    ewm_var = r.pow(2).ewm(alpha=(1 - lam), adjust=False).mean()
    return float(np.sqrt(ewm_var.iloc[-1]))

def _rolling_sigma(r: pd.Series, window: int) -> float | np.nan:
    s = r.dropna().iloc[-window:]
    if len(s) < max(5, window // 2):
        return np.nan
    return float(s.std(ddof=1))

def _cum_log_ret(r: pd.Series, window: int) -> float | np.nan:
    s = r.dropna().iloc[-window:]
    if len(s) == 0:
        return np.nan
    # cumulative simple return = exp(sum(log r)) - 1
    return float(np.exp(s.sum()) - 1.0)

def _z_last(r: pd.Series, window: int = BASELINE_W) -> float | np.nan:
    """z of the last daily return vs the previous window's mean/std (daily units)."""
    r = r.dropna()
    if len(r) < window + 1:
        return np.nan
    r_last = float(r.iloc[-1])
    baseline = r.iloc[-(window + 1):-1]  # previous 'window' obs excluding last
    mu = baseline.mean()
    sd = baseline.std(ddof=1)
    if sd == 0 or np.isnan(sd):
        return np.nan
    return float((r_last - mu) / sd)

def _gap_choice_and_z(td: TickerData, today: date) -> Tuple[str | None, float | np.nan, float | np.nan]:
    """
    Decide whether to use OC (overnight gap) or CO (intraday) as the 'most recent gap',
    then compute its 21-day z-score using the matching history.
    - If latest row date == today -> use CO (close/open for today).
    - Else -> use OC (open/prev_close for latest available trading day).
    Returns: (mode, gap_ret, gap_z_21)
    """
    df = td.df
    if df.empty:
        return None, np.nan, np.nan

    last_date = df["date"].iloc[-1].date()
    use_co = (last_date == today)

    if use_co:
        mode = "CO"
        gap_series = td.r_co.dropna()
    else:
        mode = "OC"
        gap_series = td.r_oc.dropna()

    if gap_series.empty:
        return mode, np.nan, np.nan

    gap_ret = float(gap_series.iloc[-1])

    # Baseline: previous 21 gaps of the same type (exclude the most recent)
    if len(gap_series) < BASELINE_W + 1:
        return mode, gap_ret, np.nan
    base = gap_series.iloc[-(BASELINE_W + 1):-1]
    mu, sd = base.mean(), base.std(ddof=1)
    if sd == 0 or np.isnan(sd):
        return mode, gap_ret, np.nan
    z = float((gap_ret - mu) / sd)
    return mode, gap_ret, z

def _summarize_one(td: TickerData, spy_ewma_sigma: float, today: date) -> Dict[str, float | str | bool]:
    asof = td.df["date"].iloc[-1].date() if not td.df.empty else None

    # returns
    ret_w = _cum_log_ret(td.r_cc, W_W)
    ret_m = _cum_log_ret(td.r_cc, W_M)

    # vols
    sigma_21 = _rolling_sigma(td.r_cc, BASELINE_W)
    ewma_sigma = _ewma_sigma_daily(td.r_cc, LAM)

    # ratios
    ratio_ewma_vs_21 = ewma_sigma / sigma_21 if (sigma_21 and sigma_21 > 0) else np.nan
    ratio_ewma_vs_spy = ewma_sigma / spy_ewma_sigma if (spy_ewma_sigma and spy_ewma_sigma > 0) else np.nan

    # last daily move z — prefer live premarket return if available
    ret_d = (
        td.r_pc
        if td.r_pc is not None
        else (float(td.r_cc.dropna().iloc[-1]) if td.r_cc.dropna().size else np.nan)
    )
    r_last = ret_d

    if td.r_pc is not None:
        _r = td.r_cc.dropna()
        if len(_r) >= BASELINE_W + 1:
            _baseline = _r.iloc[-(BASELINE_W + 1):-1]
            _mu, _sd = float(_baseline.mean()), float(_baseline.std(ddof=1))
            z_last_21 = float((td.r_pc - _mu) / _sd) if (_sd > 0 and not np.isnan(_sd)) else np.nan
        else:
            z_last_21 = np.nan
    else:
        z_last_21 = _z_last(td.r_cc, BASELINE_W)

    # gap choice + z
    mode, gap_ret, gap_z_21 = _gap_choice_and_z(td, today=today)

    # flags
    flag_vol_spike = bool(
        (ratio_ewma_vs_21 is not np.nan and ratio_ewma_vs_21 >= 1.5) and
        (ratio_ewma_vs_spy is not np.nan and ratio_ewma_vs_spy >= 1.3)
    )
    flag_recent_abnormal = bool(
        (not np.isnan(z_last_21) and abs(z_last_21) >= 3.0) or
        (not np.isnan(gap_z_21) and abs(gap_z_21) >= 2.5)
    )

    return dict(
        symbol=td.symbol,
        asof=str(asof) if asof else None,
        ret_d=ret_d,
        ret_w=ret_w,
        ret_m=ret_m,
        sigma_21=sigma_21,
        ewma_sigma=ewma_sigma,
        ratio_ewma_vs_21=ratio_ewma_vs_21,
        ratio_ewma_vs_spy=ratio_ewma_vs_spy,
        r_last=r_last,
        z_last_21=z_last_21,
        gap_mode=mode,
        gap_ret=gap_ret,
        gap_z_21=gap_z_21,
        flag_vol_spike=flag_vol_spike,
        flag_recent_abnormal=flag_recent_abnormal,
    )

def _summarize_macro_one(td: TickerData, today: date) -> Dict[str, float | str]:
    asof = td.df["date"].iloc[-1].date() if not td.df.empty else None
    ret_d = (
        td.r_pc
        if td.r_pc is not None
        else (float(td.r_cc.dropna().iloc[-1]) if td.r_cc.dropna().size else np.nan)
    )
    return dict(
        symbol=td.symbol,
        asof=str(asof) if asof else None,
        ret_d=ret_d,
    )


def generate_macro(tickers: Iterable[str] | None = None, asof_date: date | str | None = None) -> pd.DataFrame:
    """
    Build a simple macro summary for benchmark + sector tickers.
    Returns a DataFrame with columns: symbol, asof, ret_d
    """
    tickers = list(tickers) if tickers is not None else list(BENCHMARK + SECTOR_TICKERS)
    if BENCHMARK and BENCHMARK[0] not in tickers:
        tickers = [BENCHMARK[0]] + tickers
    tickers = list(dict.fromkeys([t.upper() for t in tickers]))

    today = date.today()
    if asof_date is not None:
        today = date.fromisoformat(asof_date) if isinstance(asof_date, str) else asof_date

    td_map: Dict[str, TickerData] = {}
    for t in tickers:
        td_map[t.upper()] = _load_one(t)

    rows = []
    for t in tickers:
        rows.append(_summarize_macro_one(td_map[t.upper()], today=today))

    summary = pd.DataFrame(rows)
    summary = summary.sort_values("symbol").reset_index(drop=True)

    # Add display name for sector overview
    summary["name"] = summary["symbol"].map(SECTOR_TICKER_NAME_MAP).fillna("")

    # Add derived row: Treasury - HY (TLT ret_d - HYG ret_d)
    tlt_val = summary.loc[summary["symbol"] == "TLT", "ret_d"]
    hyg_val = summary.loc[summary["symbol"] == "HYG", "ret_d"]
    tlt = float(tlt_val.iloc[0]) if not tlt_val.empty and not pd.isna(tlt_val.iloc[0]) else np.nan
    hyg = float(hyg_val.iloc[0]) if not hyg_val.empty and not pd.isna(hyg_val.iloc[0]) else np.nan
    treasury_hy = float(tlt - hyg) if not np.isnan(tlt) and not np.isnan(hyg) else np.nan

    derived_row = pd.DataFrame([{
        "symbol": "Treasury HY spr",
        "asof": pd.NaT,
        "ret_d": treasury_hy,
        "name": "Treasury HY spr",
    }])

    summary = pd.concat([summary, derived_row], ignore_index=True)

    # Persist wide-format per-day snapshot to data/sector.csv (one row per market_date)
    market_date = today.isoformat() if isinstance(today, date) else str(today)
    sector_file = Path(DATA_DIR) / "sector.csv"
    fixed_cols = [
        "Date", "S&P 500", "Nasdaq", "Gold", "Oil", "Long Treasuries", "Investment Grade Credit",
        "Treasury - HY", "US Dollar", "Bitcoin", "VIX", "Chips", "Financials", "Energy", "Notes"
    ]

    if sector_file.exists():
        sector_df = pd.read_csv(sector_file, dtype=str)
    else:
        sector_df = pd.DataFrame(columns=fixed_cols)

    if not ((sector_df["Date"] == market_date).any() if "Date" in sector_df.columns else False):
        row_map = summary.set_index("name")["ret_d"].to_dict()
        wide_row = {
            "Date": market_date,
            "S&P 500": row_map.get("S&P 500", ""),
            "Nasdaq": row_map.get("Nasdaq", ""),
            "Gold": row_map.get("Gold", ""),
            "Oil": row_map.get("Oil", ""),
            "Long Treasuries": row_map.get("Long Treasuries", ""),
            "Investment Grade Credit": row_map.get("Investment Grade Credit", ""),
            "Treasury - HY": treasury_hy,
            "US Dollar": row_map.get("US Dollar", ""),
            "Bitcoin": row_map.get("Bitcoin", ""),
            "VIX": row_map.get("VIX", ""),
            "Chips": row_map.get("Chips", ""),
            "Financials": row_map.get("Financials", ""),
            "Energy": row_map.get("Energy", ""),
            "Notes": "",
        }
        sector_df = pd.concat([sector_df, pd.DataFrame([wide_row])], ignore_index=True)
        sector_df = sector_df[fixed_cols]
        sector_df.to_csv(sector_file, index=False)

    return summary


def generate_reports(tickers: Iterable[str] | None = None, asof_date: date | str | None = None) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Build the summary DataFrame for SMF tickers (incl. SPY) and return:
      (summary_df, vol_spike_symbols, recent_abnormal_symbols)
    Also writes data/summary_YYYYMMDD.csv
    """
    tickers = list(tickers) if tickers is not None else list(SMF_TICKERS)

    # Ensure SPY is present and first (benchmark)
    bench = BENCHMARK[0] if BENCHMARK else "SPY"
    if bench not in tickers:
        tickers = [bench] + tickers

    # Load all, compute spy ewma once
    td_map: Dict[str, TickerData] = {}
    for t in tickers:
        td_map[t.upper()] = _load_one(t)

    spy_ewma_sigma = _ewma_sigma_daily(td_map[bench.upper()].r_cc, LAM)

    today = date.today()
    if asof_date is not None:
        today = date.fromisoformat(asof_date) if isinstance(asof_date, str) else asof_date

    rows = []
    for t in tickers:
        rows.append(_summarize_one(td_map[t.upper()], spy_ewma_sigma, today=today))

    summary = pd.DataFrame(rows)
    summary = summary.sort_values("symbol").reset_index(drop=True)

    # Save snapshot
    out_dir = Path(REPORTS_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    today = date.today() if asof_date is None else (date.fromisoformat(asof_date) if isinstance(asof_date, str) else asof_date)
    out_path = out_dir / f"equity_summary_{today.strftime('%Y%m%d')}.csv"
    summary.to_csv(out_path, index=False)

    # Flag lists
    vol_spike = summary.loc[summary["flag_vol_spike"] == True, "symbol"].tolist()
    recent_abn = summary.loc[summary["flag_recent_abnormal"] == True, "symbol"].tolist()

    return summary, vol_spike, recent_abn

# options_process.py
# Build a simple options IV monitor from options/<TICKER>_atm_iv.csv files.
# Signals (daily units):
#   - ewma_iv (λ=0.94) on iv_cm_30d
#   - z_iv_21 vs last 21 values (exclude current)
#   - iv_ratio_vs_spy (name / SPY) using the same 30D CM series
# Flags:
#   - OptionsIVSpike: z_iv_21 >= 2.5 AND iv_ratio_vs_spy >= 1.2

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple, List, Dict, Optional

import numpy as np
import pandas as pd
from datetime import date

from src.utility.constant import SMF_TICKERS, OPTIONS_DIR, REPORTS_DIR, BENCHMARK

LAM = 0.94
BASELINE_W = 21

def _iv_path(ticker: str) -> Path:
    """Load IV series from options directory."""
    return Path(OPTIONS_DIR) / f"{ticker.upper()}_atm_iv.csv"

def _load_iv_series(ticker: str) -> pd.DataFrame:
    p = _iv_path(ticker)
    if not p.exists():
        raise FileNotFoundError(f"IV CSV not found for {ticker}: {p}")
    df = pd.read_csv(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df[["date", "iv_cm_30d"]].rename(columns={"iv_cm_30d": "iv"})

def _ewma(x: pd.Series, lam=LAM) -> Optional[float]:
    xx = x.dropna()
    if xx.empty:
        return None
    return float(xx.ewm(alpha=(1 - lam), adjust=False).mean().iloc[-1])

def _z_last(x: pd.Series, window=BASELINE_W) -> Optional[float]:
    xx = x.dropna()
    if len(xx) < window + 1:
        return None
    x_last = float(xx.iloc[-1])
    base = xx.iloc[-(window + 1):-1]
    mu = base.mean()
    sd = base.std(ddof=1)
    if sd == 0 or np.isnan(sd):
        return None
    return float((x_last - mu) / sd)

def generate_options_report(tickers: Iterable[str] | None = None):
    """
    Build options summary over the universe (incl. SPY) and return:
      (summary_df, options_iv_spike_list)
    Also writes reports/options_summary_YYYYMMDD.csv
    """
    tickers = list(tickers) if tickers is not None else list(SMF_TICKERS)
    bench = BENCHMARK[0] if BENCHMARK else "SPY"
    if bench not in tickers:
        tickers = [bench] + tickers

    # Load all IV series
    iv_map: Dict[str, pd.DataFrame] = {}
    for t in map(str.upper, tickers):
        try:
            iv_map[t] = _load_iv_series(t)
        except FileNotFoundError as e:
            # Log missing files for visibility
            print(f"[iv-missing] {t}: {e}")
            continue

    if bench.upper() not in iv_map:
        raise RuntimeError(f"Missing {bench} options IV series; ensure {bench}_atm_iv.csv exists in {OPTIONS_DIR}")

    # Build latest SPY iv (ewma or close)
    spy_iv = iv_map[bench.upper()]["iv"]
    spy_latest = spy_iv.dropna().iloc[-1] if not spy_iv.dropna().empty else np.nan

    rows = []
    for t in tickers:
        df = iv_map.get(t.upper())
        if df is None or df.empty:
            continue
        ewma_iv = _ewma(df["iv"])
        z_iv_21 = _z_last(df["iv"])
        latest_iv = df["iv"].dropna().iloc[-1] if not df["iv"].dropna().empty else np.nan
        ratio_vs_spy = (latest_iv / spy_latest) if (spy_latest and not np.isnan(spy_latest) and spy_latest > 0) else np.nan

        flag_iv_spike = bool(
            (not np.isnan(z_iv_21) and z_iv_21 >= 2.5) and
            (not np.isnan(ratio_vs_spy) and ratio_vs_spy >= 1.2)
        )

        rows.append(dict(
            ticker=t.upper(),
            asof=str(df["date"].iloc[-1].date()),
            iv_latest=latest_iv,
            ewma_iv=ewma_iv,
            z_iv_21=z_iv_21,
            iv_ratio_vs_spy=ratio_vs_spy,
            flag_options_iv_spike=flag_iv_spike,
        ))

    summary = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)

    out_dir = Path(REPORTS_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    out_path = out_dir / f"options_summary_{today}.csv"
    summary.to_csv(out_path, index=False)
    print(f"[options-report] saved to {out_path.name}")

    flagged = summary.loc[summary["flag_options_iv_spike"] == True, "ticker"].tolist()
    return summary, flagged

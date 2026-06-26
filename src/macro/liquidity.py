"""
Fetch FRED macro liquidity series and cache to disk.

Raw output:
  data/macro/liquidity/YYYY-MM-DD/liquidity_raw.csv  (dated copy)
  data/macro/liquidity/liquidity_raw.csv             (rolling latest)

Series fetched:
  WALCL      Fed Total Assets
  WTREGEN    Treasury General Account (TGA)
  RRPONTSYD  Overnight Reverse Repo (RRP)
  WRESBAL    Bank Reserves
  DFII10     10Y Real Yield
  SOFR       SOFR

Usage:
  python src/macro/liquidity.py
  python src/macro/liquidity.py --force-refresh
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
import pandas as pd

from src.common.env import load_env

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
_MACRO_DIR = Path(__file__).resolve().parents[2] / "data" / "macro" / "liquidity"

SERIES_MAP: dict[str, str] = {
    "WALCL":     "Fed Total Assets",
    "WTREGEN":   "TGA",
    "RRPONTSYD": "RRP",
    "WRESBAL":   "Bank Reserves",
    "DFII10":    "10Y Real Yield",
    "SOFR":      "SOFR",
}


def get_output_dir(as_of: date) -> Path:
    p = _MACRO_DIR / as_of.isoformat()
    p.mkdir(parents=True, exist_ok=True)
    return p


def fetch_fred_series(series_map: dict[str, str], start_date: str, end_date: str,
                      api_key: str) -> pd.DataFrame:
    """Fetch each FRED series via direct API call; return wide DataFrame keyed by date."""
    frames: list[pd.DataFrame] = []

    with httpx.Client(timeout=20) as client:
        for series_id in series_map:
            try:
                resp = client.get(
                    _FRED_URL,
                    params={
                        "series_id": series_id,
                        "api_key": api_key,
                        "observation_start": start_date,
                        "observation_end": end_date,
                        "file_type": "json",
                    },
                )
                resp.raise_for_status()
                obs = resp.json().get("observations", [])
                rows = []
                for o in obs:
                    val_str = o.get("value", ".")
                    try:
                        val = float(val_str)
                    except (ValueError, TypeError):
                        val = float("nan")
                    rows.append({"date": o["date"], series_id: val})
                if rows:
                    df = pd.DataFrame(rows)
                    df["date"] = pd.to_datetime(df["date"])
                    frames.append(df.set_index("date")[[series_id]])
                    print(f"[liquidity] {series_id}: {len(rows)} observations")
                else:
                    print(f"[liquidity] {series_id}: no observations returned")
            except Exception as exc:
                print(f"[liquidity] {series_id}: fetch failed — {exc}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1).sort_index()
    combined.index.name = "date"
    return combined.reset_index()


def save_raw(df: pd.DataFrame, out_dir: Path) -> None:
    _MACRO_DIR.mkdir(parents=True, exist_ok=True)
    # dated copy
    dated_path = out_dir / "liquidity_raw.csv"
    df.to_csv(dated_path, index=False)
    # rolling latest
    rolling_path = _MACRO_DIR / "liquidity_raw.csv"
    df.to_csv(rolling_path, index=False)
    print(f"[liquidity] saved raw -> {dated_path}")


def main(as_of: date | None = None, force_refresh: bool = False) -> Path:
    load_env()

    as_of = as_of or date.today()
    out_dir = get_output_dir(as_of)
    dated_path = out_dir / "liquidity_raw.csv"

    if dated_path.exists() and not force_refresh:
        print(f"[liquidity] using cached raw: {dated_path}")
        return dated_path

    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        # Try fallback to rolling cache before raising
        rolling = _MACRO_DIR / "liquidity_raw.csv"
        if rolling.exists():
            print("[liquidity] FRED_API_KEY not set — using rolling cache")
            import shutil
            shutil.copy(rolling, dated_path)
            return dated_path
        raise RuntimeError("FRED_API_KEY not set and no cached data available")

    lookback = int(os.getenv("LIQUIDITY_LOOKBACK_DAYS", "90"))
    start_date = (as_of - timedelta(days=lookback)).isoformat()
    end_date = as_of.isoformat()

    print(f"[liquidity] fetching FRED data {start_date} -> {end_date}")
    try:
        df = fetch_fred_series(SERIES_MAP, start_date, end_date, api_key)
        if df.empty:
            raise ValueError("All FRED series returned empty")
        save_raw(df, out_dir)
        return dated_path
    except Exception as exc:
        print(f"[liquidity] fetch error: {exc}")
        # Fallback: copy rolling cache to dated dir if available
        rolling = _MACRO_DIR / "liquidity_raw.csv"
        if rolling.exists() and not dated_path.exists():
            import shutil
            shutil.copy(rolling, dated_path)
            print(f"[liquidity] fell back to rolling cache: {dated_path}")
        if dated_path.exists():
            return dated_path
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FRED liquidity data")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.date) if args.date else None
    p = main(as_of=as_of, force_refresh=args.force_refresh)
    print(f"Raw CSV: {p}")
    import pandas as pd
    print(pd.read_csv(p).tail(5).to_string(index=False))

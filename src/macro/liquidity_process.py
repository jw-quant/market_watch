"""
Process FRED raw liquidity CSV into summary table + premarket text artifact.

Inputs:
  data/macro/liquidity/YYYY-MM-DD/liquidity_raw.csv

Outputs:
  data/macro/liquidity/YYYY-MM-DD/liquidity_summary.csv
  data/reports/YYYY-MM-DD/premarket/txt/liquidity_flags_YYYYMMDD_HHMM.txt

Usage:
  python src/macro/liquidity_process.py
  python src/macro/liquidity_process.py --date 2026-05-19
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np
import pandas as pd

_MACRO_DIR = Path(__file__).resolve().parents[2] / "data" / "macro" / "liquidity"

METRICS = ["WALCL", "WTREGEN", "RRPONTSYD", "WRESBAL", "DFII10", "SOFR", "net_liquidity"]

METRIC_LABELS = {
    "WALCL":      "Fed Assets  / WALCL",
    "WTREGEN":    "TGA         / WTREGEN",
    "RRPONTSYD":  "RRP         / RRPONTSYD",
    "WRESBAL":    "Bank Res    / WRESBAL",
    "DFII10":     "10Y Real Yield / DFII10",
    "SOFR":       "SOFR",
    "net_liquidity": "Net Liquidity",
}


def _dated_dir(as_of: date) -> Path:
    return _MACRO_DIR / as_of.isoformat()


def load_raw(as_of: date) -> pd.DataFrame:
    path = _dated_dir(as_of) / "liquidity_raw.csv"
    if not path.exists():
        raise FileNotFoundError(f"[liquidity_process] raw CSV not found: {path}")
    df = pd.read_csv(path, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)
    # forward-fill sparse series (e.g. weekly FRED data)
    for col in df.columns:
        if col != "date":
            df[col] = df[col].ffill()
    return df


def compute_net_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    required = {"WALCL", "WTREGEN", "RRPONTSYD"}
    if required.issubset(df.columns):
        df["net_liquidity"] = df["WALCL"] - df["WTREGEN"] - df["RRPONTSYD"]
    else:
        missing = required - set(df.columns)
        print(f"[liquidity_process] cannot compute net_liquidity — missing: {missing}")
        df["net_liquidity"] = np.nan
    return df


def compute_metric_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Return summary DataFrame with latest_value and rolling changes per metric."""
    rows = []
    for metric in METRICS:
        if metric not in df.columns:
            rows.append({
                "metric": metric,
                "latest_value": np.nan,
                "change_1d": np.nan,
                "change_5d": np.nan,
                "change_20d": np.nan,
                "latest_date": None,
            })
            continue

        series = df[metric].dropna()
        if series.empty:
            rows.append({
                "metric": metric,
                "latest_value": np.nan,
                "change_1d": np.nan,
                "change_5d": np.nan,
                "change_20d": np.nan,
                "latest_date": None,
            })
            continue

        latest_val = float(series.iloc[-1])
        latest_date = str(df.loc[df[metric].notna(), "date"].iloc[-1].date())

        def _chg(n: int) -> Optional[float]:
            return float(series.iloc[-1] - series.iloc[-(n + 1)]) if len(series) >= n + 1 else None

        rows.append({
            "metric": metric,
            "latest_value": round(latest_val, 4),
            "change_1d": _chg(1),
            "change_5d": _chg(5),
            "change_20d": _chg(20),
            "latest_date": latest_date,
        })

    return pd.DataFrame(rows)


def save_summary(summary_df: pd.DataFrame, as_of: date) -> None:
    out_path = _dated_dir(as_of) / "liquidity_summary.csv"
    summary_df.to_csv(out_path, index=False)
    print(f"[liquidity_process] saved summary -> {out_path}")


def _fmt(val, decimals: int = 2) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "n/a"
    return f"{val:,.{decimals}f}"


def render_text_report(summary_df: pd.DataFrame, ts: str) -> str:
    lookup: dict[str, dict] = {
        row["metric"]: row for row in summary_df.to_dict("records")
    }

    def _row(metric: str) -> dict:
        return lookup.get(metric, {})

    def _line(metric: str, decimals: int = 2) -> str:
        r = _row(metric)
        label = METRIC_LABELS.get(metric, metric)
        lv = _fmt(r.get("latest_value"), decimals)
        c5 = _fmt(r.get("change_5d"), decimals)
        c20 = _fmt(r.get("change_20d"), decimals)
        ld = r.get("latest_date", "")
        return f"  {label:<30}: latest={lv}  5d={c5}  20d={c20}  (as of {ld})"

    nl = _row("net_liquidity")
    lines = [
        f"LIQUIDITY FLAGS -- {ts}",
        "",
        "NET LIQUIDITY:",
        f"  latest : {_fmt(nl.get('latest_value'), 2)}  (as of {nl.get('latest_date', '')})",
        f"  1d chg : {_fmt(nl.get('change_1d'), 2)}",
        f"  5d chg : {_fmt(nl.get('change_5d'), 2)}",
        f"  20d chg: {_fmt(nl.get('change_20d'), 2)}",
        "",
        "LIQUIDITY COMPONENTS:",
        _line("WALCL"),
        _line("WTREGEN"),
        _line("RRPONTSYD"),
        _line("WRESBAL"),
        "",
        "REAL RATES / FUNDING:",
        _line("DFII10", decimals=4),
        _line("SOFR",   decimals=4),
        "",
        "NOTES:",
        "- High-yield spread is in the existing equity/sector overview.",
        "- TLT/rate proxy is in the existing equity/sector overview.",
    ]
    return "\n".join(lines)


def main(as_of: date | None = None) -> Path:
    as_of = as_of or date.today()

    df = load_raw(as_of)
    df = compute_net_liquidity(df)
    summary_df = compute_metric_changes(df)
    save_summary(summary_df, as_of)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    content = render_text_report(summary_df, ts)

    from src.utility.paths import get_txt_dir  # noqa: PLC0415
    txt_path = get_txt_dir(run_date=as_of) / f"liquidity_flags_{ts}.txt"
    txt_path.write_text(content, encoding="utf-8")
    print(f"[liquidity_process] saved txt -> {txt_path}")

    return txt_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process FRED liquidity data")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.date) if args.date else None
    p = main(as_of=as_of)
    print(open(p).read())

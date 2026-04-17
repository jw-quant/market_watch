from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from src.common.env import load_env as _base_load_env


def load_env():
    """Load environment variables from .env if present."""
    _base_load_env()


def get_output_dir(as_of: date) -> Path:
    """Return dated output directory under data/reddit/YYYY-MM-DD."""
    out_dir = Path("data") / "reddit" / as_of.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        print(f"[warn] Invalid {name}={raw!r}; using default={default}")
        return default


def fetch_apewisdom(filter_name: str = "all") -> dict:
    """
    Fetch ApeWisdom payload for a filter.

    URL pattern: {APEWISDOM_BASE_URL}/filter/{filter}
    """
    base_url = _env_str("APEWISDOM_BASE_URL", "https://apewisdom.io/api/v1.0").rstrip("/")
    timeout = _env_int("APEWISDOM_TIMEOUT", 20)
    filt = (filter_name or "all").strip() or "all"

    url = f"{base_url}/filter/{filt}"
    print(f"[apewisdom] GET {url}")

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"ApeWisdom request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("ApeWisdom returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("ApeWisdom payload is not a JSON object")

    return payload


def normalize_results(payload: dict) -> pd.DataFrame:
    """
    Normalize top-100 results to a clean table.

    Output columns are exactly:
    rank, ticker, mentions, upvotes
    """
    results = payload.get("results")
    if not isinstance(results, list):
        print("[warn] payload.results missing or not a list")
        return pd.DataFrame(columns=["rank", "ticker", "mentions", "upvotes"])

    top100 = results[:100]
    records = []
    for row in top100:
        if not isinstance(row, dict):
            continue
        records.append(
            {
                "rank": row.get("rank"),
                "ticker": row.get("ticker"),
                "mentions": row.get("mentions"),
                "upvotes": row.get("upvotes"),
            }
        )

    df = pd.DataFrame(records, columns=["rank", "ticker", "mentions", "upvotes"])

    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"].notna()]
    df = df[df["ticker"] != ""]
    df = df[df["ticker"] != "NAN"]

    for col in ["rank", "mentions", "upvotes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("rank", na_position="last").reset_index(drop=True)
    return df[["rank", "ticker", "mentions", "upvotes"]]


def save_outputs(payload: dict, df: pd.DataFrame, out_dir: Path) -> None:
    """Save raw JSON and normalized CSV outputs."""
    raw_path = out_dir / "apewisdom_raw.json"
    csv_path = out_dir / "apewisdom_top100.csv"

    raw_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    df.to_csv(csv_path, index=False)

    print(f"[apewisdom] saved raw JSON: {raw_path}")
    print(f"[apewisdom] saved clean CSV: {csv_path}")


def main() -> None:
    load_env()

    filter_name = _env_str("APEWISDOM_FILTER", "all")
    as_of = date.today()
    out_dir = get_output_dir(as_of)

    payload = fetch_apewisdom(filter_name=filter_name)
    df = normalize_results(payload)
    save_outputs(payload, df, out_dir)

    print(f"[apewisdom] rows in CSV: {len(df)}")


if __name__ == "__main__":
    main()

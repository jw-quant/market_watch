from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dateutil import parser as dt_parser

from src.common.env import load_env as _base_load_env


def load_env() -> None:
    _base_load_env()


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value or default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        print(f"[warn] invalid {name}={raw!r}; using default={default}")
        return default


def get_output_dir(as_of: date) -> Path:
    out_dir = Path("data") / "news" / as_of.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _to_gdelt_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _extract_articles(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("articles"), list):
        return payload["articles"]
    if isinstance(payload.get("results"), list):
        return payload["results"]
    return []


def _is_cache_fresh(out_dir: Path, lookback_hours: int) -> bool:
    """Return True if today's raw JSON and CSV both exist and were written within the lookback window."""
    raw_path = out_dir / "gdelt_raw.json"
    csv_path = out_dir / "gdelt_articles.csv"
    if not raw_path.exists() or not csv_path.exists():
        return False
    age_seconds = time.time() - raw_path.stat().st_mtime
    return age_seconds < lookback_hours * 3600


def fetch_gdelt() -> dict[str, Any]:
    """Fetch recent articles from GDELT Doc API (last N hours)."""
    base_url = _env_str("GDELT_BASE_URL", "https://api.gdeltproject.org/api/v2/doc/doc")
    timeout = _env_int("GDELT_TIMEOUT", 20)
    lookback_hours = _env_int("GDELT_LOOKBACK_HOURS", 1)
    limit = _env_int("GDELT_LIMIT", 30)

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=lookback_hours)

    params = {
        "query": "*",
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(limit),
        "sort": "DateDesc",
        "startdatetime": _to_gdelt_ts(start_utc),
        "enddatetime": _to_gdelt_ts(now_utc),
    }

    print(
        "[gdelt] GET "
        f"{base_url} lookback_hours={lookback_hours} limit={limit} "
        f"start={params['startdatetime']} end={params['enddatetime']}"
    )

    try:
        response = requests.get(base_url, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"GDELT request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("GDELT returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("GDELT payload is not a JSON object")

    articles = _extract_articles(payload)
    print(f"[gdelt] received article rows={len(articles)}")
    return payload


def _safe_parse_datetime(value: Any) -> str | None:
    if value is None:
        return None
    try:
        dt = dt_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def normalize_results(payload: dict[str, Any]) -> pd.DataFrame:
    """Flatten GDELT article rows to [headline, url, source, published_at]."""
    limit = _env_int("GDELT_LIMIT", 30)
    rows = _extract_articles(payload)[:limit]
    norm_rows: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        headline = row.get("title") or row.get("headline") or ""
        url = row.get("url") or row.get("link") or ""
        source = row.get("domain") or row.get("source") or ""
        published_at_raw = row.get("seendate") or row.get("date") or row.get("published_at")

        norm_rows.append(
            {
                "headline": str(headline).strip(),
                "url": str(url).strip(),
                "source": str(source).strip(),
                "published_at": _safe_parse_datetime(published_at_raw),
            }
        )

    df = pd.DataFrame(norm_rows, columns=["headline", "url", "source", "published_at"])
    if df.empty:
        return df

    df = df[(df["headline"] != "") & (df["url"] != "")]
    df = df.drop_duplicates(subset=["url"])
    df = df.sort_values("published_at", ascending=False, na_position="last").reset_index(drop=True)
    return df


def save_outputs(payload: dict[str, Any], df: pd.DataFrame, out_dir: Path) -> None:
    raw_path = out_dir / "gdelt_raw.json"
    csv_path = out_dir / "gdelt_articles.csv"

    raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    df.to_csv(csv_path, index=False)

    print(f"[gdelt] saved raw JSON: {raw_path}")
    print(f"[gdelt] saved normalized CSV: {csv_path}")


def main(force: bool = False) -> None:
    load_env()
    as_of = date.today()
    out_dir = get_output_dir(as_of)
    lookback_hours = _env_int("GDELT_LOOKBACK_HOURS", 1)

    if not force and _is_cache_fresh(out_dir, lookback_hours):
        print("[gdelt] cache is fresh, skipping fetch")
        return

    try:
        payload = fetch_gdelt()
        df = normalize_results(payload)
        save_outputs(payload, df, out_dir)
        print(f"[gdelt] normalized rows={len(df)}")
    except RuntimeError as exc:
        csv_path = out_dir / "gdelt_articles.csv"
        if csv_path.exists():
            print(f"[gdelt] fetch failed ({exc}); falling back to cached files")
        else:
            raise


if __name__ == "__main__":
    main()

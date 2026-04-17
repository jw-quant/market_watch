from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd
from dateutil import parser as dt_parser

from src.common.env import load_env as _base_load_env


def load_env() -> None:
    _base_load_env()


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


def _clean_headline(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        dt = dt_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _headline_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def cluster_headlines(df: pd.DataFrame, sim_threshold: float = 0.90) -> list[dict[str, Any]]:
    """
    Greedy clustering by cleaned headline.
    1) exact cleaned headline match first
    2) fuzzy merge with SequenceMatcher threshold
    """
    clusters: list[dict[str, Any]] = []

    for row in df.itertuples(index=False):
        cleaned = row.headline_clean
        if not cleaned:
            continue

        assigned = False

        # Fast path: exact cleaned headline
        for c in clusters:
            if cleaned == c["key_clean"]:
                c["rows"].append(row)
                assigned = True
                break

        if assigned:
            continue

        # Fuzzy path
        for c in clusters:
            if _headline_sim(cleaned, c["key_clean"]) >= sim_threshold:
                c["rows"].append(row)
                assigned = True
                break

        if not assigned:
            clusters.append({"key_clean": cleaned, "rows": [row]})

    return clusters


def build_cluster_table(clusters: list[dict[str, Any]]) -> pd.DataFrame:
    out_rows: list[dict[str, Any]] = []

    for c in clusters:
        rows = c["rows"]
        if not rows:
            continue

        latest = max((r.published_dt for r in rows if r.published_dt is not None), default=None)
        if latest is None:
            continue

        # Representative: latest headline in cluster
        latest_row = max(rows, key=lambda r: r.published_dt or datetime.min.replace(tzinfo=timezone.utc))

        source_values = {str(r.source).strip().lower() for r in rows if str(r.source).strip()}

        out_rows.append(
            {
                "cluster_headline": latest_row.headline,
                "latest_time": latest.isoformat(),
                "article_count": len(rows),
                "source_count": len(source_values),
                "sample_url": latest_row.url,
            }
        )

    df = pd.DataFrame(
        out_rows,
        columns=["cluster_headline", "latest_time", "article_count", "source_count", "sample_url"],
    )
    if df.empty:
        return df

    df["latest_dt"] = pd.to_datetime(df["latest_time"], errors="coerce", utc=True)
    df = df.dropna(subset=["latest_dt"]).reset_index(drop=True)
    return df


def select_top(cluster_df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    breaking_window_minutes = _env_int("GDELT_BREAKING_WINDOW_MINUTES", 60)
    lookback_hours = _env_int("GDELT_LOOKBACK_HOURS", 1)

    now_utc = datetime.now(timezone.utc)
    breaking_cutoff = now_utc - timedelta(minutes=breaking_window_minutes)
    lookback_cutoff = now_utc - timedelta(hours=lookback_hours)

    recent_df = cluster_df[cluster_df["latest_dt"] >= pd.Timestamp(lookback_cutoff)].copy()

    bucket1 = recent_df[recent_df["latest_dt"] >= pd.Timestamp(breaking_cutoff)].copy()
    bucket2 = recent_df[recent_df["latest_dt"] < pd.Timestamp(breaking_cutoff)].copy()

    bucket1 = bucket1.sort_values("latest_dt", ascending=False)
    bucket2 = bucket2.sort_values(
        ["source_count", "article_count", "latest_dt"],
        ascending=[False, False, False],
    )

    if len(bucket1) >= top_n:
        top = bucket1.head(top_n)
    else:
        need = top_n - len(bucket1)
        top = pd.concat([bucket1, bucket2.head(need)], ignore_index=True)

    if top.empty:
        return top

    return top[
        ["cluster_headline", "latest_time", "article_count", "source_count", "sample_url"]
    ].reset_index(drop=True)


def save_outputs(top20: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "gdelt_top20.csv"
    json_path = out_dir / "gdelt_top20.json"

    top20.to_csv(csv_path, index=False)
    records = top20.to_dict(orient="records")
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[gdelt-process] saved top20 CSV: {csv_path}")
    print(f"[gdelt-process] saved top20 JSON: {json_path}")


def main() -> None:
    load_env()
    as_of = date.today()
    out_dir = get_output_dir(as_of)

    in_csv = out_dir / "gdelt_articles.csv"
    if not in_csv.exists():
        raise FileNotFoundError(f"Missing input file: {in_csv}")

    df = pd.read_csv(in_csv)
    if df.empty:
        print("[gdelt-process] input CSV is empty")
        empty = pd.DataFrame(columns=["cluster_headline", "latest_time", "article_count", "source_count", "sample_url"])
        save_outputs(empty, out_dir)
        return

    # Normalize required fields
    for col in ["headline", "url", "source", "published_at"]:
        if col not in df.columns:
            df[col] = ""

    df["headline"] = df["headline"].astype(str).str.strip()
    df["url"] = df["url"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()
    df["published_dt"] = df["published_at"].apply(_parse_dt)
    df["headline_clean"] = df["headline"].apply(_clean_headline)

    df = df[(df["headline"] != "") & (df["url"] != "")]
    df = df[df["published_dt"].notna()].reset_index(drop=True)

    clusters = cluster_headlines(df)
    cluster_df = build_cluster_table(clusters)

    if cluster_df.empty:
        print("[gdelt-process] no valid clusters found")
        empty = pd.DataFrame(columns=["cluster_headline", "latest_time", "article_count", "source_count", "sample_url"])
        save_outputs(empty, out_dir)
        return

    top20 = select_top(cluster_df)
    save_outputs(top20, out_dir)
    print(f"[gdelt-process] top rows={len(top20)}")


if __name__ == "__main__":
    main()

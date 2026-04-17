from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.common.env import load_env as _base_load_env

HOT_SIZE = 40
HOT_STALE_DAYS = 30
CANDIDATE_STALE_DAYS = 60

TICKERS_CONFIG_PATH = Path("data") / "config" / "tickers.json"
STATE_PATH = Path("data") / "reddit" / "ticker_state.csv"
EXCLUSION_PATH = Path("data") / "config" / "dynamic_exclusions.txt"

DEFAULT_EXCLUSIONS = {
    "SPY", "QQQ", "GLD", "USO", "TLT", "LQD", "HYG", "UUP", "IBIT", "SOXX", "XLF", "XLE", "VXX",
    "BTC", "ETH", "XAU", "XAG", "WTI", "BRENT", "DXY",
}

STATE_COLUMNS = [
    "ticker",
    "status",
    "first_seen",
    "last_seen",
    "last_hot_date",
    "days_since_seen",
    "days_since_hot",
    "current_rank",
    "best_rank",
    "times_seen_top100",
    "times_seen_hot",
]


def load_env() -> None:
    _base_load_env()


def get_reddit_day_dir(as_of: date) -> Path:
    p = Path("data") / "reddit" / as_of.isoformat()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _parse_date(v) -> pd.Timestamp | pd.NaT:
    if v is None or v == "":
        return pd.NaT
    return pd.to_datetime(v, errors="coerce")


def _to_upper_list(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in values:
        s = str(x).strip().upper()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _safe_int(v, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def load_today_apewisdom(as_of: date) -> pd.DataFrame:
    p = get_reddit_day_dir(as_of) / "apewisdom_top100.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing ApeWisdom processed file: {p}")

    df = pd.read_csv(p)
    for c in ["rank", "ticker", "mentions", "upvotes"]:
        if c not in df.columns:
            raise ValueError(f"ApeWisdom CSV missing required column: {c}")

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"] != ""]
    df = df[df["ticker"] != "NAN"]
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df = df.sort_values("rank", na_position="last").drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    return df


def load_tickers_config() -> dict:
    if not TICKERS_CONFIG_PATH.exists():
        return {}
    with TICKERS_CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not isinstance(cfg, dict):
        raise ValueError("tickers.json must be a JSON object")
    return cfg


def load_exclusions(config: dict) -> set[str]:
    exclusions = set(DEFAULT_EXCLUSIONS)

    if EXCLUSION_PATH.exists():
        for line in EXCLUSION_PATH.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            exclusions.add(raw.upper())

    for key in ["benchmark", "sector", "core", "static", "blocked"]:
        vals = config.get(key, [])
        if isinstance(vals, list):
            exclusions.update(_to_upper_list(vals))

    return exclusions


def load_state() -> pd.DataFrame:
    if not STATE_PATH.exists():
        return pd.DataFrame(columns=STATE_COLUMNS)

    df = pd.read_csv(STATE_PATH)
    for col in STATE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"] != ""]
    df = df.drop_duplicates(subset=["ticker"], keep="last").reset_index(drop=True)

    for dcol in ["first_seen", "last_seen", "last_hot_date"]:
        df[dcol] = df[dcol].apply(_parse_date)

    for ncol in ["current_rank", "best_rank", "times_seen_top100", "times_seen_hot", "days_since_seen", "days_since_hot"]:
        df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

    df["status"] = df["status"].astype(str).str.lower().replace({"nan": "inactive"})
    return df[STATE_COLUMNS].copy()


def _initial_row(ticker: str, today_ts: pd.Timestamp) -> dict:
    return {
        "ticker": ticker,
        "status": "inactive",
        "first_seen": today_ts,
        "last_seen": pd.NaT,
        "last_hot_date": pd.NaT,
        "days_since_seen": pd.NA,
        "days_since_hot": pd.NA,
        "current_rank": pd.NA,
        "best_rank": pd.NA,
        "times_seen_top100": 0,
        "times_seen_hot": 0,
    }


def classify_today(df_top100: pd.DataFrame, exclusions: set[str]) -> tuple[list[str], list[str], dict[str, float]]:
    eligible = df_top100[~df_top100["ticker"].isin(exclusions)].copy()
    eligible = eligible.sort_values("rank", na_position="last").reset_index(drop=True)

    today_hot = _to_upper_list(eligible["ticker"].head(HOT_SIZE).tolist())
    today_candidates = _to_upper_list(eligible["ticker"].iloc[HOT_SIZE:].tolist())

    rank_map: dict[str, float] = {}
    for r in eligible[["ticker", "rank"]].itertuples(index=False):
        if pd.notna(r.rank):
            rank_map[str(r.ticker).upper()] = float(r.rank)

    return today_hot, today_candidates, rank_map


def update_state(
    state_df: pd.DataFrame,
    today_hot: list[str],
    today_candidates: list[str],
    rank_map: dict[str, float],
    as_of: date,
) -> pd.DataFrame:
    today_ts = pd.Timestamp(as_of)

    if state_df.empty:
        state_df = pd.DataFrame(columns=STATE_COLUMNS)

    for t in set(today_hot + today_candidates):
        if t not in set(state_df["ticker"].tolist()):
            state_df = pd.concat([state_df, pd.DataFrame([_initial_row(t, today_ts)])], ignore_index=True)

    state_df = state_df.drop_duplicates(subset=["ticker"], keep="last").reset_index(drop=True)

    for i, row in state_df.iterrows():
        t = row["ticker"]
        in_hot = t in today_hot
        in_candidates = t in today_candidates
        seen_today = in_hot or in_candidates

        if seen_today and pd.isna(row["first_seen"]):
            state_df.at[i, "first_seen"] = today_ts

        if in_hot:
            state_df.at[i, "status"] = "hot"
            state_df.at[i, "last_seen"] = today_ts
            state_df.at[i, "last_hot_date"] = today_ts
            state_df.at[i, "times_seen_top100"] = _safe_int(row["times_seen_top100"]) + 1
            state_df.at[i, "times_seen_hot"] = _safe_int(row["times_seen_hot"]) + 1
            state_df.at[i, "current_rank"] = rank_map.get(t, pd.NA)
            prev_best = row["best_rank"]
            curr = rank_map.get(t)
            if curr is not None and (pd.isna(prev_best) or curr < float(prev_best)):
                state_df.at[i, "best_rank"] = curr

        elif in_candidates:
            state_df.at[i, "status"] = "candidate"
            state_df.at[i, "last_seen"] = today_ts
            state_df.at[i, "times_seen_top100"] = _safe_int(row["times_seen_top100"]) + 1
            state_df.at[i, "current_rank"] = rank_map.get(t, pd.NA)
            prev_best = row["best_rank"]
            curr = rank_map.get(t)
            if curr is not None and (pd.isna(prev_best) or curr < float(prev_best)):
                state_df.at[i, "best_rank"] = curr

        else:
            state_df.at[i, "current_rank"] = pd.NA

    # Recompute recency columns and apply aging transitions
    state_df["days_since_seen"] = (today_ts - pd.to_datetime(state_df["last_seen"], errors="coerce")).dt.days
    state_df["days_since_hot"] = (today_ts - pd.to_datetime(state_df["last_hot_date"], errors="coerce")).dt.days

    for i, row in state_df.iterrows():
        d_seen = row["days_since_seen"]
        d_hot = row["days_since_hot"]
        status = str(row["status"] or "inactive").lower()

        if pd.notna(d_seen) and float(d_seen) >= CANDIDATE_STALE_DAYS:
            state_df.at[i, "status"] = "inactive"
            continue

        if status == "hot" and pd.notna(d_hot) and float(d_hot) >= HOT_STALE_DAYS:
            if pd.notna(d_seen) and float(d_seen) < CANDIDATE_STALE_DAYS:
                state_df.at[i, "status"] = "candidate"
            else:
                state_df.at[i, "status"] = "inactive"

    state_df = state_df[STATE_COLUMNS].copy()
    state_df = state_df.sort_values(["status", "days_since_hot", "days_since_seen", "ticker"], na_position="last")
    return state_df.reset_index(drop=True)


def build_active_lists(state_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    active_hot = state_df[state_df["status"] == "hot"].copy()
    active_cand = state_df[state_df["status"] == "candidate"].copy()

    active_hot = active_hot.sort_values(["current_rank", "days_since_hot", "ticker"], na_position="last")
    active_cand = active_cand.sort_values(["current_rank", "days_since_seen", "ticker"], na_position="last")

    hot = _to_upper_list(active_hot["ticker"].tolist())
    candidates = [t for t in _to_upper_list(active_cand["ticker"].tolist()) if t not in set(hot)]
    return hot, candidates


def save_state(state_df: pd.DataFrame) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = state_df.copy()
    for dcol in ["first_seen", "last_seen", "last_hot_date"]:
        out[dcol] = pd.to_datetime(out[dcol], errors="coerce").dt.date.astype("string")
    out.to_csv(STATE_PATH, index=False)
    print(f"[wisdomprocess] saved rolling state: {STATE_PATH}")


def save_tickers_json(config: dict, hot: list[str], candidates: list[str]) -> None:
    benchmark = _to_upper_list(config.get("benchmark", ["SPY"]))
    sector = _to_upper_list(config.get("sector", []))
    core = _to_upper_list(config.get("core", config.get("static", [])))
    blocked = _to_upper_list(config.get("blocked", []))

    out = {
        "benchmark": benchmark,
        "sector": sector,
        "core": core,
        "hot": hot,
        "candidates": candidates,
        "blocked": blocked,
    }

    TICKERS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TICKERS_CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"[wisdomprocess] updated ticker config: {TICKERS_CONFIG_PATH}")


def main(as_of: date | None = None) -> dict:
    load_env()
    as_of = as_of or date.today()

    cfg = load_tickers_config()
    df_top = load_today_apewisdom(as_of)
    exclusions = load_exclusions(cfg)
    state_df = load_state()

    today_hot, today_candidates, rank_map = classify_today(df_top, exclusions)
    updated = update_state(state_df, today_hot, today_candidates, rank_map, as_of=as_of)
    hot, candidates = build_active_lists(updated)

    save_state(updated)
    save_tickers_json(cfg, hot, candidates)

    summary = {
        "date": as_of.isoformat(),
        "eligible_hot_today": len(today_hot),
        "eligible_candidates_today": len(today_candidates),
        "active_hot": len(hot),
        "active_candidates": len(candidates),
    }

    print(
        "[wisdomprocess] "
        f"today_hot={summary['eligible_hot_today']} "
        f"today_candidates={summary['eligible_candidates_today']} "
        f"active_hot={summary['active_hot']} "
        f"active_candidates={summary['active_candidates']}"
    )
    return summary


if __name__ == "__main__":
    main()

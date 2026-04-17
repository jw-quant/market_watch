SECTOR_TICKER_NAME_MAP = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq",
    "GLD": "Gold",
    "USO": "Oil",
    "TLT": "Long Treasuries",
    "LQD": "Investment Grade Credit",
    "HYG": "High Yield Credit",
    "UUP": "US Dollar",
    "IBIT": "Bitcoin",
    "SOXX": "Chips",
    "XLF": "Financials",
    "XLE": "Energy",
    "VXX": "VIX"
}

# cfg.py content moved here
import json
from pathlib import Path
import os

ROOT_DIR = Path(__file__).resolve().parents[2]  # repo root
DATA_DIR = str(ROOT_DIR / "data")

TICKERS_CONFIG_PATH = ROOT_DIR / "data" / "config" / "tickers.json"


def _load_tickers_config() -> dict:
    if not TICKERS_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Ticker config not found: {TICKERS_CONFIG_PATH}")
    with TICKERS_CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = json.load(f)
    if not isinstance(config, dict):
        raise ValueError(f"Ticker config must be a JSON object: {TICKERS_CONFIG_PATH}")
    return config


TICKERS_CONFIG = _load_tickers_config()

BENCHMARK = TICKERS_CONFIG.get("benchmark", ["SPY"])
SECTOR_TICKERS = TICKERS_CONFIG.get("sector", [])
CORE_TICKERS = TICKERS_CONFIG.get("core", TICKERS_CONFIG.get("static", []))
HOT_TICKERS = TICKERS_CONFIG.get("hot", [])
CANDIDATE_TICKERS = TICKERS_CONFIG.get("candidates", [])

# Final universe (loaded at import time; use get_smf_tickers() for a fresh read after tickers.json updates)
SMF_TICKERS = list(dict.fromkeys(BENCHMARK + SECTOR_TICKERS + CORE_TICKERS + HOT_TICKERS + CANDIDATE_TICKERS))


def get_smf_tickers() -> list:
    """Re-read tickers.json from disk and return the current full universe."""
    config = _load_tickers_config()
    benchmark = config.get("benchmark", ["SPY"])
    sector = config.get("sector", [])
    core = config.get("core", config.get("static", []))
    hot = config.get("hot", [])
    candidates = config.get("candidates", [])
    return list(dict.fromkeys(benchmark + sector + core ))

def get_sectors() -> list:
    """Re-read tickers.json from disk and return the current full universe."""
    config = _load_tickers_config()
    sector = config.get("sector", [])
    return list(dict.fromkeys(sector))


# How far back to pull (in calendar days). Using 370 to comfortably cover ~252 trading days.
LOOKBACK_DAYS = 370

# Where Excel files go
# ---- Data directories (single source of truth) ----
PRICES_DIR  = f"{DATA_DIR}/prices"
OPTIONS_DIR = f"{DATA_DIR}/options"
NEWS_DIR    = f"{DATA_DIR}/news"
REDDIT_DIR  = f"{DATA_DIR}/reddit"
REPORTS_DIR = f"{DATA_DIR}/reports"
GRAPHS_DIR  = f"{REPORTS_DIR}/graphs"

# Free-tier is ~5 req/min. One request per ticker => ~12s spacing.
RATE_LIMIT_SECS = 12

# Polygon API key
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

# Email / SMTP settings (used by the daily batch reporter)
# Prefer setting sensitive values via environment variables.

SMTP_HOST = os.getenv("SMF_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMF_SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMF_SMTP_USER", "")
SMTP_PASS = os.getenv("SMF_SMTP_PASS", "")
EMAIL_FROM = os.getenv("SMF_EMAIL_FROM", SMTP_USER)
EMAIL_TO = [s.strip() for s in os.getenv("SMF_EMAIL_TO", "tedkou@gmail.com,jw6542@nyu.edu,lemonmilklalala@gmail.com").split(",") if s.strip()]
EMAIL_SUBJECT = os.getenv("SMF_EMAIL_SUBJECT", "SMF Daily Report")
from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import Optional

import httpx

from src.common.env import getenv_required

_TOKEN_URL  = "https://api.schwabapi.com/v1/oauth/token"
_QUOTES_URL = "https://api.schwabapi.com/marketdata/v1/quotes"
_EXPIRY_BUFFER_SECS = 60

_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _write_env_key(key: str, value: str) -> None:
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        env_path.write_text(f"{key}={value}\n", encoding="utf-8")
        return
    lines = env_path.read_text(encoding="utf-8").splitlines(keepends=True)
    found = False
    new_lines = []
    for line in lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}\n")
    env_path.write_text("".join(new_lines), encoding="utf-8")


def get_access_token() -> str:
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - _EXPIRY_BUFFER_SECS:
        return _token_cache["access_token"]

    client_id     = getenv_required("SCHWAB_CLIENT_ID")
    client_secret = getenv_required("SCHWAB_CLIENT_SECRET")
    refresh_token = getenv_required("SCHWAB_REFRESH_TOKEN")

    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    with httpx.Client(timeout=15) as client:
        resp = client.post(
            _TOKEN_URL,
            headers={
                "Authorization": f"Basic {creds}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        )
        resp.raise_for_status()

    data = resp.json()
    access_token  = data["access_token"]
    new_refresh   = data.get("refresh_token", "")
    expires_in    = int(data.get("expires_in", 1800))

    if new_refresh:
        os.environ["SCHWAB_REFRESH_TOKEN"] = new_refresh
        _write_env_key("SCHWAB_REFRESH_TOKEN", new_refresh)

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"]   = time.time() + expires_in

    return access_token


def fetch_premarket_price(symbol: str) -> Optional[float]:
    """
    Return the latest extended-hours (premarket) price for symbol, or None on any failure.
    Prefers extended.lastPrice; falls back to quote.lastPrice.
    """
    try:
        token = get_access_token()
    except Exception as exc:
        print(f"[schwab] token fetch failed ({symbol}): {exc}")
        return None

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                _QUOTES_URL,
                headers={"Authorization": f"Bearer {token}"},
                params={"symbols": symbol.upper(), "fields": "quote,extended"},
            )
            resp.raise_for_status()
            data = resp.json()

        td = data.get(symbol.upper(), {})

        # Prefer premarket/extended price
        try:
            ep = td.get("extended", {}).get("lastPrice")
            if ep is not None and float(ep) > 0:
                return float(ep)
        except (TypeError, ValueError):
            pass

        # Fall back to regular session last price
        try:
            rp = td.get("quote", {}).get("lastPrice")
            if rp is not None and float(rp) > 0:
                return float(rp)
        except (TypeError, ValueError):
            pass

        return None

    except Exception as exc:
        print(f"[schwab] price fetch failed ({symbol}): {exc}")
        return None

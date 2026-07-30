from __future__ import annotations

import base64
import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

from src.util.env import getenv_required

_TOKEN_URL  = "https://api.schwabapi.com/v1/oauth/token"
_QUOTES_URL = "https://api.schwabapi.com/marketdata/v1/quotes"
_TRADER_BASE = "https://api.schwabapi.com/trader/v1"
_EXPIRY_BUFFER_SECS = 60

_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _get_schwab_dir(data_dir: Optional[Path] = None) -> Path:
    root = data_dir or Path(os.getenv("DATA_ROOT", str(Path.cwd())))
    d = Path(root) / "data" / "schwab"
    d.mkdir(parents=True, exist_ok=True)
    return d


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
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            _token_cache["expires_at"] = time.time() + 300
            raise RuntimeError(
                f"Schwab token refresh failed ({exc.response.status_code})"
            ) from exc

    data = resp.json()
    access_token = data["access_token"]
    new_refresh   = data.get("refresh_token", "")
    expires_in    = int(data.get("expires_in", 1800))

    if new_refresh:
        os.environ["SCHWAB_REFRESH_TOKEN"] = new_refresh
        _write_env_key("SCHWAB_REFRESH_TOKEN", new_refresh)

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"]   = time.time() + expires_in

    return access_token


def fetch_premarket_price(symbol: str) -> Optional[float]:
    """Return latest extended-hours (premarket) price for symbol, or None on failure."""
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

        try:
            ep = td.get("extended", {}).get("lastPrice")
            if ep is not None and float(ep) > 0:
                return float(ep)
        except (TypeError, ValueError):
            pass

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


def fetch_accounts() -> list[dict]:
    """GET /trader/v1/accounts — all linked accounts with accountNumber and hashValue."""
    try:
        token = get_access_token()
    except RuntimeError as exc:
        raise RuntimeError(f"[schwab] fetch_accounts: {exc}") from exc
    with httpx.Client(timeout=15) as client:
        resp = client.get(
            f"{_TRADER_BASE}/accounts",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
    return resp.json()


def fetch_transactions(account_hash: str, start_date: str, end_date: str) -> list[dict]:
    """GET /trader/v1/accounts/{accountHash}/transactions filtered to TRADE type."""
    try:
        token = get_access_token()
    except RuntimeError as exc:
        raise RuntimeError(f"[schwab] fetch_transactions: {exc}") from exc
    with httpx.Client(timeout=15) as client:
        resp = client.get(
            f"{_TRADER_BASE}/accounts/{account_hash}/transactions",
            headers={"Authorization": f"Bearer {token}"},
            params={
                "startDate": f"{start_date}T00:00:00Z",
                "endDate":   f"{end_date}T23:59:59Z",
                "types":     "TRADE",
            },
        )
        resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def get_cached_accounts(data_dir: Optional[Path] = None) -> list[dict]:
    """Fetch accounts from API; return today's cached result if already fetched today."""
    cache_file = _get_schwab_dir(data_dir) / f"accounts_{date.today().isoformat()}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))
    data = fetch_accounts()
    cache_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def get_cached_transactions(
    account_hash: str,
    start_date: str,
    end_date: str,
    data_dir: Optional[Path] = None,
) -> list[dict]:
    """Fetch transactions; return cached result if file already exists for this range."""
    fname = f"transactions_{account_hash[:8]}_{start_date}_{end_date}.json"
    cache_file = _get_schwab_dir(data_dir) / fname
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))
    data = fetch_transactions(account_hash, start_date, end_date)
    cache_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data

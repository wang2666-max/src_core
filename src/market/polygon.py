# src_core/src/market/polygon.py
# Minimal client for Polygon that supports:
# - Initial backfill to CSV (from hard start 2024-01-01) when a ticker has no CSV yet
# - Incremental updates for existing tickers based on each ticker's latest CSV date
# - Dividend-aware total return adjustment (all tickers, all runs)

import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from typing import Optional, Iterable

import httpx
import pandas as pd
import numpy as np

from src.util.env import getenv_required

BASE = "https://api.polygon.io"
DEFAULT_INIT_START = "2024-01-01"  # ISO format
DEFAULT_RATE_LIMIT_SECS = 12
DEFAULT_LOOKBACK_DAYS = 370


class PolygonClient:
    """
    Configurable Polygon data client.
    Initialize with a data directory path and optional rate limit / lookback settings.
    """

    def __init__(
        self,
        data_dir: Path,
        rate_limit_secs: int = DEFAULT_RATE_LIMIT_SECS,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit_secs = rate_limit_secs
        self.lookback_days = lookback_days
        self._last_call = 0.0

    def _rate_limit(self):
        """Simple rate limiter."""
        elapsed = time.time() - self._last_call
        if elapsed < self.rate_limit_secs:
            time.sleep(self.rate_limit_secs - elapsed)
        self._last_call = time.time()

    def _get(self, path: str, params: dict) -> dict:
        """Make authenticated request to Polygon API."""
        p = dict(params or {})
        api_key = getenv_required("POLYGON_API_KEY").strip().strip('"').strip("'")
        p["apiKey"] = api_key
        self._rate_limit()
        with httpx.Client(timeout=30) as client:
            r = client.get(f"{BASE}{path}", params=p)
            if r.status_code == 429:
                time.sleep(15)
                r = client.get(f"{BASE}{path}", params=p)
            r.raise_for_status()
            return r.json()

    def _csv_path(self, ticker: str) -> Path:
        """Return CSV path for a ticker."""
        return self.data_dir / f"{ticker.upper()}.csv"

    @staticmethod
    def _rows_to_df(rows: list[dict], ticker: str) -> pd.DataFrame:
        """Convert Polygon API response rows to DataFrame."""
        if not rows:
            return pd.DataFrame(
                columns=["ticker", "date", "open", "high", "low", "close"]
            )

        def one(r):
            d = datetime.fromtimestamp(
                r["t"] / 1000, tz=timezone.utc
            ).date().isoformat()
            return {
                "ticker": ticker.upper(),
                "date": d,
                "open": float(r["o"]),
                "high": float(r["h"]),
                "low": float(r["l"]),
                "close": float(r["c"]),
            }

        return pd.DataFrame([one(r) for r in rows])

    def fetch_range_dividends(
        self, ticker: str, start: str, end: str
    ) -> pd.DataFrame:
        """
        Fetch dividend history from Polygon /v3/reference/dividends endpoint.
        Returns DataFrame with [date, dividend] columns.
        """
        rows = []
        path = "/v3/reference/dividends"
        params = {
            "ticker": ticker.upper(),
            "limit": 1000,
            "order": "asc",
            "sort": "ex_dividend_date",
        }

        while True:
            try:
                j = self._get(path, params)
            except Exception as e:
                print(
                    f"[warn] {ticker}: dividend fetch failed, "
                    f"continuing with 0 dividends: {e}"
                )
                break

            results = j.get("results") or []

            for r in results:
                ex_date = r.get("ex_dividend_date")
                cash = r.get("cash_amount")
                if ex_date is None or cash is None:
                    continue
                rows.append(
                    {
                        "date": str(ex_date),
                        "dividend": float(cash),
                    }
                )

            next_url = j.get("next_url")
            if not next_url:
                break

            # Parse next_url into path and params
            if next_url.startswith(BASE):
                path = next_url[len(BASE) :]
            else:
                path = next_url

            params = {}

        if not rows:
            return pd.DataFrame(columns=["date", "dividend"])

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
        df = df.dropna(subset=["date"])
        df = df.groupby("date", as_index=False)["dividend"].sum()

        # Filter to date range
        df = df[
            (df["date"] >= str(start)) & (df["date"] <= str(end))
        ].reset_index(drop=True)
        return df

    @staticmethod
    def apply_total_return_adjustment(
        price_df: pd.DataFrame,
        dividend_df: Optional[pd.DataFrame] = None,
        prior_close: Optional[float] = None,
        prior_adj_close: Optional[float] = None,
    ) -> pd.DataFrame:
        """
        Enrich price_df with dividend and total-return-adjusted OHLC columns.
        All numeric values rounded to 4 decimals (0.0001 precision).

        By default the adjustment chain is anchored at price_df's own first
        row (adj_factor = 1.0 there). Pass prior_close/prior_adj_close to
        continue an existing chain instead — e.g. when appending new rows to
        an already-adjusted history, so the whole history doesn't need to be
        recomputed just to add a few new rows at the end.
        """
        df = price_df.copy()

        if df.empty:
            for col in [
                "dividend",
                "adj_factor_total_return",
                "adj_open",
                "adj_high",
                "adj_low",
                "adj_close",
            ]:
                if col not in df.columns:
                    df[col] = pd.Series(dtype=float)
            return df

        # Normalize date format
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
        df = df.sort_values("date").reset_index(drop=True)

        # Ensure numeric types and round OHLC to 4 decimals upfront
        numeric_cols = ["open", "high", "low", "close"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

        # Prepare dividend DataFrame
        if dividend_df is None or dividend_df.empty:
            div = pd.DataFrame(columns=["date", "dividend"])
        else:
            div = dividend_df.copy()
            div["date"] = (
                pd.to_datetime(div["date"], errors="coerce").dt.date.astype(str)
            )
            div = div.dropna(subset=["date"])
            div["dividend"] = (
                pd.to_numeric(div["dividend"], errors="coerce")
                .fillna(0.0)
                .round(4)
            )
            div = div.groupby("date", as_index=False)["dividend"].sum()

        # Merge dividends by date
        df = df.merge(div, on="date", how="left")
        df["dividend"] = df["dividend"].fillna(0.0).astype(float).round(4)

        # Compute forward cumulative total-return series
        adj_close_vals = []
        adj_factor_vals = []

        first_close = (
            float(df.loc[0, "close"])
            if pd.notna(df.loc[0, "close"])
            else float("nan")
        )

        if prior_close is not None and prior_adj_close is not None:
            # Continue an existing chain rather than restarting at 1.0 here.
            first_div = float(df.loc[0, "dividend"])
            if prior_close > 0 and pd.notna(first_close):
                gross_ret = (first_close + first_div) / prior_close
                first_adj_close = prior_adj_close * gross_ret
            else:
                first_adj_close = first_close
            adj_close_vals.append(first_adj_close)
            adj_factor_vals.append(
                first_adj_close / first_close
                if pd.notna(first_close) and first_close != 0
                else float("nan")
            )
        else:
            adj_close_vals.append(first_close)
            adj_factor_vals.append(
                1.0
                if pd.notna(first_close) and first_close != 0
                else float("nan")
            )

        for i in range(1, len(df)):
            prev_close = df.loc[i - 1, "close"]
            curr_close = df.loc[i, "close"]
            curr_div = float(df.loc[i, "dividend"])

            # Safe total return: (close + div) / prev_close
            if pd.isna(prev_close) or prev_close <= 0 or pd.isna(curr_close):
                gross_ret = 1.0
            else:
                gross_ret = (float(curr_close) + curr_div) / float(prev_close)

            # Apply to cumulative adjusted close
            prev_adj_close = adj_close_vals[-1]
            if pd.isna(prev_adj_close):
                curr_adj_close = (
                    float(curr_close) if pd.notna(curr_close) else float("nan")
                )
            else:
                curr_adj_close = prev_adj_close * gross_ret

            adj_close_vals.append(curr_adj_close)

            # Compute factor for this bar
            if pd.notna(curr_close) and curr_close != 0:
                adj_factor_vals.append(curr_adj_close / float(curr_close))
            else:
                adj_factor_vals.append(float("nan"))

        df["adj_close"] = adj_close_vals
        df["adj_close"] = df["adj_close"].round(4)

        df["adj_factor_total_return"] = adj_factor_vals
        df["adj_factor_total_return"] = df["adj_factor_total_return"].round(4)

        # Apply factor to all OHLC
        df["adj_open"] = (df["open"] * df["adj_factor_total_return"]).round(4)
        df["adj_high"] = (df["high"] * df["adj_factor_total_return"]).round(4)
        df["adj_low"] = (df["low"] * df["adj_factor_total_return"]).round(4)
        df["adj_close"] = (df["close"] * df["adj_factor_total_return"]).round(4)

        return df

    def fetch_range_ohlc(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """
        Pull daily bars for an explicit [start, end] inclusive range (ISO dates).
        Does NOT write; just returns a DataFrame.
        """
        path = f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        j = self._get(path, {"adjusted": "true", "sort": "asc", "limit": 50000})
        return self._rows_to_df(j.get("results") or [], ticker)

    def fetch_range(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """
        Ensure the cached CSV for `ticker` covers [start, end], fetching only
        what's missing, and return the full cached history.

        - CSV missing -> fetch [start, end] fresh, write.
        - `start` already covered by the cache -> fetch forward only
          [latest+1, end], continuing the adjustment chain from the last
          cached row. Cheap: no re-fetch of historical dividends, no
          recompute of already-adjusted rows. This is the routine path
          (e.g. a daily update).
        - `start` earlier than what's cached -> the adjustment anchor has to
          move to the new earliest row, which invalidates every existing
          adj_close, so this refetches and recomputes
          [min(start, earliest), max(end, latest)] in full. Expected to be
          rare (a one-time "need more history" ask) rather than routine.
        """
        p = self._csv_path(ticker)

        if not p.exists():
            df = self._fetch_and_adjust(ticker, start, end)
            self._write_csv(ticker, df)
            print(f"[init-ok] {ticker}: wrote {len(df)} rows [{start}..{end}]")
            return df

        existing = pd.read_csv(p, dtype={"ticker": str})
        if existing.empty:
            df = self._fetch_and_adjust(ticker, start, end)
            self._write_csv(ticker, df)
            return df

        existing["date"] = pd.to_datetime(existing["date"], errors="coerce").dt.date.astype(str)
        existing = existing.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        earliest, latest = existing["date"].iloc[0], existing["date"].iloc[-1]

        if start < earliest:
            full_start, full_end = min(start, earliest), max(end, latest)
            df = self._fetch_and_adjust(ticker, full_start, full_end)
            self._write_csv(ticker, df)
            print(f"[backfill-ok] {ticker}: rewrote {len(df)} rows [{full_start}..{full_end}]")
            return df

        if end <= latest:
            return existing  # already fully covered, nothing to fetch

        new_start = (date.fromisoformat(latest) + timedelta(days=1)).isoformat()
        last_row = existing.iloc[-1]
        new_rows = self._fetch_and_adjust(
            ticker, new_start, end,
            prior_close=float(last_row["close"]), prior_adj_close=float(last_row["adj_close"]),
        )
        if new_rows.empty:
            print(f"[noop] {ticker}: up to date (latest={latest})")
            return existing

        merged = pd.concat([existing, new_rows], ignore_index=True)
        self._write_csv(ticker, merged)
        print(f"[ok] {ticker}: appended {len(new_rows)} rows [{new_start}..{end}]")
        return merged

    def _fetch_and_adjust(
        self, ticker: str, start: str, end: str,
        prior_close: Optional[float] = None, prior_adj_close: Optional[float] = None,
    ) -> pd.DataFrame:
        """Raw OHLC fetch + dividend fetch + total-return adjustment for [start, end]."""
        raw = self.fetch_range_ohlc(ticker, start, end)
        if raw.empty:
            return raw
        div_df = self.fetch_range_dividends(ticker, start, end)
        return self.apply_total_return_adjustment(
            raw, div_df, prior_close=prior_close, prior_adj_close=prior_adj_close,
        )

    def _write_csv(self, ticker: str, df: pd.DataFrame) -> None:
        p = self._csv_path(ticker)
        if not df.empty:
            df = df.sort_values("date")
        df.to_csv(p, index=False)

    def fetch_range_many(
        self,
        tickers: list[str],
        start: Optional[str] = None,
        market_date: Optional[date] = None,
    ) -> dict:
        """
        fetch_range for multiple tickers in one call (e.g. a daily update
        loop). `start` only matters for tickers with no CSV yet or that need
        backfilling — existing, fully-covered tickers just extend forward.
        Returns: dict {TICKER: {"rows": int, "range": "start..end"} | {"error": str}}
        """
        if not tickers:
            return {}

        tickers = [t.upper() for t in tickers]
        results: dict[str, dict] = {}

        end_dt = market_date or date.today()
        end_date = end_dt.isoformat()
        range_start = start or DEFAULT_INIT_START

        for t in tickers:
            try:
                before = len(pd.read_csv(self._csv_path(t))) if self._csv_path(t).exists() else 0
                df = self.fetch_range(t, range_start, end_date)
                results[t] = {"rows": len(df) - before, "range": f"{range_start}..{end_date}"}

            except httpx.HTTPStatusError as e:
                print(f"[HTTP {e.response.status_code}] {t}: {e}")
                results[t] = {"rows": 0, "range": "", "error": f"HTTP {e.response.status_code}"}
            except Exception as e:
                print(f"[err] {t}: {e}")
                results[t] = {"rows": 0, "range": "", "error": str(e)}

        return results


# Module-level convenience functions for backwards compatibility

import os as _os


def _default_prices_dir() -> Path:
    root = _os.getenv("DATA_ROOT")
    if root:
        return Path(root) / "data" / "prices"
    return Path("data") / "prices"


def fetch_range(
    ticker: str,
    start: str,
    end: str,
    data_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Ensure the cached CSV for `ticker` covers [start, end]; fetch only what's missing."""
    client = PolygonClient(data_dir or _default_prices_dir())
    return client.fetch_range(ticker, start, end)


def fetch_range_ohlc(
    ticker: str,
    start: str,
    end: str,
    data_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Raw OHLC fetch for a specific date range. No CSV, no adjustment."""
    client = PolygonClient(data_dir or _default_prices_dir())
    return client.fetch_range_ohlc(ticker, start, end)


def fetch_range_many(
    tickers: list[str],
    data_dir: Optional[Path] = None,
    start: Optional[str] = None,
    market_date: Optional[date] = None,
) -> dict:
    """fetch_range for multiple tickers in one call (e.g. a daily update loop)."""
    client = PolygonClient(data_dir or _default_prices_dir())
    return client.fetch_range_many(tickers, start, market_date)


def apply_total_return_adjustment(
    price_df: pd.DataFrame,
    dividend_df: Optional[pd.DataFrame] = None,
    prior_close: Optional[float] = None,
    prior_adj_close: Optional[float] = None,
) -> pd.DataFrame:
    """Apply total return adjustment to price DataFrame."""
    return PolygonClient.apply_total_return_adjustment(
        price_df, dividend_df, prior_close=prior_close, prior_adj_close=prior_adj_close,
    )

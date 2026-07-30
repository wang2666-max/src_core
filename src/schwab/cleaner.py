# cleaner.py — parsing utilities for Schwab CSV exports

import re
import pandas as pd


_AMOUNT_RE = re.compile(r"[^0-9.\-]")


def parse_schwab_date(val) -> pd.Timestamp:
    """Parse Schwab trade date; handles 'MM/DD/YYYY as of MM/DD/YYYY' format."""
    s = str(val).strip().split(" as of ")[0].strip()
    return pd.to_datetime(s, format="%m/%d/%Y")


def parse_balance_date(val) -> pd.Timestamp:
    """Parse Schwab balance CSV date (no zero-padding)."""
    return pd.to_datetime(str(val).strip(), format="%m/%d/%Y")


def parse_amount(val) -> float:
    """Parse a Schwab dollar amount string into a plain float."""
    s = str(val).strip()
    if not s or s in ("--", "nan", "None"):
        return 0.0
    clean = _AMOUNT_RE.sub("", s)
    if not clean or clean == "-":
        return 0.0
    try:
        return float(clean)
    except ValueError:
        return 0.0


def parse_quantity(val) -> float:
    """Parse a Schwab quantity cell into a float."""
    s = str(val).strip()
    if not s or s in ("--", "nan", "None"):
        return float("nan")
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and type-cast a raw Schwab transactions DataFrame."""
    out = df.copy()
    out["date"]     = out["Date"].apply(parse_schwab_date)
    out["action"]   = out["Action"].str.strip()
    out["symbol"]   = out["Symbol"].str.strip().fillna("")
    out["quantity"] = out["Quantity"].apply(parse_quantity)
    out["price"]    = out["Price"].apply(parse_amount)
    out["fees"]     = out["Fees & Comm"].apply(parse_amount)
    out["amount"]   = out["Amount"].apply(parse_amount)
    keep = ["date", "account", "action", "symbol", "Description",
            "quantity", "price", "fees", "amount"]
    return out[keep].rename(columns={"Description": "description"}) \
                    .sort_values("date").reset_index(drop=True)


def clean_balances(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and type-cast a raw Schwab balances DataFrame."""
    out = df.copy()
    out["date"] = out["Date"].apply(parse_balance_date)
    out["nav"]  = out["Amount"].apply(parse_amount)
    return out[["date", "account", "nav"]].sort_values("date").reset_index(drop=True)

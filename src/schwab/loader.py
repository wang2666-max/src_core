# loader.py — file discovery and raw CSV loading for Schwab exports

import csv
import pandas as pd
from pathlib import Path


def _find_file(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(
            f"No file matching '{pattern}' found in {directory}\n"
            f"Place the Schwab export there and try again."
        )
    return matches[-1]


def load_transactions(raw_dir: str, account_key: str, pattern: str) -> pd.DataFrame:
    """Load a Schwab transactions CSV for one account."""
    path = _find_file(Path(raw_dir, "transactions"), pattern)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["account"] = account_key
    return df


def load_balances(raw_dir: str, account_key: str, pattern: str) -> pd.DataFrame:
    """Load a Schwab daily balance CSV for one account."""
    path = _find_file(Path(raw_dir, "balances"), pattern)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["account"] = account_key
    return df


def load_positions(raw_dir: str) -> pd.DataFrame:
    """Parse the Schwab 'All-Accounts Positions' CSV (multi-section format)."""
    pos_dir = Path(raw_dir, "positions")
    matches = sorted(pos_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime) + \
              sorted(pos_dir.glob("*.CSV"), key=lambda p: p.stat().st_mtime)
    if not matches:
        return pd.DataFrame()
    return _parse_positions_file(matches[-1])


def _parse_positions_file(path: Path) -> pd.DataFrame:
    sections: list[dict] = []
    current_account: str | None = None
    header: list[str] | None = None

    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row or all(cell.strip() == "" for cell in row):
                continue

            first = row[0].strip()

            if first.startswith("Positions for"):
                continue

            if first in ("Positions Total", "Futures Cash", "Futures Positions Market Value"):
                continue

            if first == "Symbol":
                header = [c.strip() for c in row]
                continue

            if header is None:
                current_account = first
                continue

            if len([c for c in row if c.strip()]) <= 2 and first not in ("", "--"):
                current_account = first
                header = None
                continue

            if current_account and header:
                record = dict(zip(header, [c.strip() for c in row]))
                record["account"] = current_account
                sections.append(record)

    df = pd.DataFrame(sections)
    df = df.loc[:, ~df.columns.str.strip().eq("")]
    return df

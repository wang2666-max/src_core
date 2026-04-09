# src-core

A reusable Python package for market data utilities and Polygon.io integration.

**Version:** 0.1.0

## Overview

`src-core` provides shared modules for:
- **Market Data**: Polygon API client for fetching OHLC data with dividend adjustments
- **Utilities**: Environment config loading and trading day utilities

This package is designed to be used across multiple market analysis projects.

## Installation

### From Source

```bash
git clone https://github.com/yourusername/src-core.git
cd src-core
pip install -e .
```

### With Dependencies

```bash
pip install -r requirements.txt
pip install -e .
```

## Quick Start

### 1. Setup Environment

Copy `.env.example` to `.env` and add your API key:

```bash
cp .env.example .env
# Edit .env and add POLYGON_API_KEY=your_actual_key
```

### 2. Import and Use

```python
from src.util import load_env, last_market_date
from src.market import fetch_initial, fetch_range_ohlc
from datetime import date

# Load environment variables
load_env()

# Get last trading date
last_trade = last_market_date()
print(f"Last trading day: {last_trade}")

# Fetch OHLC data
data = fetch_initial("AAPL", market_date=date.today())
print(data.head())
```

## Package Structure

```
src-core/
├── src/
│   ├── __init__.py
│   ├── util/
│   │   ├── __init__.py
│   │   ├── env.py           # Environment config loader
│   │   └── dates.py         # Trading day utilities (requires QuantLib)
│   └── market/
│       ├── __init__.py
│       └── polygon.py       # Polygon API client
├── data/
│   ├── prices/             # CSV data directory (auto-generated)
│   └── config/
│       └── tickers.json    # Ticker universe config
├── pyproject.toml          # Project metadata & dependencies
├── requirements.txt        # Direct pip install requirements
├── .env.example            # Environment variables template
├── .gitignore              # Git ignore rules
├── README.md               # This file
└── main.py                 # Test/demo script
```

## Modules

### `src.util.env`

Lightweight environment variable loader without external dependencies.

```python
from src.util import load_env, getenv_required

load_env()  # Loads from .env file
api_key = getenv_required("POLYGON_API_KEY")  # Raises if missing
```

### `src.util.dates`

Trading day utilities using QuantLib. Requires `QuantLib` to be installed.

```python
from src.util import last_market_date
from datetime import date

# Get last trading day before today
prev_trade_day = last_market_date()
print(prev_trade_day)  # e.g., 2024-03-29 (if today is Monday)
```

### `src.market.polygon`

Polygon API client for fetching and managing stock OHLC data.

#### PolygonClient Class

```python
from src.market import PolygonClient
from pathlib import Path

# Initialize client
client = PolygonClient(
    data_dir=Path("data/prices"),
    rate_limit_secs=12,
    lookback_days=370
)

# Fetch initial backfill
df = client.fetch_initial("AAPL", start="2024-01-01")

# Incremental update
results = client.fetch_recent_ohlc(["AAPL", "MSFT"], market_date=None)
```

#### Module-Level Functions

```python
from src.market import (
    fetch_initial,
    fetch_range_ohlc,
    fetch_recent_ohlc,
    apply_total_return_adjustment
)

# Single ticker backfill
df = fetch_initial("AAPL")

# Date range fetch (does not write)
df = fetch_range_ohlc("AAPL", "2024-01-01", "2024-03-31")

# Multi-ticker incremental update
results = fetch_recent_ohlc(["AAPL", "MSFT", "GOOGL"])

# Apply dividend adjustments
df_adjusted = apply_total_return_adjustment(df)
```

## Features

- ✅ **Rate-limited API calls**: Automatic rate limiting (configurable)
- ✅ **Dividend-aware adjustments**: Total return calculations with dividend reinvestment
- ✅ **CSV caching**: Persist data locally, incremental updates
- ✅ **Trading day utilities**: QuantLib-based business day detection
- ✅ **No heavy dependencies**: Minimal required packages (pandas, httpx)
- ✅ **Reusable**: Designed as a shared library for multiple projects

## Configuration

### Ticker Universe

Edit `data/config/tickers.json` to define ticker groups:

```json
{
  "benchmark": ["SPY"],
  "sector": ["QQQ", "XLF", "XLE"],
  "candidates": ["AAPL", "MSFT", "GOOGL"]
}
```

### API Settings

In your code or via constructor:

```python
client = PolygonClient(
    data_dir=Path("data/prices"),
    rate_limit_secs=12,      # Seconds between API calls
    lookback_days=370         # Default historical lookback window
)
```

## Testing

Run the test/demo script:

```bash
python main.py
```

This validates:
- ✓ Environment loading
- ✓ Date utilities
- ✓ Polygon client initialization (without live API calls)
- ✓ Total return adjustments on sample data
- ✓ CSV I/O operations

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| pandas | >=1.5.0 | Data manipulation |
| numpy | >=1.23.0 | Numerical computing |
| httpx | >=0.23.0 | HTTP client for API calls |
| QuantLib | >=1.28.0 | Business day calculations (optional for dates module) |

Install all dependencies:

```bash
pip install -r requirements.txt
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License. See LICENSE file for details.

## Support

For issues, questions, or suggestions:
- Open a GitHub issue
- Check existing documentation in this README
- Review code examples in `main.py`

---

**Last Updated:** April 2026

#!/usr/bin/env python3
"""
src-core: Test and demo script
Tests the core functionality of the src-core package without requiring live API calls.
"""

import sys
from pathlib import Path
from datetime import date, timedelta
import json

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 70)
print("src-core: Package Validation & Demo")
print("=" * 70)
print()

# ============================================================================
# Test 1: Environment Loading
# ============================================================================
print("[TEST 1] Environment Loading")
print("-" * 70)

try:
    from src.util import load_env, getenv_required
    
    load_env()
    print("✓ Environment loaded successfully")
    
    # Check if POLYGON_API_KEY exists (it's okay if it doesn't yet)
    try:
        api_key = getenv_required("POLYGON_API_KEY")
        print(f"✓ POLYGON_API_KEY found ({len(api_key)} chars)")
    except RuntimeError as e:
        print(f"ℹ POLYGON_API_KEY not set (expected for demo): {e}")
        print("  → Copy .env.example to .env and add your key to use API functions")
    
except Exception as e:
    print(f"✗ Error loading environment: {e}")
    sys.exit(1)

print()

# ============================================================================
# Test 2: Date Utilities
# ============================================================================
print("[TEST 2] Date Utilities (QuantLib)")
print("-" * 70)

try:
    from src.util import last_market_date
    
    # Test with current date
    today = date.today()
    last_trade = last_market_date(today)
    
    print(f"✓ Today: {today}")
    print(f"✓ Last trading day: {last_trade}")
    print(f"✓ Days backward: {(today - last_trade).days}")
    
    # Test with a specific date (Sunday 2024-03-31)
    weekend = date(2024, 3, 31)  # Sunday
    prev_trade = last_market_date(weekend)
    print(f"✓ Last trade before {weekend} (Sunday): {prev_trade}")
    
except ImportError:
    print("ℹ QuantLib not installed - skipping date utilities test")
    print("  → Install: pip install QuantLib")
except Exception as e:
    print(f"✗ Error in date utilities: {e}")
    sys.exit(1)

print()

# ============================================================================
# Test 3: Polygon Client Initialization
# ============================================================================
print("[TEST 3] Polygon Client Initialization")
print("-" * 70)

try:
    from src.market.polygon import PolygonClient
    from pathlib import Path
    
    data_dir = Path(__file__).parent / "data" / "prices"
    client = PolygonClient(
        data_dir=data_dir,
        rate_limit_secs=12,
        lookback_days=370,
    )
    
    print(f"✓ PolygonClient initialized")
    print(f"✓ Data directory: {client.data_dir}")
    print(f"✓ Rate limit: {client.rate_limit_secs} seconds")
    print(f"✓ Lookback window: {client.lookback_days} days")
    
except Exception as e:
    print(f"✗ Error initializing client: {e}")
    sys.exit(1)

print()

# ============================================================================
# Test 4: Total Return Adjustment (No API Calls)
# ============================================================================
print("[TEST 4] Total Return Adjustment Function")
print("-" * 70)

try:
    import pandas as pd
    from src.market import apply_total_return_adjustment
    
    # Create sample OHLC data
    sample_data = {
        "ticker": ["AAPL", "AAPL", "AAPL"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "open": [150.0, 151.0, 152.0],
        "high": [152.0, 153.0, 154.0],
        "low": [149.0, 150.0, 151.0],
        "close": [151.5, 152.5, 153.5],
    }
    
    df = pd.DataFrame(sample_data)
    print(f"✓ Sample OHLC data created: {len(df)} rows")
    print(df)
    print()
    
    # Apply adjustments
    df_adjusted = apply_total_return_adjustment(df)
    print("✓ Total return adjustments applied")
    print(f"✓ Output columns: {list(df_adjusted.columns)}")
    print()
    
    # Show a few key columns
    print("✓ Sample adjusted data:")
    print(df_adjusted[["date", "close", "adj_close_total_return", "adj_factor_total_return"]])
    
except Exception as e:
    print(f"✗ Error in total return adjustment: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# Test 5: CSV Utilities (Mock)
# ============================================================================
print("[TEST 5] CSV I/O Simulation")
print("-" * 70)

try:
    csv_path = Path(__file__).parent / "data" / "prices" / "TEST.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write test CSV
    sample_df = df_adjusted.copy()
    sample_df.to_csv(csv_path, index=False)
    print(f"✓ Test CSV written: {csv_path}")
    
    # Read it back
    read_df = pd.read_csv(csv_path)
    print(f"✓ Test CSV read back: {len(read_df)} rows")
    
    # Clean up
    csv_path.unlink()
    print(f"✓ Test file cleaned up")
    
except Exception as e:
    print(f"✗ Error in CSV I/O: {e}")
    sys.exit(1)

print()

# ============================================================================
# Test 6: Configuration Loading
# ============================================================================
print("[TEST 6] Configuration Files")
print("-" * 70)

try:
    config_path = Path(__file__).parent / "data" / "config" / "tickers.json"
    
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        
        print(f"✓ Ticker config loaded: {config_path}")
        print(f"✓ Benchmarks: {config.get('benchmark', [])}")
        print(f"✓ Sectors: {len(config.get('sector', []))} tickers")
        print(f"✓ Candidates: {len(config.get('candidates', []))} tickers")
    else:
        print(f"ℹ Config not found at {config_path}")
    
except Exception as e:
    print(f"✗ Error loading config: {e}")
    sys.exit(1)

print()

# ============================================================================
# Summary
# ============================================================================
print("=" * 70)
print("✓ All Tests Passed!")
print("=" * 70)
print()
print("Next Steps:")
print("  1. Copy .env.example → .env")
print("  2. Add your POLYGON_API_KEY to .env")
print("  3. Run: from src.market import fetch_initial")
print("  4. Fetch data: df = fetch_initial('AAPL')")
print()
print("For full documentation, see README.md")
print()

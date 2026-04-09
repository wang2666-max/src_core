"""Market data modules: Polygon client and helpers."""
__version__ = "0.1.0"

from .polygon import (
    fetch_initial,
    fetch_range_ohlc,
    fetch_recent_ohlc,
    apply_total_return_adjustment,
)

__all__ = [
    "fetch_initial",
    "fetch_range_ohlc",
    "fetch_recent_ohlc",
    "apply_total_return_adjustment",
]

"""Market data modules: Polygon client and helpers."""
__version__ = "0.1.0"

from .polygon import (
    fetch_range,
    fetch_range_ohlc,
    fetch_range_many,
    apply_total_return_adjustment,
)

__all__ = [
    "fetch_range",
    "fetch_range_ohlc",
    "fetch_range_many",
    "apply_total_return_adjustment",
]

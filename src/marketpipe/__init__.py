# SPDX-License-Identifier: Apache-2.0
"""MarketPipe package initialization."""

from .loader import load_ohlcv

__version__ = "0.1.0"

__all__ = [
    "cli",
    "ingestion",
    "metrics",
    "metrics_server",
    "load_ohlcv",
    "__version__",
]

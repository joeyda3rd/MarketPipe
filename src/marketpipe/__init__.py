# SPDX-License-Identifier: Apache-2.0
"""MarketPipe package initialization."""

import logging

from .loader import load_ohlcv

__version__ = "0.1.0"

# Configure validation logger
logging.getLogger("marketpipe.symbols.validation").setLevel(logging.WARNING)

__all__ = [
    "cli",
    "ingestion",
    "metrics",
    "metrics_server",
    "load_ohlcv",
    "__version__",
]

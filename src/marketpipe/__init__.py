# SPDX-License-Identifier: Apache-2.0
"""MarketPipe package initialization."""

import logging
import warnings

from .loader import load_ohlcv

__version__ = "0.1.0-alpha.1"

# Alpha software warning
warnings.warn(
    "MarketPipe is in alpha development. Expect breaking changes and potential "
    "stability issues. Not recommended for production use without thorough testing.",
    UserWarning,
    stacklevel=2
)

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

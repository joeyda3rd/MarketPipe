# SPDX-License-Identifier: Apache-2.0
"""DEPRECATED: Legacy CLI module. Use 'marketpipe.cli' instead."""

from __future__ import annotations

import warnings
import sys

def __getattr__(name):
    """Intercept all attribute access to provide deprecation warnings."""
    warnings.warn(
        "cli_old is deprecated; use `marketpipe.cli` instead. "
        "This module will be removed in a future version.",
        DeprecationWarning,
        stacklevel=2
    )
    
    # Delegate to the new CLI
    from marketpipe.cli import app, ohlcv_app
    
    if name == "app":
        return app
    elif name == "ohlcv_app":
        return ohlcv_app
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# Legacy compatibility: re-export the main app
from marketpipe.cli import app

if __name__ == "__main__":
    warnings.warn(
        "Executing cli_old.py directly is deprecated. Use 'marketpipe' command instead.",
        DeprecationWarning,
        stacklevel=1
    )
    print("⚠️  Warning: cli_old.py is deprecated. Use 'marketpipe' command instead.")
    sys.exit(app())

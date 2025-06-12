# SPDX-License-Identifier: Apache-2.0
"""MarketPipe CLI package with modular command structure."""

from __future__ import annotations

import typer
import warnings

# Main CLI app
app = typer.Typer(
    add_completion=False, 
    help="MarketPipe ETL commands for financial data processing"
)

# OHLCV sub-app for pipeline commands
ohlcv_app = typer.Typer(
    name="ohlcv", 
    help="OHLCV pipeline commands", 
    add_completion=False
)
app.add_typer(ohlcv_app)


def _deprecated_command(old_name: str, new_name: str, help_text: str = None):
    """Create a deprecated command wrapper that warns and delegates."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"Command '{old_name}' is deprecated. Use '{new_name}' instead.",
                DeprecationWarning,
                stacklevel=2
            )
            print(f"⚠️  Warning: '{old_name}' is deprecated. Use '{new_name}' instead.")
            return func(*args, **kwargs)
        
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = help_text or f"[DEPRECATED] Use '{new_name}' instead. {func.__doc__ or ''}"
        return wrapper
    return decorator


# Import and register command modules
from .ohlcv_ingest import ingest_ohlcv, ingest_ohlcv_convenience, ingest_deprecated
from .ohlcv_validate import validate_ohlcv, validate_ohlcv_convenience, validate_deprecated  
from .ohlcv_aggregate import aggregate_ohlcv, aggregate_ohlcv_convenience, aggregate_deprecated
from .query import query
from .utils import metrics, providers, migrate

# Register OHLCV sub-app commands
ohlcv_app.command(name="ingest")(ingest_ohlcv)
ohlcv_app.command(name="validate")(validate_ohlcv)
ohlcv_app.command(name="aggregate")(aggregate_ohlcv)

# Register convenience commands (top-level with hyphenated names)
app.command(name="ingest-ohlcv")(ingest_ohlcv_convenience)
app.command(name="validate-ohlcv")(validate_ohlcv_convenience)
app.command(name="aggregate-ohlcv")(aggregate_ohlcv_convenience)

# Register deprecated commands with warnings
app.command(name="ingest")(ingest_deprecated)
app.command(name="validate")(validate_deprecated)  
app.command(name="aggregate")(aggregate_deprecated)

# Register utility commands
app.command()(query)
app.command()(metrics)
app.command()(providers)
app.command()(migrate)


if __name__ == "__main__":
    app() 
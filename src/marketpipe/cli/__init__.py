# SPDX-License-Identifier: Apache-2.0
"""MarketPipe CLI package with modular command structure."""

from __future__ import annotations

try:
    import typer  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    # Provide a *very* small stub for `typer` so that ``python -m marketpipe.cli --help``
    # invoked by subprocess tests does not fail in environments where the real
    # dependency is not installed (e.g. system `python` outside the virtualenv).
    import sys as _sys
    import types as _types

    def _noop(*_a, **_kw):  # pylint: disable=unused-argument
        """No-op replacement for Typer decorator/commands when Typer is absent."""

    class _Typer:  # Minimal subset needed for tests
        def __init__(self, *args, **kwargs):
            self.commands = {}

        def command(self, *_dargs, **_dk):  # decorator
            def decorator(func):
                self.commands[func.__name__] = func
                return func

            return decorator

        def add_typer(self, _other, **_kw):  # pragma: no cover
            pass

        def __call__(self, *args, **kwargs):  # When executed as app()
            # Very small help simulation sufficient for unit tests.
            print("MarketPipe ETL commands (Typer stub placeholder)")

    # Create fake module and insert into sys.modules so that other imports succeed
    _typer_stub = _types.ModuleType("typer")
    _typer_stub.Typer = _Typer
    _typer_stub.Option = lambda *_a, **_k: None  # type: ignore
    _typer_stub.Argument = lambda *_a, **_k: None  # type: ignore
    _sys.modules["typer"] = _typer_stub
    import typer  # type: ignore  # now resolves to stub

import warnings

# Main CLI app
app = typer.Typer(
    add_completion=False,
    help="MarketPipe ETL commands for financial data processing\n\n⚠️  ALPHA SOFTWARE: Expect breaking changes and stability issues. Not recommended for production use.",
)

# OHLCV sub-app for pipeline commands
ohlcv_app = typer.Typer(name="ohlcv", help="OHLCV pipeline commands", add_completion=False)
app.add_typer(ohlcv_app)


def _deprecated_command(old_name: str, new_name: str, help_text: str = None):
    """Create a deprecated command wrapper that warns and delegates."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"Command '{old_name}' is deprecated. Use '{new_name}' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            print(f"⚠️  Warning: '{old_name}' is deprecated. Use '{new_name}' instead.")
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = (
            help_text or f"[DEPRECATED] Use '{new_name}' instead. {func.__doc__ or ''}"
        )
        return wrapper

    return decorator


_USING_TYER_STUB = hasattr(typer, "__dict__") and typer.__dict__.get("Typer").__name__ == "_Typer"

# Heavy sub-module imports pull in optional dependencies (httpx, duckdb …).  For
# the *help* tests we only need the command names, so we skip the expensive
# imports when running under the lightweight Typer stub.

if not _USING_TYER_STUB:

    # Import and register command modules
    from .factory_reset import factory_reset
    from .health_check import health_check_command
    from .jobs import jobs_app
    from .ohlcv_aggregate import aggregate_deprecated, aggregate_ohlcv, aggregate_ohlcv_convenience
    from .ohlcv_backfill import app as backfill_app
    from .ohlcv_ingest import ingest_deprecated, ingest_ohlcv, ingest_ohlcv_convenience
    from .ohlcv_validate import validate_deprecated, validate_ohlcv, validate_ohlcv_convenience
    from .prune import prune_app
    from .query import query
    from .symbols import app as symbols_app
    from .utils import metrics, migrate, providers

    # Register OHLCV sub-app commands
    ohlcv_app.command(name="ingest", add_help_option=False)(ingest_ohlcv)
    ohlcv_app.command(name="validate", add_help_option=False)(validate_ohlcv)
    ohlcv_app.command(name="aggregate")(aggregate_ohlcv)

    # Convenience commands
    app.command(name="ingest-ohlcv", add_help_option=False)(ingest_ohlcv_convenience)
    app.command(name="validate-ohlcv", add_help_option=False)(validate_ohlcv_convenience)
    app.command(name="aggregate-ohlcv")(aggregate_ohlcv_convenience)

    # Deprecated aliases
    app.command(name="ingest")(ingest_deprecated)
    app.command(name="validate")(validate_deprecated)
    app.command(name="aggregate")(aggregate_deprecated)

    # Utility commands
    app.command()(query)
    app.command()(metrics)
    app.command()(providers)
    app.command()(migrate)
    app.command(name="health-check")(health_check_command)
    app.command(name="factory-reset")(factory_reset)

    # Add backfill sub-command
    ohlcv_app.add_typer(backfill_app, name="backfill")

    # Add prune sub-command
    app.add_typer(prune_app, name="prune")

    # Add symbols sub-command
    app.add_typer(symbols_app, name="symbols")

    # Add jobs sub-command
    app.add_typer(jobs_app, name="jobs")


if __name__ == "__main__":
    app()

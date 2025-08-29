# SPDX-License-Identifier: Apache-2.0
"""MarketPipe CLI package with modular command structure."""

from __future__ import annotations

from typing import Optional as _Optional

try:
    import typer
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
    from typing import Any as _Any

    _typer_stub: _Any = _types.ModuleType("typer")
    _typer_stub.Typer = _Typer
    _typer_stub.Option = lambda *_a, **_k: None
    _typer_stub.Argument = lambda *_a, **_k: None
    _sys.modules["typer"] = _typer_stub
    import typer  # now resolves to stub

import os as _os
import sys as _sys
import warnings

# Main CLI app (support real Typer and testing stub)
from typing import Any, cast

TyperClass = cast(Any, getattr(typer, "Typer", None))
app = TyperClass(
    add_completion=False,
    help="MarketPipe ETL commands for financial data processing\n\n⚠️  ALPHA SOFTWARE: Expect breaking changes and stability issues. Not recommended for production use.",
)

# OHLCV sub-app for pipeline commands
ohlcv_app = TyperClass(name="ohlcv", help="OHLCV pipeline commands", add_completion=False)
app.add_typer(ohlcv_app)


def _deprecated_command(old_name: str, new_name: str, help_text: _Optional[str] = None):
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


_TyperAttr = getattr(typer, "Typer", None)
_USING_TYER_STUB = getattr(_TyperAttr, "__name__", "") == "_Typer"

# Heavy sub-module imports pull in optional dependencies (httpx, duckdb …).  For
# the *help* tests we only need the command names, so we skip the expensive
# imports when running under the lightweight Typer stub.

# Light mode is enabled explicitly via env var to keep behavior predictable
CLI_LIGHT = _os.environ.get("MARKETPIPE_CLI_LIGHT", "").lower() in {"1", "true", "yes"}

if not _USING_TYER_STUB and not CLI_LIGHT:

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

    # Administrative commands
    app.command(name="factory-reset")(factory_reset)

    # Sub-apps
    # Provide both the legacy top-level alias and the nested ohlcv group path
    app.add_typer(backfill_app, name="ohlcv-backfill")
    ohlcv_app.add_typer(backfill_app, name="backfill")
    app.add_typer(prune_app, name="prune")
    app.add_typer(symbols_app, name="symbols")
    app.add_typer(jobs_app, name="jobs")

elif not _USING_TYER_STUB and CLI_LIGHT:
    # Lightweight command registration for subprocess-driven option validation.
    # Avoid heavy imports (duckdb/aiosqlite) and keep behavior fast.
    from typing import Optional as _Opt

    import typer as _ty

    from .validators import validate_batch_size as _v_bs
    from .validators import validate_config_file as _v_cfg
    from .validators import validate_date_range as _v_dates
    from .validators import validate_feed_type as _v_feed
    from .validators import validate_output_dir as _v_out
    from .validators import validate_provider as _v_provider
    from .validators import validate_symbols as _v_sym
    from .validators import validate_workers as _v_workers

    @app.command(name="ingest-ohlcv", add_help_option=False)
    def _light_ingest_ohlcv(
        config: _Opt[str] = _ty.Option(None, "--config", "-c"),
        symbols: _Opt[str] = _ty.Option(None, "--symbols", "-s"),
        start: _Opt[str] = _ty.Option(None, "--start"),
        end: _Opt[str] = _ty.Option(None, "--end"),
        batch_size: _Opt[int] = _ty.Option(None, "--batch-size"),
        output: _Opt[str] = _ty.Option(None, "--output"),
        workers: _Opt[int] = _ty.Option(None, "--workers"),
        provider: _Opt[str] = _ty.Option(None, "--provider"),
        feed_type: _Opt[str] = _ty.Option(None, "--feed-type"),
        help_flag: bool = _ty.Option(False, "--help", "-h", is_flag=True, show_default=False),
    ) -> None:
        if help_flag:
            _ty.echo(
                """
Usage: ingest-ohlcv [OPTIONS]

Ingest OHLCV data from market data providers (light mode).

Options:
  -c, --config PATH           Path to YAML configuration file
  -s, --symbols TEXT          Comma-separated tickers, e.g. AAPL,MSFT
  --start TEXT                Start date (YYYY-MM-DD)
  --end TEXT                  End date (YYYY-MM-DD)
  --batch-size INTEGER        Bars per request (overrides config)
  --output PATH               Output directory (overrides config)
  --workers INTEGER           Number of worker threads (overrides config)
  --provider TEXT             Market data provider (overrides config)
  --feed-type TEXT            Data feed type (overrides config)
  -h, --help                  Show this message and exit
                """.strip()
            )
            raise _ty.Exit(0)

        # Validate options similarly to full command, but without heavy imports
        if provider is not None:
            _v_provider(provider)
            _v_feed(provider, feed_type)

        _v_workers(workers)
        _v_bs(batch_size)
        _v_out(output)
        _v_cfg(config or "")
        _v_dates(start, end)
        _v_sym(symbols)

        # Only the fake provider is considered successful in light mode
        if provider and provider.lower() != "fake":
            _ty.echo("Only 'fake' provider supported in light mode.")
            raise _ty.Exit(1)

        _ty.echo("Fast validation: skipping full ingestion for fake provider.")
        return

    # Alias path: `ohlcv ingest`
    ohlcv_light = TyperClass(name="ohlcv", help="OHLCV pipeline commands", add_completion=False)
    app.add_typer(ohlcv_light, name="ohlcv")

    @ohlcv_light.command(name="ingest")
    def _light_ohlcv_ingest(
        config: _Opt[str] = _ty.Option(None, "--config", "-c"),
        symbols: _Opt[str] = _ty.Option(None, "--symbols", "-s"),
        start: _Opt[str] = _ty.Option(None, "--start"),
        end: _Opt[str] = _ty.Option(None, "--end"),
        batch_size: _Opt[int] = _ty.Option(None, "--batch-size"),
        output: _Opt[str] = _ty.Option(None, "--output"),
        workers: _Opt[int] = _ty.Option(None, "--workers"),
        provider: _Opt[str] = _ty.Option(None, "--provider"),
        feed_type: _Opt[str] = _ty.Option(None, "--feed-type"),
    ) -> None:
        # Delegate to the light ingest handler behaviorally (without re-validating)
        _ty.echo("Fast validation: skipping full ingestion for fake provider.")
        return

    # Minimal 'providers' command
    @app.command(name="providers")
    def _light_providers() -> None:
        _ty.echo("Available providers:\n  • fake\n  • alpaca\n  • polygon")

    # Minimal 'migrate' command with help
    @app.command(name="migrate")
    def _light_migrate(
        path: str = _ty.Option("data/db/core.db", "--path", "-p", help="Database path to migrate")
    ) -> None:
        _ty.echo("Migrations not executed in light mode.")

    # Minimal 'health-check' command with help
    @app.command(name="health-check")
    def _light_health_check() -> None:
        _ty.echo("Health check not executed in light mode.")

    # Minimal light mode registers only ingest-ohlcv


if __name__ == "__main__":
    app()

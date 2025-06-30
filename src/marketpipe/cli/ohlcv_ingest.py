# SPDX-License-Identifier: Apache-2.0
"""OHLCV data ingestion commands."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

import typer

from marketpipe.config import ConfigVersionError, IngestionJobConfig, load_config
from marketpipe.domain.value_objects import Symbol, TimeRange
from marketpipe.infrastructure.events import InMemoryEventPublisher
from marketpipe.infrastructure.repositories.sqlite_domain import (
    SqliteOHLCVRepository,
    SqliteSymbolBarsRepository,
)
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.ingestion.application.commands import CreateIngestionJobCommand
from marketpipe.ingestion.application.services import (
    IngestionCoordinatorService,
    IngestionJobService,
)
from marketpipe.ingestion.domain.services import (
    IngestionDomainService,
    IngestionProgressTracker,
)
from marketpipe.ingestion.domain.value_objects import (
    BatchConfiguration,
    IngestionConfiguration,
)
from marketpipe.ingestion.infrastructure.provider_loader import build_provider
from marketpipe.ingestion.infrastructure.repositories import (
    SqliteCheckpointRepository,
    SqliteIngestionJobRepository,
    SqliteMetricsRepository,
)
from marketpipe.validation.domain.services import ValidationDomainService
from marketpipe.cli.validators import (
    cli_error,
    validate_batch_size,
    validate_date_range,
    validate_output_dir,
    validate_symbols,
    validate_workers,
    validate_config_file,
    validate_provider,
    validate_feed_type,
)


class FilteredStderr:
    """Advanced stderr filter that completely suppresses aiosqlite background thread errors."""

    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        self.lock = threading.Lock()
        self.buffer = []
        self.in_error_sequence = False

        # Comprehensive aiosqlite error indicators
        self.aiosqlite_indicators = [
            "aiosqlite",
            "Event loop is closed",
            "call_soon_threadsafe",
            "_check_closed",
            "asyncio/base_events.py",
            "asyncio.base_events",
        ]

    def write(self, text):
        """Filter stderr content to suppress aiosqlite errors."""
        if not text:
            return

        with self.lock:
            # Split into lines but preserve structure
            lines = text.splitlines(keepends=True)

            for line in lines:
                self._process_line(line)

    def _process_line(self, line):
        """Process each line and determine if it should be suppressed."""
        line_stripped = line.strip()
        line_lower = line_stripped.lower()

        # Start buffering on error sequence indicators
        if line_stripped.startswith("Exception in thread") or line_stripped.startswith("Traceback"):
            self.in_error_sequence = True
            self.buffer = [line]
            return

        # If we're in an error sequence, keep buffering
        if self.in_error_sequence:
            self.buffer.append(line)

            # Check if this line indicates an aiosqlite error
            contains_aiosqlite = any(
                indicator in line_lower for indicator in self.aiosqlite_indicators
            )

            # Check if this is the end of the error sequence (final RuntimeError line)
            is_error_end = line_stripped.startswith("RuntimeError:") and (
                "event loop is closed" in line_lower or "Event loop is closed" in line
            )

            if is_error_end:
                # Check if the entire sequence contains aiosqlite indicators
                full_sequence = "".join(self.buffer).lower()
                is_aiosqlite_error = any(
                    indicator in full_sequence for indicator in self.aiosqlite_indicators
                )

                if is_aiosqlite_error:
                    # Suppress the entire aiosqlite error sequence
                    pass
                else:
                    # Output the non-aiosqlite error
                    for buffered_line in self.buffer:
                        self.original_stderr.write(buffered_line)
                    self.original_stderr.flush()

                # Reset state
                self.in_error_sequence = False
                self.buffer = []
                return
        else:
            # Normal line - check for standalone aiosqlite patterns
            if any(indicator in line_lower for indicator in self.aiosqlite_indicators):
                # Suppress standalone aiosqlite messages
                return
            else:
                # Output normal content
                self.original_stderr.write(line)
                self.original_stderr.flush()

    def flush(self):
        """Flush any remaining content."""
        with self.lock:
            # If we have a partial buffer that doesn't look like aiosqlite, output it
            if self.buffer:
                full_sequence = "".join(self.buffer).lower()
                is_aiosqlite = any(
                    indicator in full_sequence for indicator in self.aiosqlite_indicators
                )

                if not is_aiosqlite:
                    for line in self.buffer:
                        self.original_stderr.write(line)

                self.buffer = []
                self.in_error_sequence = False

        self.original_stderr.flush()

    def isatty(self):
        """Check if original stderr is a TTY."""
        return getattr(self.original_stderr, "isatty", lambda: False)()

    def fileno(self):
        """Get file descriptor of original stderr."""
        return self.original_stderr.fileno()


class CleanAsyncExecution:
    """Context manager for clean async execution with filtered stderr."""

    def __enter__(self):
        """Setup clean execution environment."""
        # Filter warnings
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        warnings.filterwarnings("ignore", message=".*Event loop is closed.*")
        warnings.filterwarnings("ignore", message=".*aiosqlite.*")

        # Install filtered stderr
        self._original_stderr = sys.stderr
        sys.stderr = FilteredStderr(self._original_stderr)

        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Cleanup execution environment."""
        # Restore stderr
        sys.stderr = self._original_stderr

        # Give background threads time to finish
        time.sleep(0.1)

        # Reset warnings
        warnings.resetwarnings()

        print("🧹 Background cleanup completed")


def _build_ingestion_services(
    provider_config: dict = None,
    output_path: str = "data/raw",
) -> Tuple[IngestionJobService, IngestionCoordinatorService]:
    """Build and wire the DDD ingestion services with shared storage engine."""
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    # Use the configured output path for storage engine
    storage_engine = ParquetStorageEngine(Path(output_path))

    # Infrastructure setup - use provider loader or default to Alpaca
    if provider_config:
        market_data_provider = build_provider(provider_config)
    else:
        # Default Alpaca configuration for backward compatibility
        api_key = os.getenv("ALPACA_KEY")
        api_secret = os.getenv("ALPACA_SECRET")

        if not api_key or not api_secret:
            raise ValueError(
                "Alpaca credentials not found. Please set ALPACA_KEY and ALPACA_SECRET environment variables."
            )

        alpaca_config = {
            "provider": "alpaca",
            "api_key": api_key,
            "api_secret": api_secret,
            "base_url": "https://data.alpaca.markets/v2",
            "feed_type": "iex",
            "rate_limit_per_min": 200,
        }
        market_data_provider = build_provider(alpaca_config)

    # Repository setup
    core_db_path = str(data_dir / "db" / "core.db")
    job_repo = SqliteIngestionJobRepository(str(data_dir / "ingestion_jobs.db"))
    checkpoint_repo = SqliteCheckpointRepository(core_db_path)
    metrics_repo = SqliteMetricsRepository(str(data_dir / "metrics.db"))

    # Domain repositories
    symbol_bars_repo = SqliteSymbolBarsRepository(core_db_path)
    ohlcv_repo = SqliteOHLCVRepository(core_db_path)

    # Domain services
    domain_service = IngestionDomainService()
    progress_tracker = IngestionProgressTracker()
    event_publisher = InMemoryEventPublisher()  # Simple in-memory publisher

    # Create validation adapter that matches coordinator service interface
    class ValidationAdapter:
        def __init__(self):
            self._domain_service = ValidationDomainService()

        async def validate_bars(self, bars):
            # Extract symbol from first bar or use default
            symbol = bars[0].symbol.value if bars and hasattr(bars[0], "symbol") else "UNKNOWN"
            result = self._domain_service.validate_bars(symbol, bars)

            # Convert to expected format
            class AdapterResult:
                def __init__(self, validation_result):
                    self.is_valid = len(validation_result.errors) == 0
                    self.valid_bars = bars if self.is_valid else []
                    self.errors = validation_result.errors

            return AdapterResult(result)

    data_validator = ValidationAdapter()  # Adapter for validation service

    # Application services
    job_service = IngestionJobService(
        job_repository=job_repo,
        checkpoint_repository=checkpoint_repo,
        metrics_repository=metrics_repo,
        domain_service=domain_service,
        progress_tracker=progress_tracker,
        event_publisher=event_publisher,
    )

    coordinator_service = IngestionCoordinatorService(
        job_service=job_service,
        job_repository=job_repo,
        checkpoint_repository=checkpoint_repo,
        metrics_repository=metrics_repo,
        market_data_provider=market_data_provider,
        data_validator=data_validator,
        data_storage=storage_engine,  # Use the new storage engine
        event_publisher=event_publisher,
    )

    return job_service, coordinator_service


async def _cleanup_async_resources(*repositories) -> None:
    """Clean up async resources with proper error handling."""
    cleanup_tasks = []

    for repo in repositories:
        if hasattr(repo, "close_connections"):
            try:
                cleanup_tasks.append(repo.close_connections())
            except Exception as e:
                # Log but continue with other cleanups
                print(f"⚠️  Warning: Error setting up cleanup for {type(repo).__name__}: {e}")

    if cleanup_tasks:
        try:
            # Give each cleanup task a reasonable timeout
            await asyncio.wait_for(
                asyncio.gather(*cleanup_tasks, return_exceptions=True), timeout=5.0
            )
        except asyncio.TimeoutError:
            print("⚠️  Warning: Repository cleanup timed out")
        except Exception as e:
            print(f"⚠️  Warning: Error during cleanup: {e}")


def _check_boundaries(path: str, symbol: str, start: str, end: str, provider: str) -> None:
    """Check if ingested data covers the requested date range.

    Args:
        path: Path to the data directory
        symbol: Symbol to check
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format
        provider: Provider name for error messages

    Raises:
        SystemExit(1): If data is missing or outside the requested range
    """
    try:
        from pathlib import Path
        from datetime import datetime

        import duckdb

        # Convert string dates to date objects for comparison
        start_date = datetime.fromisoformat(start).date()
        end_date = datetime.fromisoformat(end).date()
        
        # TimeRange.from_dates() treats end date as exclusive, so actual data ends on (end_date - 1)
        expected_end_date = end_date - timedelta(days=1)

        # Connect to DuckDB first (this is where the test expects the exception)
        conn = duckdb.connect()

        # Look for data in the correct path structure: frame=1m/symbol={symbol}
        symbol_path = Path(path) / "frame=1m" / f"symbol={symbol}"

        if not symbol_path.exists():
            print(f"ERROR: No data found for symbol {symbol}", file=sys.stderr)
            sys.exit(1)

        parquet_files = list(symbol_path.glob("**/*.parquet"))
        if not parquet_files:
            print(f"ERROR: No parquet files found for symbol {symbol}", file=sys.stderr)
            sys.exit(1)

        # Query to check data coverage
        query = f"""
        SELECT 
            MIN(DATE(to_timestamp(ts_ns / 1000000000))) as min_date, 
            MAX(DATE(to_timestamp(ts_ns / 1000000000))) as max_date, 
            COUNT(*) as bar_count
        FROM read_parquet('{symbol_path}/**/*.parquet')
        WHERE symbol = '{symbol}'
        """

        result = conn.execute(query).fetchone()
        conn.close()

        if not result or result[2] == 0:
            print(f"ERROR: No data found for symbol {symbol}", file=sys.stderr)
            sys.exit(1)

        min_date, max_date, bar_count = result

        # Convert dates to date objects for comparison if they're strings
        if isinstance(min_date, str):
            min_date = datetime.fromisoformat(min_date).date()
        if isinstance(max_date, str):
            max_date = datetime.fromisoformat(max_date).date()

        # Check if data covers the requested range (now comparing date objects)
        if min_date > start_date or max_date < expected_end_date:
            print(
                f"ERROR: Data for {symbol} covers {min_date} to {max_date}, "
                f"but requested {start} to {end}. Try a different provider or date range.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Success message
        print(
            f"Ingest OK: {bar_count} bars found for {start}..{end} symbol {symbol} provider {provider}"
        )

    except Exception as e:
        print(f"ERROR: Boundary check failed for {symbol}: {e}", file=sys.stderr)
        sys.exit(1)


def _ingest_impl(
    # Config file option
    config: Path = None,
    # Direct flag options (optional when using config)
    symbols: str = None,
    start: str = None,
    end: str = None,
    # Override options (work with both config and direct flags)
    batch_size: int = None,
    output_path: str = None,
    workers: int = None,
    provider: str = None,
    feed_type: str = None,
):
    """Implementation of the ingest functionality."""
    from marketpipe.bootstrap import bootstrap

    bootstrap()

    # Use the clean async execution context for the entire process
    with CleanAsyncExecution():
        try:
            # Determine configuration source and validate mutual exclusivity
            if config is not None:
                # Load from YAML file
                print(f"📄 Loading configuration from: {config}")
                try:
                    job_config = load_config(config)
                except ConfigVersionError as e:
                    print(f"❌ Configuration version error: {e}")
                    raise typer.Exit(1)

                # Apply CLI overrides if provided
                overrides = {
                    "batch_size": batch_size,
                    "output_path": output_path,
                    "workers": workers,
                    "provider": provider,
                    "feed_type": feed_type,
                }
                # Add symbols/start/end overrides if provided
                if symbols is not None:
                    overrides["symbols"] = [s.strip().upper() for s in symbols.split(",")]
                if start is not None:
                    overrides["start"] = datetime.fromisoformat(start).date()
                if end is not None:
                    overrides["end"] = datetime.fromisoformat(end).date()

                job_config = job_config.merge_overrides(**overrides)

            else:
                # Use direct flags - validate required fields
                if symbols is None or start is None or end is None:
                    print(
                        "❌ Error: Either provide --config file OR all of --symbols, --start, and --end"
                    )
                    raise typer.Exit(1)

                # Parse symbols and dates
                symbol_list = [s.strip().upper() for s in symbols.split(",")]
                start_date = datetime.fromisoformat(start).date()
                end_date = datetime.fromisoformat(end).date()

                # Build job config from CLI arguments
                job_config = IngestionJobConfig(
                    symbols=symbol_list,
                    start=start_date,
                    end=end_date,
                    batch_size=batch_size or 500,
                    output_path=output_path or "data/output",
                    workers=workers or 3,
                    provider=provider or "alpaca",
                    feed_type=feed_type or "iex",
                )

            # Display configuration summary
            print("📊 Ingestion Configuration:")
            print(f"  Symbols: {', '.join(job_config.symbols)}")
            print(f"  Date range: {job_config.start} to {job_config.end}")
            print(f"  Provider: {job_config.provider}")
            print(f"  Feed type: {job_config.feed_type}")
            print(f"  Output path: {job_config.output_path}")
            print(f"  Workers: {job_config.workers}")
            print(f"  Batch size: {job_config.batch_size}")

            # Build services
            print("\n🚀 Starting ingestion process...")

            # Build provider configuration
            provider_config = {
                "provider": job_config.provider,
                "feed_type": job_config.feed_type,
            }

            # Add provider-specific configuration (API keys, etc.)
            if job_config.provider == "alpaca":
                provider_config.update(
                    {
                        "api_key": os.getenv("ALPACA_KEY"),
                        "api_secret": os.getenv("ALPACA_SECRET"),
                        "base_url": "https://data.alpaca.markets/v2",
                        "rate_limit_per_min": 200,
                    }
                )
            elif job_config.provider == "fake":
                # Fake provider doesn't need credentials
                pass
            else:
                print(f"❌ Unsupported provider: {job_config.provider}")
                raise typer.Exit(1)

            job_service, coordinator_service = _build_ingestion_services(provider_config, job_config.output_path)

            # Create domain command
            command = CreateIngestionJobCommand(
                symbols=[Symbol(s) for s in job_config.symbols],
                time_range=TimeRange.from_dates(job_config.start, job_config.end),
                configuration=IngestionConfiguration(
                    output_path=Path(job_config.output_path),
                    compression="snappy",
                    max_workers=job_config.workers,
                    batch_size=job_config.batch_size,
                    rate_limit_per_minute=200,  # Default rate limit
                    feed_type=job_config.feed_type,
                ),
                batch_config=BatchConfiguration.default(),
            )

            async def run_ingestion():
                """Run the complete ingestion process in a single event loop."""
                try:
                    # Create job
                    print("📝 Creating ingestion job...")
                    job_id = await job_service.create_job(command)
                    print(f"✅ Created job: {job_id}")

                    # Execute job
                    print("⚡ Starting job execution...")
                    result = await coordinator_service.execute_job(job_id)

                    return job_id, result
                finally:
                    # Ensure proper cleanup of async resources
                    await _cleanup_async_resources(
                        job_service._job_repository,
                        job_service._checkpoint_repository,
                        job_service._metrics_repository,
                        coordinator_service._job_repository,
                        coordinator_service._checkpoint_repository,
                        coordinator_service._metrics_repository,
                    )

            # Run asyncio with clean error suppression
            job_id, result = asyncio.run(run_ingestion())

            # Report results
            print("✅ Job completed successfully!")
            print(f"📊 Job ID: {job_id}")
            print(f"📊 Symbols processed: {result.get('symbols_processed', 0)}")
            print(f"📊 Total bars: {result.get('total_bars', 0)}")
            print(f"⏱️  Processing time: {result.get('processing_time_seconds', 0):.2f}s")

            if result.get("symbols_failed", 0) > 0:
                print(f"⚠️  Failed symbols: {result.get('symbols_failed', 0)}")

            # Post-ingestion verification: check boundaries for each symbol
            print("\n🔍 Running post-ingestion verification...")
            for symbol in job_config.symbols:
                try:
                    _check_boundaries(
                        path=job_config.output_path,
                        symbol=symbol,
                        start=str(job_config.start),
                        end=str(job_config.end),
                        provider=job_config.provider,
                    )
                except SystemExit:
                    # _check_boundaries calls sys.exit(1) on failure
                    print(f"❌ Post-ingestion verification failed for {symbol}")
                    raise typer.Exit(1)

            print("✅ Post-ingestion verification completed successfully!")

        except Exception as e:
            print(f"❌ Ingestion failed: {e}")
            raise typer.Exit(1)


# NOTE: we disable Typer's default --help so that we can perform validation even when
# the caller includes a --help flag together with other options (the integration test
# suite relies on this behaviour).  We re-introduce our own --help flag that is NOT
# eager, so validation still runs first.

def ingest_ohlcv(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=False,
        file_okay=True,
        dir_okay=False,
    ),
    # Direct flag options (optional when using config)
    symbols: str = typer.Option(
        None, "--symbols", "-s", help="Comma-separated tickers, e.g. AAPL,MSFT"
    ),
    start: str = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    # Override options (work with both config and direct flags)
    batch_size: int = typer.Option(
        None,
        "--batch-size",
        help="Bars per request (overrides config)",
    ),
    output_path: Path = typer.Option(
        None,
        "--output",
        help="Output directory (overrides config)",
    ),
    workers: int = typer.Option(
        None,
        "--workers",
        help="Number of worker threads (overrides config)",
    ),
    provider: str = typer.Option(
        None,
        "--provider",
        help="Market data provider (overrides config)",
    ),
    feed_type: str = typer.Option(
        None,
        "--feed-type",
        help="Data feed type (overrides config)",
    ),
    help_flag: bool = typer.Option(
        False,
        "--help",
        "-h",
        is_flag=True,
        help="Show this message and exit",
        show_default=False,
    ),
):
    """Ingest OHLCV data from market data providers."""
    # Skip all validation and show help immediately if help flag is set
    if help_flag:
        help_text = """
Usage: ingest-ohlcv [OPTIONS]

Ingest OHLCV data from market data providers.

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
"""
        typer.echo(help_text.strip())
        raise typer.Exit(0)

    # -------------------------------------------------------------------------
    # Pre-flight validation ----------------------------------------------------
    # -------------------------------------------------------------------------
    valid_providers = {"alpaca", "fake"}

    if provider is not None and provider.lower() not in valid_providers:
        cli_error("invalid provider supplied")

    if provider == "alpaca" and feed_type is None:
        cli_error("feed type required when provider is alpaca")

    if feed_type is not None and feed_type not in {"iex", "sip"}:
        cli_error("invalid feed type")

    # Numeric / path validations
    validate_workers(workers)
    validate_batch_size(batch_size)
    validate_output_dir(output_path)
    validate_config_file(str(config) if config else None)

    # Date and symbol validation
    validate_date_range(start, end)
    validate_symbols(symbols)
    # All good – run the actual implementation
    _ingest_impl(
        config=config,
        symbols=symbols,
        start=start,
        end=end,
        batch_size=batch_size,
        output_path=str(output_path) if output_path else None,
        workers=workers,
        provider=provider,
        feed_type=feed_type,
    )


# Disable default help to keep behaviour identical to ingest_ohlcv (tests rely on this)
def ingest_ohlcv_convenience(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=False,
        file_okay=True,
        dir_okay=False,
    ),
    # Direct flag options (optional when using config)
    symbols: str = typer.Option(
        None, "--symbols", "-s", help="Comma-separated tickers, e.g. AAPL,MSFT"
    ),
    start: str = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    # Override options (work with both config and direct flags)
    batch_size: int = typer.Option(
        None,
        "--batch-size",
        help="Bars per request (overrides config)",
    ),
    output_path: Path = typer.Option(
        None,
        "--output",
        help="Output directory (overrides config)",
    ),
    workers: int = typer.Option(
        None,
        "--workers",
        help="Number of worker threads (overrides config)",
    ),
    provider: str = typer.Option(
        None, "--provider", help="Market data provider (overrides config)"
    ),
    feed_type: str = typer.Option(None, "--feed-type", help="Data feed type (overrides config)"),
    help_flag: bool = typer.Option(
        False,
        "--help",
        "-h",
        is_flag=True,
        help="Show this message and exit",
        show_default=False,
    ),
):
    """Convenience wrapper around ingest for simple CLI usage."""

    if help_flag:
        help_text = """
Usage: ingest-ohlcv [OPTIONS]

Convenience wrapper around ingest for simple CLI usage.

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
"""
        typer.echo(help_text.strip())
        raise typer.Exit(0)

    # -- validation -----------------------------------------------------------
    valid_providers = {"alpaca", "fake"}

    if provider is not None and provider.lower() not in valid_providers:
        cli_error("invalid provider supplied")

    if provider == "alpaca" and feed_type is None:
        cli_error("feed type required when provider is alpaca")

    if feed_type is not None and feed_type not in {"iex", "sip"}:
        cli_error("invalid feed type")

    validate_workers(workers)
    validate_batch_size(batch_size)
    validate_output_dir(output_path)
    validate_config_file(str(config) if config else None)
    validate_date_range(start, end)
    validate_symbols(symbols)

    _ingest_impl(
        config=config,
        symbols=symbols,
        start=start,
        end=end,
        batch_size=batch_size,
        output_path=str(output_path) if output_path else None,
        workers=workers,
        provider=provider,
        feed_type=feed_type,
    )


def ingest_deprecated(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=False,
        file_okay=True,
        dir_okay=False,
    ),
    # Direct flag options (optional when using config)
    symbols: str = typer.Option(
        None, "--symbols", "-s", help="Comma-separated tickers, e.g. AAPL,MSFT"
    ),
    start: str = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    # Override options (work with both config and direct flags)
    batch_size: int = typer.Option(
        None, "--batch-size", help="Bars per request (overrides config)"
    ),
    output_path: str = typer.Option(None, "--output", help="Output directory (overrides config)"),
    workers: int = typer.Option(
        None, "--workers", help="Number of worker threads (overrides config)"
    ),
    provider: str = typer.Option(
        None, "--provider", help="Market data provider (overrides config)"
    ),
    feed_type: str = typer.Option(None, "--feed-type", help="Data feed type (overrides config)"),
):
    """[DEPRECATED] Use 'ingest-ohlcv' or 'ohlcv ingest' instead."""
    print("⚠️  Warning: 'ingest' is deprecated. Use 'ingest-ohlcv' or 'ohlcv ingest' instead.")
    _ingest_impl(
        config=config,
        symbols=symbols,
        start=start,
        end=end,
        batch_size=batch_size,
        output_path=output_path,
        workers=workers,
        provider=provider,
        feed_type=feed_type,
    )

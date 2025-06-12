# SPDX-License-Identifier: Apache-2.0
"""OHLCV data ingestion commands."""

from __future__ import annotations

import os
import asyncio
from typing import Tuple
from datetime import datetime
from pathlib import Path

import typer

from marketpipe.domain.value_objects import Symbol, TimeRange
from marketpipe.ingestion.application.services import (
    IngestionJobService,
    IngestionCoordinatorService,
)
from marketpipe.ingestion.application.commands import CreateIngestionJobCommand
from marketpipe.ingestion.domain.services import (
    IngestionDomainService,
    IngestionProgressTracker,
)
from marketpipe.ingestion.domain.value_objects import (
    IngestionConfiguration,
    BatchConfiguration,
)
from marketpipe.ingestion.infrastructure.provider_loader import build_provider
from marketpipe.ingestion.infrastructure.repositories import (
    SqliteIngestionJobRepository,
    SqliteCheckpointRepository,
    SqliteMetricsRepository,
)
from marketpipe.infrastructure.repositories.sqlite_domain import (
    SqliteSymbolBarsRepository,
    SqliteOHLCVRepository,
)
from marketpipe.domain.events import InMemoryEventPublisher
from marketpipe.config import IngestionJobConfig
from marketpipe.events import IngestionJobCompleted
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.validation.domain.services import ValidationDomainService


def _build_ingestion_services(
    provider_config: dict = None,
) -> Tuple[IngestionJobService, IngestionCoordinatorService]:
    """Build and wire the DDD ingestion services with shared storage engine."""
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    # Shared storage engine for all contexts
    storage_engine = ParquetStorageEngine(data_dir / "raw")

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
            symbol = (
                bars[0].symbol.value
                if bars and hasattr(bars[0], "symbol")
                else "UNKNOWN"
            )
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
    
    try:
        # Determine configuration source and validate mutual exclusivity
        if config is not None:
            # Load from YAML file
            print(f"📄 Loading configuration from: {config}")
            job_config = IngestionJobConfig.from_yaml(config)

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

            # Parse and validate dates
            try:
                start_date = datetime.fromisoformat(start).date()
                end_date = datetime.fromisoformat(end).date()
            except ValueError as e:
                print(f"❌ Error: Invalid date format: {e}")
                print("💡 Use YYYY-MM-DD format, e.g., 2024-01-15")
                raise typer.Exit(1)

            # Parse symbols
            symbol_list = [s.strip().upper() for s in symbols.split(",")]

            # Create job configuration from flags
            job_config = IngestionJobConfig(
                symbols=symbol_list,
                start=start_date,
                end=end_date,
                batch_size=batch_size or 1000,
                output_path=output_path or "data/raw",
                workers=workers or 3,
                provider=provider or "alpaca",
                feed_type=feed_type or "iex",
            )

        # Validate configuration
        if len(job_config.symbols) == 0:
            print("❌ Error: No symbols specified")
            raise typer.Exit(1)

        if job_config.start >= job_config.end:
            print("❌ Error: Start date must be before end date")
            raise typer.Exit(1)

        # Display configuration summary
        print("📊 Ingestion Configuration:")
        print(f"  Symbols: {', '.join(job_config.symbols)}")
        print(f"  Date range: {job_config.start} to {job_config.end}")
        print(f"  Provider: {job_config.provider}")
        print(f"  Feed type: {job_config.feed_type}")
        print(f"  Output path: {job_config.output_path}")
        print(f"  Workers: {job_config.workers}")
        print(f"  Batch size: {job_config.batch_size}")
        print()

        # Build provider configuration for service creation
        provider_config = {
            "provider": job_config.provider,
            "api_key": os.getenv("ALPACA_KEY"),
            "api_secret": os.getenv("ALPACA_SECRET"),
            "base_url": "https://data.alpaca.markets/v2",
            "feed_type": job_config.feed_type,
            "rate_limit_per_min": 200,
        }

        # Build services
        job_service, coordinator_service = _build_ingestion_services(provider_config)

        # Create ingestion command
        command = CreateIngestionJobCommand(
            symbols=[Symbol(s) for s in job_config.symbols],
            time_range=TimeRange.from_dates(job_config.start, job_config.end),
            configuration=IngestionConfiguration(
                output_path=Path(job_config.output_path),
                compression="snappy",
                max_workers=job_config.workers,
                batch_size=job_config.batch_size,
                rate_limit_per_minute=provider_config["rate_limit_per_min"],
                feed_type=job_config.feed_type,
            ),
            batch_config=BatchConfiguration.default(),
        )

        # Create and execute job
        print("🚀 Creating ingestion job...")
        job_id = asyncio.run(job_service.create_job(command))
        print(f"📝 Created job: {job_id}")

        print("⚡ Starting job execution...")
        result = asyncio.run(coordinator_service.execute_job(job_id))

        # Report results
        print("✅ Job completed successfully!")
        print(f"📊 Job ID: {job_id}")
        print(f"📈 Symbols processed: {result.get('symbols_processed', 0)}")
        print(f"📊 Total bars: {result.get('total_bars', 0)}")
        print(f"⏱️  Processing time: {result.get('processing_time_seconds', 0):.2f}s")

        if result.get("symbols_failed", 0) > 0:
            print(f"⚠️  Failed symbols: {result.get('symbols_failed', 0)}")

    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        raise typer.Exit(1)


def ingest_ohlcv(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=True,
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
    output_path: str = typer.Option(
        None, "--output", help="Output directory (overrides config)"
    ),
    workers: int = typer.Option(
        None, "--workers", help="Number of worker threads (overrides config)"
    ),
    provider: str = typer.Option(
        None, "--provider", help="Market data provider (overrides config)"
    ),
    feed_type: str = typer.Option(
        None, "--feed-type", help="Data feed type (overrides config)"
    ),
):
    """Ingest OHLCV data from market data providers."""
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


def ingest_ohlcv_convenience(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=True,
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
    output_path: str = typer.Option(
        None, "--output", help="Output directory (overrides config)"
    ),
    workers: int = typer.Option(
        None, "--workers", help="Number of worker threads (overrides config)"
    ),
    provider: str = typer.Option(
        None, "--provider", help="Market data provider (overrides config)"
    ),
    feed_type: str = typer.Option(
        None, "--feed-type", help="Data feed type (overrides config)"
    ),
):
    """Ingest OHLCV data from market data providers (convenience command)."""
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


def ingest_deprecated(
    # Config file option
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to YAML configuration file",
        exists=True,
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
    output_path: str = typer.Option(
        None, "--output", help="Output directory (overrides config)"
    ),
    workers: int = typer.Option(
        None, "--workers", help="Number of worker threads (overrides config)"
    ),
    provider: str = typer.Option(
        None, "--provider", help="Market data provider (overrides config)"
    ),
    feed_type: str = typer.Option(
        None, "--feed-type", help="Data feed type (overrides config)"
    ),
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
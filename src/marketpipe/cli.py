# SPDX-License-Identifier: Apache-2.0
"""Command line interface for MarketPipe."""

from __future__ import annotations

import os
import asyncio
from typing import Tuple
from datetime import datetime
from pathlib import Path

import typer

from marketpipe.validation import ValidationRunnerService
ValidationRunnerService.register()

from marketpipe.aggregation import AggregationRunnerService
AggregationRunnerService.register()

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
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
from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter
from marketpipe.ingestion.infrastructure.repositories import (
    SqliteIngestionJobRepository,
    SqliteMetricsRepository,
)
from marketpipe.infrastructure.repositories.sqlite_domain import (
    SqliteSymbolBarsRepository,
    SqliteOHLCVRepository,
    SqliteCheckpointRepository,
)
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from marketpipe.domain.events import InMemoryEventPublisher
from marketpipe.config import IngestionJobConfig
from marketpipe.events import IngestionJobCompleted
from .metrics_server import run as metrics_server_run
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine

app = typer.Typer(add_completion=False, help="MarketPipe ETL commands")


def _build_ingestion_services() -> Tuple[IngestionJobService, IngestionCoordinatorService]:
    """Build and wire the DDD ingestion services with shared storage engine."""
    # Get configuration from environment
    api_key = os.getenv("ALPACA_KEY")
    api_secret = os.getenv("ALPACA_SECRET")
    
    if not api_key or not api_secret:
        raise ValueError(
            "Alpaca credentials not found. Please set ALPACA_KEY and ALPACA_SECRET environment variables."
        )
    
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Shared storage engine for all contexts
    storage_engine = ParquetStorageEngine(data_dir / "raw")
    
    # Infrastructure setup
    market_data_provider = AlpacaMarketDataAdapter(
        api_key=api_key,
        api_secret=api_secret,
        base_url="https://data.alpaca.markets/v2",
        feed_type="iex",
        rate_limit_per_min=200
    )
    
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
    
    # Application services
    job_service = IngestionJobService(
        job_repository=job_repo,
        checkpoint_repository=checkpoint_repo,
        metrics_repository=metrics_repo,
        domain_service=domain_service,
        progress_tracker=progress_tracker,
        event_publisher=event_publisher
    )
    
    coordinator_service = IngestionCoordinatorService(
        job_service=job_service,
        job_repository=job_repo,
        checkpoint_repository=checkpoint_repo,
        metrics_repository=metrics_repo,
        market_data_provider=market_data_provider,
        data_validator=None,  # TODO: Wire up validation
        data_storage=storage_engine,  # Use the new storage engine
        event_publisher=event_publisher
    )
    
    return job_service, coordinator_service


@app.command()
def ingest(
    # Config file option
    config: Path = typer.Option(
        None, 
        "--config", 
        "-c", 
        help="Path to YAML configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False
    ),
    # Direct flag options (optional when using config)
    symbols: str = typer.Option(None, "--symbols", "-s", help="Comma-separated tickers, e.g. AAPL,MSFT"),
    start: str = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    # Override options (work with both config and direct flags)
    batch_size: int = typer.Option(None, "--batch-size", help="Bars per request (overrides config)"),
    output_path: str = typer.Option(None, "--output", help="Output directory (overrides config)"),
    workers: int = typer.Option(None, "--workers", help="Number of worker threads (overrides config)"),
    provider: str = typer.Option(None, "--provider", help="Market data provider (overrides config)"),
    feed_type: str = typer.Option(None, "--feed-type", help="Data feed type (overrides config)"),
):
    """Start a new ingestion job and run it synchronously.
    
    Either use --config to load from YAML file, or specify --symbols, --start, and --end directly.
    CLI flags override config file values when both are provided.
    """
    try:
        # Determine configuration source and validate mutual exclusivity
        if config is not None:
            # Load from YAML file
            print(f"📄 Loading configuration from: {config}")
            job_config = IngestionJobConfig.from_yaml(config)
            
            # Apply CLI overrides if provided
            overrides = {
                'batch_size': batch_size,
                'output_path': output_path,
                'workers': workers,
                'provider': provider,
                'feed_type': feed_type,
            }
            # Add symbols/start/end overrides if provided
            if symbols is not None:
                overrides['symbols'] = [s.strip().upper() for s in symbols.split(",")]
            if start is not None:
                overrides['start'] = datetime.fromisoformat(start).date()
            if end is not None:
                overrides['end'] = datetime.fromisoformat(end).date()
                
            job_config = job_config.merge_overrides(**overrides)
            
        else:
            # Use direct flags - validate required fields
            if symbols is None or start is None or end is None:
                print("❌ Error: Either provide --config file OR all of --symbols, --start, and --end")
                print("💡 Examples:")
                print("   marketpipe ingest --config config.yaml")
                print("   marketpipe ingest --symbols AAPL,MSFT --start 2025-01-01 --end 2025-01-07")
                raise typer.Exit(1)
            
            # Build config from direct flags
            symbol_list = [s.strip().upper() for s in symbols.split(",")]
            start_date = datetime.fromisoformat(start).date()
            end_date = datetime.fromisoformat(end).date()
            
            job_config = IngestionJobConfig(
                symbols=symbol_list,
                start=start_date,
                end=end_date,
                batch_size=batch_size or 1000,
                output_path=output_path or "./data",
                workers=workers or 4,
                provider=provider or "alpaca",
                feed_type=feed_type or "iex",
            )
        
        # Convert config to domain objects
        symbol_list = [Symbol.from_string(s) for s in job_config.symbols]
        time_range = TimeRange.from_dates(job_config.start, job_config.end)
        
        # Build services
        job_service, coordinator_service = _build_ingestion_services()
        
        # Create ingestion configuration
        ingestion_config = IngestionConfiguration(
            output_path=Path(job_config.output_path),
            compression="snappy",
            max_workers=job_config.workers,
            batch_size=job_config.batch_size,
            rate_limit_per_minute=200,
            feed_type=job_config.feed_type
        )
        
        batch_config = BatchConfiguration.default()
        
        # Create ingestion job
        command = CreateIngestionJobCommand(
            symbols=symbol_list,
            time_range=time_range,
            configuration=ingestion_config,
            batch_config=batch_config
        )
        
        print(f"🚀 Creating ingestion job for {len(symbol_list)} symbols from {job_config.start} to {job_config.end}")
        print(f"   Provider: {job_config.provider} ({job_config.feed_type})")
        print(f"   Batch size: {job_config.batch_size}")
        print(f"   Workers: {job_config.workers}")
        print(f"   Output: {job_config.output_path}")
        
        # Create and execute job
        job_id = asyncio.run(job_service.create_job(command))
        print(f"📝 Created job: {job_id}")
        
        print("⚡ Starting job execution...")
        result = asyncio.run(coordinator_service.execute_job(job_id))
        
        # Report results
        print(f"✅ Job completed successfully!")
        print(f"   Symbols processed: {result.get('symbols_processed', 0)}")
        print(f"   Total bars: {result.get('total_bars', 0)}")
        print(f"   Files created: {result.get('files_created', 0)}")
        print(f"   Processing time: {result.get('processing_time_seconds', 0):.2f}s")
        
        if result.get('failed_symbols'):
            print(f"⚠️  Failed symbols: {', '.join(result['failed_symbols'])}")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        raise typer.Exit(1)
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        raise typer.Exit(1)


@app.command()
def validate(job_id: str):
    """Run validation manually for an existing job."""
    ValidationRunnerService.build_default().handle_ingestion_completed(
        IngestionJobCompleted(job_id)
    )
    print(f"✅ Validation completed for job: {job_id}")


@app.command()
def aggregate(job_id: str):
    """Run aggregation manually for a given ingestion job."""
    print(f"🔄 Starting aggregation for job: {job_id}")
    try:
        AggregationRunnerService.build_default().run_manual_aggregation(job_id)
        print(f"✅ Aggregation completed for job: {job_id}")
    except Exception as e:
        print(f"❌ Aggregation failed for job {job_id}: {e}")
        raise typer.Exit(1)


@app.command()
def metrics(port: int = typer.Option(8000, "--port", "-p", help="Port to run metrics server on")):
    """Start the Prometheus metrics server."""
    import tempfile
    
    # Set up multiprocess metrics directory if not already set
    if 'PROMETHEUS_MULTIPROC_DIR' not in os.environ:
        multiproc_dir = os.path.join(tempfile.gettempdir(), 'prometheus_multiproc')
        os.makedirs(multiproc_dir, exist_ok=True)
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = multiproc_dir
        print(f"📊 Multiprocess metrics enabled: {multiproc_dir}")
    
    print(f"📊 Starting metrics server on http://localhost:{port}/metrics")
    print("Press Ctrl+C to stop the server")
    
    try:
        metrics_server_run(port=port)
        # Keep the server running
        import time
        while True:
            time.sleep(1)
    except OSError as e:
        if e.errno == 98:  # Address already in use
            print(f"\n❌ Error: Port {port} is already in use!")
            print(f"💡 To find what's using the port: lsof -i :{port}")
            print(f"💡 To kill the process: kill <PID>")
            print(f"💡 Or try a different port: marketpipe metrics --port <other_port>")
            raise typer.Exit(1)
        else:
            raise  # Re-raise if it's a different OSError
    except KeyboardInterrupt:
        print("\n👋 Metrics server stopped")


if __name__ == "__main__":
    app()

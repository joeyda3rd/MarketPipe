"""Command line interface for MarketPipe."""

from __future__ import annotations

import os
import asyncio
from typing import Tuple
from datetime import datetime
from pathlib import Path

import typer

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
    SqliteCheckpointRepository,
    SqliteMetricsRepository,
)
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from marketpipe.domain.events import InMemoryEventPublisher
from .metrics_server import run as metrics_server_run

app = typer.Typer(add_completion=False, help="MarketPipe ETL commands")


def _build_ingestion_services() -> Tuple[IngestionJobService, IngestionCoordinatorService]:
    """Build and wire the DDD ingestion services."""
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
    
    # Infrastructure setup
    market_data_provider = AlpacaMarketDataAdapter(
        api_key=api_key,
        api_secret=api_secret,
        base_url="https://data.alpaca.markets/v2",
        feed_type="iex",
        rate_limit_per_min=200
    )
    
    # Repository setup
    job_repo = SqliteIngestionJobRepository(str(data_dir / "ingestion_jobs.db"))
    checkpoint_repo = SqliteCheckpointRepository(str(data_dir / "checkpoints.db"))
    metrics_repo = SqliteMetricsRepository(str(data_dir / "metrics.db"))
    data_storage = ParquetDataStorage(str(data_dir / "raw"))
    
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
        data_storage=data_storage,
        event_publisher=event_publisher
    )
    
    return job_service, coordinator_service


@app.command()
def ingest(
    symbols: str = typer.Option(..., "--symbols", "-s", help="Comma-separated tickers, e.g. AAPL,MSFT"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    batch_size: int = typer.Option(1000, "--batch-size", help="Bars per request"),
    output_path: str = typer.Option("./data", "--output", help="Output directory"),
    workers: int = typer.Option(4, "--workers", help="Number of worker threads"),
):
    """Start a new ingestion job and run it synchronously."""
    try:
        # Parse command line arguments
        symbol_list = [Symbol.from_string(s.strip().upper()) for s in symbols.split(",")]
        start_date = datetime.fromisoformat(start).date()
        end_date = datetime.fromisoformat(end).date()
        
        # Create time range from dates
        time_range = TimeRange.from_dates(start_date, end_date)
        
        # Build services
        job_service, coordinator_service = _build_ingestion_services()
        
        # Create ingestion configuration
        config = IngestionConfiguration(
            output_path=Path(output_path),
            compression="snappy",
            max_workers=workers,
            batch_size=batch_size,
            rate_limit_per_minute=200,
            feed_type="iex"
        )
        
        batch_config = BatchConfiguration.default()
        
        # Create ingestion job
        command = CreateIngestionJobCommand(
            symbols=symbol_list,
            time_range=time_range,
            configuration=config,
            batch_config=batch_config
        )
        
        print(f"🚀 Creating ingestion job for {len(symbol_list)} symbols from {start} to {end}")
        
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

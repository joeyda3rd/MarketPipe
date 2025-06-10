# SPDX-License-Identifier: Apache-2.0
"""Command line interface for MarketPipe."""

from __future__ import annotations

import os
import asyncio
from typing import Tuple, Optional, List
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
def validate(
    job_id: str = typer.Option(None, "--job-id", help="Re-run validation for job"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Path = typer.Option(None, "--show", help="Show a report CSV"),
):
    """Validate ingested data and manage validation reports.
    
    Examples:
        marketpipe validate --job-id abc123        # Re-run validation
        marketpipe validate --list                 # List all reports
        marketpipe validate --show path/to/report.csv  # Show specific report
    """
    from marketpipe.validation.infrastructure.repositories import CsvReportRepository
    
    reporter = CsvReportRepository()
    
    # Handle --list option
    if list_reports:
        reports = reporter.list_reports()
        if not reports:
            print("📋 No validation reports found")
            return
        
        print("📋 Validation Reports:")
        print("=" * 80)
        for i, report_path in enumerate(reports[:20]):  # Limit to 20 most recent
            try:
                summary = reporter.get_report_summary(report_path)
                # Extract job_id and symbol from path
                parts = report_path.parts
                if len(parts) >= 2:
                    job_id_from_path = parts[-2]  # Parent directory is job_id
                    filename = parts[-1]
                    symbol = filename.replace(f"{job_id_from_path}_", "").replace(".csv", "")
                else:
                    job_id_from_path = "unknown"
                    symbol = report_path.stem
                
                print(f"{i+1:2d}. Job: {job_id_from_path} | Symbol: {symbol}")
                print(f"    Path: {report_path}")
                print(f"    Errors: {summary['total_errors']}")
                print(f"    Modified: {report_path.stat().st_mtime}")
                print()
            except Exception as e:
                print(f"    Error reading {report_path}: {e}")
        
        if len(reports) > 20:
            print(f"... and {len(reports) - 20} more reports")
        return
    
    # Handle --show option
    if show:
        if not show.exists():
            print(f"❌ Report file not found: {show}")
            raise typer.Exit(1)
        
        try:
            df = reporter.load_report(show)
            summary = reporter.get_report_summary(show)
            
            print(f"📊 Validation Report: {show}")
            print("=" * 80)
            print(f"Total Errors: {summary['total_errors']}")
            print(f"Symbols: {', '.join(summary['symbols'])}")
            print()
            
            if summary['most_common_errors']:
                print("Most Common Errors:")
                for error in summary['most_common_errors']:
                    print(f"  • {error['reason']}: {error['count']} occurrences")
                print()
            
            if not df.empty:
                # Try to use rich for pretty display, fallback to standard
                try:
                    from rich.console import Console
                    from rich.table import Table
                    
                    console = Console()
                    table = Table(show_header=True, header_style="bold magenta")
                    
                    for col in df.columns:
                        table.add_column(col)
                    
                    # Show first 20 rows
                    for _, row in df.head(20).iterrows():
                        table.add_row(*[str(val) for val in row.values])
                    
                    console.print(table)
                    
                    if len(df) > 20:
                        console.print(f"... and {len(df) - 20} more rows")
                        
                except ImportError:
                    # Fallback to standard display
                    print("Validation Errors:")
                    print(df.head(20).to_string(index=False))
                    if len(df) > 20:
                        print(f"... and {len(df) - 20} more rows")
            else:
                print("✅ No validation errors found")
                
        except Exception as e:
            print(f"❌ Error reading report: {e}")
            raise typer.Exit(1)
        return
    
    # Handle --job-id option (re-run validation)
    if job_id:
        print(f"🔄 Re-running validation for job: {job_id}")
        try:
            ValidationRunnerService.build_default().handle_ingestion_completed(
                IngestionJobCompleted(job_id)
            )
            print(f"✅ Validation completed for job: {job_id}")
            
            # Show summary of reports created
            reports = reporter.list_reports(job_id)
            if reports:
                print(f"📊 Generated {len(reports)} validation reports:")
                for report in reports:
                    summary = reporter.get_report_summary(report)
                    print(f"  • {report.name}: {summary['total_errors']} errors")
        except Exception as e:
            print(f"❌ Validation failed for job {job_id}: {e}")
            raise typer.Exit(1)
        return
    
    # If no options provided, show help
    print("❌ Please specify an option:")
    print("  --job-id <id>     Re-run validation for a job")
    print("  --list            List available reports")
    print("  --show <path>     Show a specific report")
    print()
    print("Use 'marketpipe validate --help' for more information")
    raise typer.Exit(1)


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
def metrics(
    port: int = typer.Option(None, "--port", "-p", help="Port to run Prometheus metrics server"),
    metric: str = typer.Option(None, "--metric", "-m", help="Show specific metric history"),
    since: str = typer.Option(None, "--since", help="Show metrics since timestamp (e.g., '2024-01-01 10:00')"),
    avg: str = typer.Option(None, "--avg", help="Show average metrics over window (e.g., '1h', '1d')"),
    plot: bool = typer.Option(False, "--plot", help="Show ASCII sparkline plots"),
    list_metrics: bool = typer.Option(False, "--list", help="List available metrics"),
):
    """Start Prometheus metrics server or query historical metrics.
    
    Examples:
        marketpipe metrics --port 8080                      # Start metrics server
        marketpipe metrics --list                          # List available metrics
        marketpipe metrics --metric ingest_rows --since "2024-01-01" --plot
        marketpipe metrics --avg 1h                        # Show 1-hour averages
    """
    from marketpipe.metrics import SqliteMetricsRepository
    
    # If port is specified, start the metrics server
    if port is not None:
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
        return
    
    # Handle historical metrics queries
    try:
        repo = SqliteMetricsRepository()
        
        # Handle --list option
        if list_metrics:
            metrics_list = repo.list_metric_names()
            if not metrics_list:
                print("📊 No metrics found in database")
                return
            
            print("📊 Available Metrics:")
            print("=" * 50)
            for i, metric_name in enumerate(sorted(metrics_list), 1):
                print(f"{i:2d}. {metric_name}")
            print(f"\nTotal: {len(metrics_list)} metrics")
            return
        
        # Parse since timestamp if provided
        since_ts = None
        if since:
            try:
                from datetime import datetime
                since_ts = datetime.fromisoformat(since)
            except ValueError:
                print(f"❌ Invalid timestamp format: {since}")
                print("💡 Use format: 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'")
                raise typer.Exit(1)
        
        # Handle --avg option
        if avg:
            window_seconds = _parse_time_window(avg)
            if window_seconds is None:
                print(f"❌ Invalid time window: {avg}")
                print("💡 Use format: '1h', '2d', '30m', '1w'")
                raise typer.Exit(1)
            
            window_minutes = window_seconds // 60
            metrics_list = repo.list_metric_names()
            
            print(f"📊 Average Metrics (Window: {avg})")
            print("=" * 60)
            
            for metric_name in sorted(metrics_list)[:10]:  # Top 10 metrics
                try:
                    avg_value = asyncio.run(repo.get_average_metrics(metric_name, window_minutes=window_minutes))
                    if avg_value > 0:
                        print(f"{metric_name:30s}: {avg_value:>8.2f} (avg over {avg})")
                        
                        if plot:
                            # Simple ASCII bar for average - normalized to 0-40 chars
                            max_bar_length = 40
                            # Use log scale for better visualization
                            import math
                            if avg_value > 0:
                                bar_length = int(math.log10(avg_value + 1) * 8)
                                bar_length = min(bar_length, max_bar_length)
                            else:
                                bar_length = 0
                            bar = "█" * bar_length
                            print(f"{'':<30s}   {bar}")
                except Exception as e:
                    print(f"{metric_name:30s}: Error: {e}")
            
            return
        
        # Handle --metric option
        if metric:
            points = asyncio.run(repo.get_metrics_history(metric, since=since_ts))
            if not points:
                print(f"📊 No data found for metric: {metric}")
                return
            
            print(f"📊 Metric History: {metric}")
            print("=" * 60)
            
            if plot:
                # Create ASCII sparkline
                values = [p.value for p in points]
                sparkline = _create_sparkline(values)
                print(f"Sparkline: {sparkline}")
                print()
            
            # Show recent data points
            recent_points = points[-20:]  # Last 20 points
            for point in recent_points:
                timestamp_str = point.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                print(f"  {timestamp_str}: {point.value:.2f}")
            
            if len(points) > 20:
                print(f"... and {len(points) - 20} earlier points")
            
            # Show summary stats
            values = [p.value for p in points]
            print(f"\nSummary:")
            print(f"  Total points: {len(points)}")
            print(f"  Average: {sum(values) / len(values):.2f}")
            print(f"  Min: {min(values):.2f}")
            print(f"  Max: {max(values):.2f}")
            return
        
        # If no specific option, show recent metrics summary
        metrics_list = repo.list_metric_names()
        if not metrics_list:
            print("📊 No metrics found in database")
            print("💡 Try: marketpipe metrics --port 8000  # Start metrics server")
            return
        
        print("📊 Recent Metrics Summary")
        print("=" * 50)
        
        # Show latest value for each metric
        for metric_name in sorted(metrics_list)[:10]:  # Top 10 metrics
            points = asyncio.run(repo.get_metrics_history(metric_name, since=since_ts))
            if points:
                latest = points[0]
                timestamp_str = latest.timestamp.strftime("%Y-%m-%d %H:%M")
                print(f"{metric_name:30s}: {latest.value:>8.1f} ({timestamp_str})")
        
        if len(metrics_list) > 10:
            print(f"... and {len(metrics_list) - 10} more metrics")
        
        print(f"\n💡 Use --list to see all metrics")
        print(f"💡 Use --metric <name> to see history")
        print(f"💡 Use --avg 1h to see hourly averages")
        
    except Exception as e:
        print(f"❌ Error querying metrics: {e}")
        raise typer.Exit(1)


def _parse_time_window(window_str: str) -> Optional[int]:
    """Parse time window string to seconds."""
    import re
    
    match = re.match(r'(\d+)([smhdw])', window_str.lower())
    if not match:
        return None
    
    value, unit = match.groups()
    value = int(value)
    
    multipliers = {
        's': 1,           # seconds
        'm': 60,          # minutes
        'h': 3600,        # hours
        'd': 86400,       # days
        'w': 604800,      # weeks
    }
    
    return value * multipliers.get(unit, 1)


def _create_sparkline(values: List[float]) -> str:
    """Create ASCII sparkline from list of values."""
    if not values:
        return ""
    
    # Normalize values to 0-7 range for sparkline characters
    min_val = min(values)
    max_val = max(values)
    
    if min_val == max_val:
        return "▄" * len(values)  # Flat line
    
    # Sparkline characters from lowest to highest
    chars = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    
    # Normalize and map to characters
    normalized = []
    for val in values:
        norm = (val - min_val) / (max_val - min_val)
        char_index = min(int(norm * len(chars)), len(chars) - 1)
        normalized.append(chars[char_index])
    
    return "".join(normalized)


if __name__ == "__main__":
    app()

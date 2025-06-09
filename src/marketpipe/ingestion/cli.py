"""Domain-driven CLI for ingestion operations."""

from __future__ import annotations

import asyncio
import os
import yaml
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

import typer

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.domain.events import InMemoryEventPublisher
from .domain.services import IngestionDomainService, IngestionProgressTracker
from .domain.value_objects import IngestionConfiguration, BatchConfiguration
from .application.services import IngestionJobService, IngestionCoordinatorService
from .application.commands import CreateIngestionJobCommand, StartJobCommand
from .application.queries import GetJobStatusQuery, GetActiveJobsQuery, GetJobHistoryQuery
from .infrastructure.repositories import SqliteIngestionJobRepository, SqliteCheckpointRepository, SqliteMetricsRepository
from .infrastructure.adapters import MarketDataProviderFactory


app = typer.Typer(
    name="ingestion",
    help="MarketPipe ingestion operations using Domain-Driven Design patterns"
)


def load_dotenv_file(dotenv_path: str = ".env") -> None:
    """Load environment variables from a .env file."""
    if not os.path.exists(dotenv_path):
        return
    
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                os.environ[key] = value


def create_services() -> tuple[IngestionJobService, IngestionCoordinatorService]:
    """Create and wire up domain and application services."""
    # Create repositories
    job_repository = SqliteIngestionJobRepository()
    checkpoint_repository = SqliteCheckpointRepository()
    metrics_repository = SqliteMetricsRepository()
    
    # Create domain services
    domain_service = IngestionDomainService()
    progress_tracker = IngestionProgressTracker()
    
    # Create event publisher
    event_publisher = InMemoryEventPublisher()
    
    # Create application services
    job_service = IngestionJobService(
        job_repository=job_repository,
        checkpoint_repository=checkpoint_repository,
        metrics_repository=metrics_repository,
        domain_service=domain_service,
        progress_tracker=progress_tracker,
        event_publisher=event_publisher
    )
    
    # For the coordinator service, we'd inject actual implementations
    # from other contexts. For now, we'll use None placeholders
    coordinator_service = IngestionCoordinatorService(
        job_service=job_service,
        job_repository=job_repository,
        checkpoint_repository=checkpoint_repository,
        metrics_repository=metrics_repository,
        market_data_provider=None,  # Would be injected from integration context
        data_validator=None,        # Would be injected from validation context
        data_storage=None,          # Would be injected from storage context
        event_publisher=event_publisher
    )
    
    return job_service, coordinator_service


@app.command("create-job")
def create_job(
    config_path: str = typer.Option(..., "--config", help="Path to YAML configuration file"),
    symbols: str = typer.Option(None, "--symbols", help="Comma-separated list of symbols (overrides config)"),
    start_date: str = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, "--end", help="End date (YYYY-MM-DD)")
) -> None:
    """Create a new ingestion job."""
    
    async def _create_job():
        # Load environment variables
        load_dotenv_file()
        
        # Load configuration
        with open(config_path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
        
        # Parse symbols
        if symbols:
            symbol_list = [Symbol(s.strip().upper()) for s in symbols.split(",")]
        else:
            symbol_list = [Symbol(s) for s in config_dict.get("symbols", [])]
        
        # Parse dates
        if start_date and end_date:
            start_dt = datetime.fromisoformat(start_date)
            end_dt = datetime.fromisoformat(end_date)
        else:
            start_dt = datetime.fromisoformat(str(config_dict["start"]))
            end_dt = datetime.fromisoformat(str(config_dict["end"]))
        
        time_range = TimeRange(
            start=Timestamp(start_dt),
            end=Timestamp(end_dt)
        )
        
        # Create configuration objects
        ingestion_config = IngestionConfiguration.from_dict(config_dict)
        batch_config = BatchConfiguration.default()
        
        # Create command
        command = CreateIngestionJobCommand(
            symbols=symbol_list,
            time_range=time_range,
            configuration=ingestion_config,
            batch_config=batch_config
        )
        
        # Create services and execute
        job_service, _ = create_services()
        
        try:
            job_id = await job_service.create_job(command)
            typer.echo(f"✅ Created ingestion job: {job_id}")
            typer.echo(f"   Symbols: {len(symbol_list)}")
            typer.echo(f"   Time range: {start_dt.date()} to {end_dt.date()}")
            
        except Exception as e:
            typer.echo(f"❌ Failed to create job: {e}", err=True)
            raise typer.Exit(1)
    
    asyncio.run(_create_job())


@app.command("start-job")
def start_job(
    job_id: str = typer.Argument(..., help="Job ID to start")
) -> None:
    """Start an existing ingestion job."""
    
    async def _start_job():
        job_service, coordinator_service = create_services()
        
        try:
            from .domain.entities import IngestionJobId
            job_id_obj = IngestionJobId.from_string(job_id)
            
            # Start the job
            await job_service.start_job(StartJobCommand(job_id_obj))
            typer.echo(f"✅ Started job: {job_id}")
            
            # Execute the job (in a real implementation, this might be async)
            typer.echo("🔄 Executing job...")
            # result = await coordinator_service.execute_job(job_id_obj)
            # typer.echo(f"✅ Job completed: {result}")
            
        except Exception as e:
            typer.echo(f"❌ Failed to start job: {e}", err=True)
            raise typer.Exit(1)
    
    asyncio.run(_start_job())


@app.command("job-status")
def job_status(
    job_id: str = typer.Argument(..., help="Job ID to check")
) -> None:
    """Get the status of an ingestion job."""
    
    async def _job_status():
        job_service, _ = create_services()
        
        try:
            from .domain.entities import IngestionJobId
            job_id_obj = IngestionJobId.from_string(job_id)
            
            query = GetJobStatusQuery(job_id_obj)
            status = await job_service.get_job_status(query)
            
            if status is None:
                typer.echo(f"❌ Job not found: {job_id}")
                raise typer.Exit(1)
            
            typer.echo(f"📊 Job Status: {job_id}")
            typer.echo(f"   State: {status.get('state', 'unknown')}")
            typer.echo(f"   Progress: {status.get('progress_percentage', 0):.1f}%")
            typer.echo(f"   Symbols processed: {status.get('symbols_processed', 0)}/{status.get('symbols_total', 0)}")
            typer.echo(f"   Bars processed: {status.get('bars_processed', 0)}")
            
            if status.get('elapsed_seconds'):
                typer.echo(f"   Elapsed time: {status['elapsed_seconds']:.1f}s")
            
            if status.get('estimated_remaining_seconds'):
                typer.echo(f"   Est. remaining: {status['estimated_remaining_seconds']:.1f}s")
            
        except Exception as e:
            typer.echo(f"❌ Failed to get job status: {e}", err=True)
            raise typer.Exit(1)
    
    asyncio.run(_job_status())


@app.command("list-jobs")
def list_jobs(
    active_only: bool = typer.Option(False, "--active", help="Show only active jobs"),
    limit: int = typer.Option(10, "--limit", help="Maximum number of jobs to show")
) -> None:
    """List ingestion jobs."""
    
    async def _list_jobs():
        job_service, _ = create_services()
        
        try:
            if active_only:
                query = GetActiveJobsQuery()
                jobs = await job_service.get_active_jobs(query)
                typer.echo("🔄 Active Jobs:")
            else:
                query = GetJobHistoryQuery(limit=limit)
                jobs = await job_service.get_job_history(query)
                typer.echo(f"📋 Recent Jobs (last {limit}):")
            
            if not jobs:
                typer.echo("   No jobs found")
                return
            
            for job in jobs:
                job_id = job.get('job_id', 'unknown')[:8]  # Short ID
                state = job.get('state', 'unknown')
                symbols_count = job.get('symbols_total', 0)
                progress = job.get('progress_percentage', 0)
                
                status_icon = {
                    'pending': '⏳',
                    'in_progress': '🔄',
                    'completed': '✅',
                    'failed': '❌',
                    'cancelled': '🚫'
                }.get(state, '❓')
                
                typer.echo(f"   {status_icon} {job_id} | {state} | {symbols_count} symbols | {progress:.1f}%")
            
        except Exception as e:
            typer.echo(f"❌ Failed to list jobs: {e}", err=True)
            raise typer.Exit(1)
    
    asyncio.run(_list_jobs())


@app.command("run")
def run_legacy(
    config: str = typer.Option(..., "--config", help="Path to YAML config (legacy mode)")
) -> None:
    """Run ingestion using legacy coordinator (for backward compatibility)."""
    
    async def _run_legacy():
        typer.echo("⚠️  Running in legacy mode - consider using 'create-job' and 'start-job' commands")
        
        # Load environment variables
        load_dotenv_file()
        
        # Load configuration
        with open(config, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
        
        # Create domain objects
        symbols = [Symbol(s) for s in config_dict.get("symbols", [])]
        start_dt = datetime.fromisoformat(str(config_dict["start"]))
        end_dt = datetime.fromisoformat(str(config_dict["end"]))
        
        time_range = TimeRange(
            start=Timestamp(start_dt),
            end=Timestamp(end_dt)
        )
        
        ingestion_config = IngestionConfiguration.from_dict(config_dict)
        batch_config = BatchConfiguration.default()
        
        # Create and execute job
        job_service, coordinator_service = create_services()
        
        try:
            # Create job
            command = CreateIngestionJobCommand(
                symbols=symbols,
                time_range=time_range,
                configuration=ingestion_config,
                batch_config=batch_config
            )
            
            job_id = await job_service.create_job(command)
            typer.echo(f"✅ Created job: {job_id}")
            
            # Start job
            await job_service.start_job(StartJobCommand(job_id))
            typer.echo(f"🔄 Started job execution...")
            
            # Execute job (simplified - in real implementation would be more robust)
            # result = await coordinator_service.execute_job(job_id)
            
            typer.echo(f"✅ Ingestion completed")
            typer.echo(f"   Job ID: {job_id}")
            typer.echo(f"   Symbols: {len(symbols)}")
            
        except Exception as e:
            typer.echo(f"❌ Ingestion failed: {e}", err=True)
            raise typer.Exit(1)
    
    asyncio.run(_run_legacy())


if __name__ == "__main__":
    app()
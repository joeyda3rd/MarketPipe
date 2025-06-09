"""Ingestion application services."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from marketpipe.domain.value_objects import Symbol
from marketpipe.domain.events import IEventPublisher
from ..domain.entities import IngestionJob, IngestionJobId, ProcessingState
from ..domain.repositories import (
    IIngestionJobRepository, 
    IIngestionCheckpointRepository,
    IIngestionMetricsRepository,
    IngestionJobNotFoundError
)
from ..domain.services import IngestionDomainService, IngestionProgressTracker, JobCreationRequest
from ..domain.value_objects import IngestionPartition, ProcessingMetrics, IngestionCheckpoint
from ..domain.storage import IDataStorage
from .commands import (
    CreateIngestionJobCommand, 
    StartJobCommand, 
    CancelJobCommand,
    RestartJobCommand
)
from .queries import (
    GetJobStatusQuery,
    GetJobHistoryQuery, 
    GetActiveJobsQuery,
    GetJobMetricsQuery,
    GetJobProgressQuery
)


class IngestionJobService:
    """Application service for managing ingestion jobs."""
    
    def __init__(
        self,
        job_repository: IIngestionJobRepository,
        checkpoint_repository: IIngestionCheckpointRepository,
        metrics_repository: IIngestionMetricsRepository,
        domain_service: IngestionDomainService,
        progress_tracker: IngestionProgressTracker,
        event_publisher: IEventPublisher
    ):
        self._job_repository = job_repository
        self._checkpoint_repository = checkpoint_repository
        self._metrics_repository = metrics_repository
        self._domain_service = domain_service
        self._progress_tracker = progress_tracker
        self._event_publisher = event_publisher
    
    async def create_job(self, command: CreateIngestionJobCommand) -> IngestionJobId:
        """Create a new ingestion job."""
        # Validate against active jobs
        active_jobs = await self._job_repository.get_active_jobs()
        
        # Create job creation request
        request = JobCreationRequest(
            symbols=command.symbols,
            time_range=command.time_range,
            configuration=command.configuration,
            batch_config=command.batch_config
        )
        
        # Use domain service to create job with business logic
        job = self._domain_service.create_ingestion_job(request)
        
        # Validate schedule against active jobs
        validation_errors = self._domain_service.validate_job_schedule(job, active_jobs)
        if validation_errors:
            raise ValueError(f"Job scheduling conflicts: {'; '.join(validation_errors)}")
        
        # Save the job
        await self._job_repository.save(job)
        
        # Publish domain events
        for event in job.domain_events:
            await self._event_publisher.publish(event)
        
        return job.job_id
    
    async def start_job(self, command: StartJobCommand) -> None:
        """Start an ingestion job."""
        job = await self._job_repository.get_by_id(command.job_id)
        if not job:
            raise IngestionJobNotFoundError(command.job_id)
        
        # Start the job (domain logic handles state validation)
        job.start()
        
        # Save updated job
        await self._job_repository.save(job)
        
        # Publish domain events
        for event in job.domain_events:
            await self._event_publisher.publish(event)
    
    async def cancel_job(self, command: CancelJobCommand) -> None:
        """Cancel an ingestion job."""
        job = await self._job_repository.get_by_id(command.job_id)
        if not job:
            raise IngestionJobNotFoundError(command.job_id)
        
        # Cancel the job (domain logic handles state validation)
        job.cancel()
        
        # Save updated job
        await self._job_repository.save(job)
        
        # Publish domain events
        for event in job.domain_events:
            await self._event_publisher.publish(event)
    
    async def restart_job(self, command: RestartJobCommand) -> IngestionJobId:
        """Restart a failed or cancelled job."""
        original_job = await self._job_repository.get_by_id(command.job_id)
        if not original_job:
            raise IngestionJobNotFoundError(command.job_id)
        
        # Check if job can be restarted
        if not command.force and not self._domain_service.can_restart_job(original_job):
            raise ValueError(f"Job {command.job_id} cannot be restarted in state {original_job.state}")
        
        # Create new job with same configuration
        new_request = JobCreationRequest(
            symbols=original_job.symbols,
            time_range=original_job.time_range,
            configuration=original_job.configuration,
            batch_config=original_job.configuration  # Simplified for this example
        )
        
        new_job = self._domain_service.create_ingestion_job(new_request)
        await self._job_repository.save(new_job)
        
        # Publish events
        for event in new_job.domain_events:
            await self._event_publisher.publish(event)
        
        return new_job.job_id
    
    async def get_job_status(self, query: GetJobStatusQuery) -> Optional[Dict[str, Any]]:
        """Get the current status of a job."""
        job = await self._job_repository.get_by_id(query.job_id)
        if not job:
            return None
        
        # Get progress information
        progress = self._progress_tracker.calculate_job_progress(job)
        
        # Get throughput metrics if available
        throughput = self._progress_tracker.calculate_throughput_metrics(job)
        if throughput:
            progress.update(throughput)
        
        return progress
    
    async def get_job_history(self, query: GetJobHistoryQuery) -> List[Dict[str, Any]]:
        """Get job history with optional filtering."""
        if query.state_filter:
            jobs = await self._job_repository.get_by_state(query.state_filter)
        elif query.start_date and query.end_date:
            jobs = await self._job_repository.get_jobs_by_date_range(
                query.start_date, query.end_date
            )
        else:
            jobs = await self._job_repository.get_job_history(query.limit)
        
        return [job.get_processing_summary() for job in jobs]
    
    async def get_active_jobs(self, query: GetActiveJobsQuery) -> List[Dict[str, Any]]:
        """Get all currently active jobs."""
        jobs = await self._job_repository.get_active_jobs()
        return [
            {
                **job.get_processing_summary(),
                **self._progress_tracker.calculate_job_progress(job)
            }
            for job in jobs
        ]
    
    async def get_job_metrics(self, query: GetJobMetricsQuery) -> Optional[Dict[str, Any]]:
        """Get performance metrics for jobs."""
        if query.job_id:
            metrics = await self._metrics_repository.get_metrics(query.job_id)
            return metrics.to_dict() if metrics else None
        
        if query.start_date and query.end_date:
            avg_metrics = await self._metrics_repository.get_average_metrics(
                query.start_date, query.end_date
            )
            return avg_metrics.to_dict() if avg_metrics else None
        
        return None


class IngestionCoordinatorService:
    """
    Application service that coordinates the entire ingestion process.
    
    This service orchestrates:
    - Market data fetching via integration context
    - Data validation via validation context  
    - Data storage via storage context
    - Progress tracking via ingestion repositories
    """
    
    def __init__(
        self,
        job_service: IngestionJobService,
        job_repository: IIngestionJobRepository,
        checkpoint_repository: IIngestionCheckpointRepository,
        metrics_repository: IIngestionMetricsRepository,
        # These would be injected from other contexts
        market_data_provider,  # From integration context
        data_validator,        # From validation context
        data_storage: IDataStorage,  # From storage context
        event_publisher: IEventPublisher
    ):
        self._job_service = job_service
        self._job_repository = job_repository
        self._checkpoint_repository = checkpoint_repository
        self._metrics_repository = metrics_repository
        self._market_data_provider = market_data_provider
        self._data_validator = data_validator
        self._data_storage = data_storage
        self._event_publisher = event_publisher
    
    async def execute_job(self, job_id: IngestionJobId) -> Dict[str, Any]:
        """
        Execute an ingestion job end-to-end.
        
        This coordinates the entire process:
        1. Start the job
        2. Process symbols in parallel
        3. Handle checkpointing and recovery
        4. Collect metrics
        5. Complete or fail the job
        """
        # Get the job
        job = await self._job_repository.get_by_id(job_id)
        if not job:
            raise IngestionJobNotFoundError(job_id)
        
        # Start the job
        await self._job_service.start_job(StartJobCommand(job_id))
        
        start_time = datetime.now(timezone.utc)
        processed_symbols = 0
        failed_symbols = 0
        total_bars = 0
        
        try:
            # Process symbols in parallel using thread pool
            with ThreadPoolExecutor(max_workers=job.configuration.max_workers) as executor:
                # Submit all symbol processing tasks
                futures = {
                    executor.submit(self._process_symbol, job, symbol): symbol
                    for symbol in job.symbols
                }
                
                # Process completed tasks
                for future in as_completed(futures):
                    symbol = futures[future]
                    
                    try:
                        # Get the result
                        bars_count, partition = await asyncio.wrap_future(future)
                        
                        # Mark symbol as processed in the job
                        job = await self._job_repository.get_by_id(job_id)  # Refresh job state
                        job.mark_symbol_processed(symbol, bars_count, partition)
                        await self._job_repository.save(job)
                        
                        # Update metrics
                        processed_symbols += 1
                        total_bars += bars_count
                        
                        # Publish events
                        for event in job.domain_events:
                            await self._event_publisher.publish(event)
                        
                    except Exception as e:
                        # Log error and continue with other symbols
                        failed_symbols += 1
                        print(f"Failed to process symbol {symbol}: {e}")
                        # Could emit a domain event for symbol processing failure
            
            # Job should auto-complete when all symbols are processed
            # Calculate and save final metrics
            end_time = datetime.now(timezone.utc)
            processing_time = (end_time - start_time).total_seconds()
            
            metrics = ProcessingMetrics(
                symbols_processed=processed_symbols,
                symbols_failed=failed_symbols,
                total_bars_ingested=total_bars,
                total_processing_time_seconds=processing_time,
                average_processing_time_per_symbol=processing_time / max(1, processed_symbols)
            )
            
            await self._metrics_repository.save_metrics(job_id, metrics)
            
            return {
                "job_id": str(job_id),
                "status": "completed",
                "symbols_processed": processed_symbols,
                "symbols_failed": failed_symbols,
                "total_bars": total_bars,
                "processing_time_seconds": processing_time
            }
            
        except Exception as e:
            # Job failed - mark it as failed
            job = await self._job_repository.get_by_id(job_id)
            job.fail(str(e))
            await self._job_repository.save(job)
            
            # Publish failure events
            for event in job.domain_events:
                await self._event_publisher.publish(event)
            
            raise
    
    async def _process_symbol(
        self, 
        job: IngestionJob, 
        symbol: Symbol
    ) -> tuple[int, IngestionPartition]:
        """
        Process a single symbol.
        
        This method:
        1. Checks for existing checkpoint
        2. Fetches data from market data provider
        3. Validates the data
        4. Stores the data
        5. Updates checkpoint
        6. Returns results
        """
        # Check for existing checkpoint
        checkpoint = await self._checkpoint_repository.get_checkpoint(job.job_id, symbol)
        
        # Determine start point for data fetching
        start_timestamp = checkpoint.last_processed_timestamp if checkpoint else 0
        
        # Fetch data from market data provider (anti-corruption layer)
        bars = await self._market_data_provider.fetch_bars(
            symbol=symbol,
            start_timestamp=start_timestamp,
            end_timestamp=int(job.time_range.end.value.timestamp() * 1_000_000_000),
            batch_size=job.configuration.batch_size
        )
        
        if not bars:
            # No data to process
            return 0, IngestionPartition(
                symbol=symbol,
                file_path=job.configuration.output_path / f"{symbol.value}_empty.parquet",
                record_count=0,
                file_size_bytes=0,
                created_at=datetime.now(timezone.utc)
            )
        
        # Validate data using validation context
        validation_result = await self._data_validator.validate_bars(bars)
        if not validation_result.is_valid:
            raise ValueError(f"Data validation failed for {symbol}: {validation_result.errors}")
        
        # Store data using storage context
        partition = await self._data_storage.store_bars(bars, job.configuration)
        
        # Update checkpoint
        last_timestamp = max(bar.timestamp.value.timestamp() * 1_000_000_000 for bar in bars)
        new_checkpoint = IngestionCheckpoint(
            symbol=symbol,
            last_processed_timestamp=int(last_timestamp),
            records_processed=len(bars),
            updated_at=datetime.now(timezone.utc)
        )
        
        await self._checkpoint_repository.save_checkpoint(job.job_id, new_checkpoint)
        
        return len(bars), partition
"""Ingestion domain services."""

from __future__ import annotations

from typing import List, Optional, Set
from datetime import datetime, timezone
from dataclasses import dataclass

from marketpipe.domain.services import DomainService
from marketpipe.domain.value_objects import Symbol, TimeRange
from .entities import IngestionJob, IngestionJobId, ProcessingState
from .value_objects import IngestionConfiguration, BatchConfiguration, ProcessingMetrics
from .events import IngestionJobStarted


@dataclass
class JobCreationRequest:
    """Request for creating a new ingestion job."""
    symbols: List[Symbol]
    time_range: TimeRange
    configuration: IngestionConfiguration
    batch_config: BatchConfiguration


class IngestionDomainService(DomainService):
    """Domain service for ingestion business logic."""
    
    def create_ingestion_job(self, request: JobCreationRequest) -> IngestionJob:
        """
        Create a new ingestion job with validation of business rules.
        
        Args:
            request: Job creation request with symbols, time range, and configuration
            
        Returns:
            A new IngestionJob entity
            
        Raises:
            ValueError: If the request violates business rules
        """
        # Validate business rules
        self._validate_symbols(request.symbols)
        self._validate_time_range(request.time_range)
        self._validate_configuration(request.configuration)
        
        # Generate unique job ID
        job_id = IngestionJobId.generate()
        
        # Create the job entity
        job = IngestionJob(
            job_id=job_id,
            configuration=request.configuration,
            symbols=request.symbols,
            time_range=request.time_range
        )
        
        return job
    
    def can_restart_job(self, job: IngestionJob) -> bool:
        """
        Determine if a job can be restarted based on business rules.
        
        Args:
            job: The ingestion job to check
            
        Returns:
            True if the job can be restarted
        """
        # Can restart failed or cancelled jobs
        if job.state in (ProcessingState.FAILED, ProcessingState.CANCELLED):
            return True
        
        # Cannot restart completed or active jobs
        return False
    
    def calculate_optimal_batch_size(
        self, 
        symbols: List[Symbol], 
        available_workers: int,
        rate_limit_per_minute: Optional[int] = None
    ) -> int:
        """
        Calculate optimal batch size based on symbols count and constraints.
        
        Args:
            symbols: List of symbols to be processed
            available_workers: Number of available worker threads
            rate_limit_per_minute: API rate limit if applicable
            
        Returns:
            Optimal batch size for processing
        """
        symbol_count = len(symbols)
        
        # Start with even distribution across workers
        base_batch_size = max(1, symbol_count // available_workers)
        
        # Adjust for rate limiting if applicable
        if rate_limit_per_minute:
            # Ensure we don't exceed rate limits
            max_batch_size = max(1, rate_limit_per_minute // available_workers)
            base_batch_size = min(base_batch_size, max_batch_size)
        
        # Business rule: Keep batches manageable (max 50 symbols per batch)
        return min(base_batch_size, 50)
    
    def estimate_job_duration(
        self, 
        job: IngestionJob, 
        historical_metrics: Optional[ProcessingMetrics] = None
    ) -> Optional[float]:
        """
        Estimate job completion time based on historical performance.
        
        Args:
            job: The ingestion job to estimate
            historical_metrics: Historical performance data
            
        Returns:
            Estimated duration in seconds, or None if cannot estimate
        """
        if not historical_metrics:
            # Default estimation: 30 seconds per symbol
            return len(job.symbols) * 30.0
        
        # Use historical average processing time
        estimated_duration = len(job.symbols) * historical_metrics.average_processing_time_per_symbol
        
        # Add buffer for startup/coordination overhead (20%)
        return estimated_duration * 1.2
    
    def validate_job_schedule(
        self, 
        proposed_job: IngestionJob, 
        active_jobs: List[IngestionJob]
    ) -> List[str]:
        """
        Validate that a proposed job doesn't conflict with active jobs.
        
        Args:
            proposed_job: Job being scheduled
            active_jobs: Currently active jobs
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check for overlapping symbols in active jobs
        proposed_symbols = set(proposed_job.symbols)
        
        for active_job in active_jobs:
            if active_job.state == ProcessingState.IN_PROGRESS:
                active_symbols = set(active_job.symbols)
                overlapping = proposed_symbols & active_symbols
                
                if overlapping:
                    overlapping_list = [symbol.value for symbol in overlapping]
                    errors.append(
                        f"Symbols {overlapping_list} are already being processed "
                        f"by job {active_job.job_id}"
                    )
        
        # Business rule: Limit concurrent jobs to prevent resource exhaustion
        active_count = len([job for job in active_jobs 
                           if job.state == ProcessingState.IN_PROGRESS])
        
        if active_count >= 5:  # Maximum 5 concurrent jobs
            errors.append(
                f"Maximum concurrent jobs limit reached ({active_count}/5). "
                "Wait for existing jobs to complete."
            )
        
        return errors
    
    def determine_retry_strategy(
        self, 
        job: IngestionJob, 
        failure_reason: str
    ) -> tuple[bool, Optional[str]]:
        """
        Determine if and how a failed job should be retried.
        
        Args:
            job: The failed job
            failure_reason: Reason for failure
            
        Returns:
            Tuple of (should_retry, retry_strategy_description)
        """
        # Check for retryable failure types
        retryable_keywords = [
            "rate limit", "timeout", "connection", "temporary", 
            "network", "service unavailable"
        ]
        
        failure_lower = failure_reason.lower()
        is_retryable = any(keyword in failure_lower for keyword in retryable_keywords)
        
        if not is_retryable:
            return False, "Non-retryable error (data validation, authentication, etc.)"
        
        # Business rule: Only retry jobs that failed recently
        if job.failed_at:
            time_since_failure = datetime.now(timezone.utc) - job.failed_at
            if time_since_failure.total_seconds() > 3600:  # 1 hour
                return False, "Job failed too long ago for automatic retry"
        
        # Determine retry strategy based on failure type
        if "rate limit" in failure_lower:
            return True, "Retry with exponential backoff (rate limiting detected)"
        elif "timeout" in failure_lower:
            return True, "Retry with increased timeout values"
        else:
            return True, "Standard retry with backoff"
    
    def _validate_symbols(self, symbols: List[Symbol]) -> None:
        """Validate symbols list for business rules."""
        if not symbols:
            raise ValueError("Job must include at least one symbol")
        
        if len(symbols) > 1000:
            raise ValueError("Job cannot exceed 1000 symbols per batch")
        
        # Check for duplicates
        symbol_set = set(symbols)
        if len(symbol_set) != len(symbols):
            raise ValueError("Duplicate symbols are not allowed in a job")
        
        # Validate symbol format (basic check)
        for symbol in symbols:
            if not symbol.value or len(symbol.value) > 10:
                raise ValueError(f"Invalid symbol format: {symbol}")
    
    def _validate_time_range(self, time_range: TimeRange) -> None:
        """Validate time range for business rules."""
        # Check range duration
        duration = time_range.end.value - time_range.start.value
        
        # Business rule: Maximum 30 days per job to prevent resource exhaustion
        if duration.days > 30:
            raise ValueError("Ingestion jobs cannot span more than 30 days")
        
        # Business rule: Don't allow jobs for future dates
        now = datetime.now(timezone.utc)
        if time_range.start.value > now:
            raise ValueError("Cannot create jobs for future dates")
        
        # Business rule: Don't allow jobs for data older than 2 years (data availability)
        max_age_days = 730  # 2 years
        if (now - time_range.end.value).days > max_age_days:
            raise ValueError(f"Cannot create jobs for data older than {max_age_days} days")
    
    def _validate_configuration(self, config: IngestionConfiguration) -> None:
        """Validate configuration for business rules."""
        # Validate output path exists or can be created
        try:
            config.output_path.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot access output path {config.output_path}: {e}")
        
        # Business rule: Reasonable worker limits
        if config.max_workers > 20:
            raise ValueError("Maximum workers cannot exceed 20 (resource protection)")
        
        # Business rule: Reasonable batch sizes
        if config.batch_size > 10000:
            raise ValueError("Batch size cannot exceed 10,000 records (memory protection)")


class IngestionProgressTracker(DomainService):
    """Domain service for tracking ingestion progress and performance."""
    
    def calculate_job_progress(self, job: IngestionJob) -> dict:
        """Calculate detailed progress information for a job."""
        progress = {
            "job_id": str(job.job_id),
            "state": job.state.value,
            "progress_percentage": job.progress_percentage,
            "symbols_total": len(job.symbols),
            "symbols_processed": len(job.processed_symbols),
            "symbols_remaining": len(job.symbols) - len(job.processed_symbols),
            "bars_processed": job.total_bars_processed,
            "partitions_created": len(job.completed_partitions),
            "is_complete": job.is_complete
        }
        
        # Add timing information if available
        if job.started_at:
            now = datetime.now(timezone.utc)
            elapsed = (now - job.started_at).total_seconds()
            progress["elapsed_seconds"] = elapsed
            
            # Estimate remaining time
            if job.progress_percentage > 0:
                estimated_total = elapsed / (job.progress_percentage / 100.0)
                progress["estimated_remaining_seconds"] = estimated_total - elapsed
        
        return progress
    
    def calculate_throughput_metrics(self, job: IngestionJob) -> Optional[dict]:
        """Calculate throughput metrics for a job."""
        if not job.started_at or job.total_bars_processed == 0:
            return None
        
        now = datetime.now(timezone.utc)
        elapsed = (now - job.started_at).total_seconds()
        
        if elapsed <= 0:
            return None
        
        return {
            "bars_per_second": job.total_bars_processed / elapsed,
            "symbols_per_second": len(job.processed_symbols) / elapsed,
            "average_bars_per_symbol": (
                job.total_bars_processed / len(job.processed_symbols)
                if job.processed_symbols else 0
            )
        }
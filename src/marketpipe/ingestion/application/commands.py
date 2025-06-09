"""Ingestion application commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from marketpipe.domain.value_objects import Symbol, TimeRange
from ..domain.entities import IngestionJobId
from ..domain.value_objects import IngestionConfiguration, BatchConfiguration


@dataclass(frozen=True)
class CreateIngestionJobCommand:
    """Command to create a new ingestion job."""
    
    symbols: List[Symbol]
    time_range: TimeRange
    configuration: IngestionConfiguration
    batch_config: BatchConfiguration
    
    def __post_init__(self):
        """Validate command data."""
        if not self.symbols:
            raise ValueError("Symbols list cannot be empty")
        
        if len(self.symbols) > 1000:
            raise ValueError("Cannot create job with more than 1000 symbols")


@dataclass(frozen=True)
class StartJobCommand:
    """Command to start an ingestion job."""
    
    job_id: IngestionJobId


@dataclass(frozen=True)
class CancelJobCommand:
    """Command to cancel an ingestion job."""
    
    job_id: IngestionJobId
    reason: str = "User requested cancellation"


@dataclass(frozen=True)
class RestartJobCommand:
    """Command to restart a failed or cancelled job."""
    
    job_id: IngestionJobId
    force: bool = False  # Whether to restart even if job is not in failed/cancelled state


@dataclass(frozen=True)
class UpdateJobConfigurationCommand:
    """Command to update job configuration (only for pending jobs)."""
    
    job_id: IngestionJobId
    new_configuration: IngestionConfiguration
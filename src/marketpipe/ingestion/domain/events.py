"""Ingestion domain events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

from marketpipe.domain.events import DomainEvent
from marketpipe.domain.value_objects import Symbol, TimeRange
from .entities import IngestionJobId
from .value_objects import IngestionPartition


class IngestionJobStarted(DomainEvent):
    """Event raised when an ingestion job starts."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        symbols: List[Symbol],
        time_range: TimeRange,
        started_at: datetime
    ):
        super().__init__()
        self.job_id = job_id
        self.symbols = symbols
        self.time_range = time_range
        self.started_at = started_at
    
    @property
    def event_type(self) -> str:
        return "ingestion_job_started"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "symbols": [symbol.value for symbol in self.symbols],
            "time_range": {
                "start": self.time_range.start.value.isoformat(),
                "end": self.time_range.end.value.isoformat()
            },
            "started_at": self.started_at.isoformat()
        }


class IngestionJobCompleted(DomainEvent):
    """Event raised when an ingestion job completes successfully."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        symbols_processed: int,
        total_bars_processed: int,
        partitions_created: int,
        completed_at: datetime
    ):
        super().__init__()
        self.job_id = job_id
        self.symbols_processed = symbols_processed
        self.total_bars_processed = total_bars_processed
        self.partitions_created = partitions_created
        self.completed_at = completed_at
    
    @property
    def event_type(self) -> str:
        return "ingestion_job_completed"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "symbols_processed": self.symbols_processed,
            "total_bars_processed": self.total_bars_processed,
            "partitions_created": self.partitions_created,
            "completed_at": self.completed_at.isoformat()
        }


class IngestionJobFailed(DomainEvent):
    """Event raised when an ingestion job fails."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        error_message: str,
        failed_at: datetime,
        symbols_processed: int
    ):
        super().__init__()
        self.job_id = job_id
        self.error_message = error_message
        self.failed_at = failed_at
        self.symbols_processed = symbols_processed
    
    @property
    def event_type(self) -> str:
        return "ingestion_job_failed"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "error_message": self.error_message,
            "failed_at": self.failed_at.isoformat(),
            "symbols_processed": self.symbols_processed
        }


class IngestionJobCancelled(DomainEvent):
    """Event raised when an ingestion job is cancelled."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        cancelled_at: datetime,
        symbols_processed: int
    ):
        super().__init__()
        self.job_id = job_id
        self.cancelled_at = cancelled_at
        self.symbols_processed = symbols_processed
    
    @property
    def event_type(self) -> str:
        return "ingestion_job_cancelled"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "cancelled_at": self.cancelled_at.isoformat(),
            "symbols_processed": self.symbols_processed
        }


class IngestionBatchProcessed(DomainEvent):
    """Event raised when a batch (symbol) is processed."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        symbol: Symbol,
        bars_processed: int,
        partition: IngestionPartition,
        processed_at: datetime
    ):
        super().__init__()
        self.job_id = job_id
        self.symbol = symbol
        self.bars_processed = bars_processed
        self.partition = partition
        self.processed_at = processed_at
    
    @property
    def event_type(self) -> str:
        return "ingestion_batch_processed"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "symbol": self.symbol.value,
            "bars_processed": self.bars_processed,
            "partition": self.partition.get_storage_info(),
            "processed_at": self.processed_at.isoformat()
        }


class IngestionCheckpointSaved(DomainEvent):
    """Event raised when an ingestion checkpoint is saved."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        symbol: Symbol,
        checkpoint_timestamp: int,
        records_processed: int
    ):
        super().__init__()
        self.job_id = job_id
        self.symbol = symbol
        self.checkpoint_timestamp = checkpoint_timestamp
        self.records_processed = records_processed
    
    @property
    def event_type(self) -> str:
        return "ingestion_checkpoint_saved"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "symbol": self.symbol.value,
            "checkpoint_timestamp": self.checkpoint_timestamp,
            "records_processed": self.records_processed
        }


class IngestionRateLimited(DomainEvent):
    """Event raised when ingestion is rate limited."""
    
    def __init__(
        self,
        job_id: IngestionJobId,
        symbol: Symbol,
        delay_seconds: float,
        reason: str
    ):
        super().__init__()
        self.job_id = job_id
        self.symbol = symbol
        self.delay_seconds = delay_seconds
        self.reason = reason
    
    @property
    def event_type(self) -> str:
        return "ingestion_rate_limited"
    
    @property
    def aggregate_id(self) -> str:
        return str(self.job_id)
    
    def _get_event_data(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "symbol": self.symbol.value,
            "delay_seconds": self.delay_seconds,
            "reason": self.reason
        }
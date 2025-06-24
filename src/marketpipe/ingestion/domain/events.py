# SPDX-License-Identifier: Apache-2.0
"""Ingestion domain events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
from uuid import UUID, uuid4

from marketpipe.domain.events import DomainEvent
from marketpipe.domain.value_objects import Symbol, TimeRange

from .entities import IngestionJobId
from .value_objects import IngestionPartition


@dataclass(frozen=True)
class IngestionJobStarted(DomainEvent):
    """Event raised when an ingestion job starts."""

    job_id: IngestionJobId
    symbols: List[Symbol]
    time_range: TimeRange
    started_at: datetime
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
                "end": self.time_range.end.value.isoformat(),
            },
            "started_at": self.started_at.isoformat(),
        }


@dataclass(frozen=True)
class IngestionJobCompleted(DomainEvent):
    """Event raised when an ingestion job completes successfully."""

    job_id: IngestionJobId
    symbols_processed: int
    total_bars_processed: int
    partitions_created: int
    completed_at: datetime
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "completed_at": self.completed_at.isoformat(),
        }


@dataclass(frozen=True)
class IngestionJobFailed(DomainEvent):
    """Event raised when an ingestion job fails."""

    job_id: IngestionJobId
    error_message: str
    failed_at: datetime
    symbols_processed: int
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "symbols_processed": self.symbols_processed,
        }


@dataclass(frozen=True)
class IngestionJobCancelled(DomainEvent):
    """Event raised when an ingestion job is cancelled."""

    job_id: IngestionJobId
    cancelled_at: datetime
    symbols_processed: int
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "symbols_processed": self.symbols_processed,
        }


@dataclass(frozen=True)
class IngestionBatchProcessed(DomainEvent):
    """Event raised when a batch (symbol) is processed."""

    job_id: IngestionJobId
    symbol: Symbol
    bars_processed: int
    partition: IngestionPartition
    processed_at: datetime
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "partition": str(self.partition),  # Simplified for now
            "processed_at": self.processed_at.isoformat(),
        }


@dataclass(frozen=True)
class IngestionCheckpointSaved(DomainEvent):
    """Event raised when an ingestion checkpoint is saved."""

    job_id: IngestionJobId
    symbol: Symbol
    checkpoint_timestamp: int
    records_processed: int
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "records_processed": self.records_processed,
        }


@dataclass(frozen=True)
class IngestionRateLimited(DomainEvent):
    """Event raised when an ingestion is rate limited."""

    job_id: IngestionJobId
    symbol: Symbol
    delay_seconds: float
    reason: str
    event_id: UUID = None
    occurred_at: datetime = None
    version: int = 1

    def __post_init__(self):
        # Use object.__setattr__ for frozen dataclasses
        if self.event_id is None:
            object.__setattr__(self, "event_id", uuid4())
        if self.occurred_at is None:
            object.__setattr__(self, "occurred_at", datetime.now(timezone.utc))

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
            "reason": self.reason,
        }

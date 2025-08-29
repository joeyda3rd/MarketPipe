# SPDX-License-Identifier: Apache-2.0
"""Domain events for MarketPipe.

Domain events represent important business occurrences that other parts
of the system may need to react to. They enable loose coupling between
bounded contexts and support event-driven architecture patterns.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional, Protocol
from uuid import UUID, uuid4

from .value_objects import Symbol, Timestamp


class IEventBus(Protocol):
    """Protocol for event bus implementations.

    This interface defines the contract that any event bus implementation
    must follow, enabling dependency inversion in the domain layer.
    """

    def subscribe(self, etype: type[DomainEvent], fn: Callable[[DomainEvent], None]) -> None:
        """Subscribe a function to handle events of a specific type.

        Args:
            etype: The type of domain event to subscribe to
            fn: Function that will handle events of this type
        """
        ...

    def publish(self, event: DomainEvent) -> None:
        """Publish an event to all subscribers.

        Args:
            event: The domain event to publish
        """
        ...


class DomainEvent(ABC):
    """Base class for all domain events.

    Domain events represent significant business occurrences that have
    already happened and may be of interest to other parts of the system.
    """

    @property
    @abstractmethod
    def event_type(self) -> str:
        """Unique identifier for the event type."""
        pass

    @property
    @abstractmethod
    def aggregate_id(self) -> str:
        """Identifier of the aggregate that generated this event."""
        pass

    # Note: Concrete event classes typically define fields for event_id, occurred_at,
    # and version via dataclass declarations. We intentionally avoid declaring these
    # as abstract properties here to permit simple dataclass-based subclasses.

    @abstractmethod
    def _get_event_data(self) -> dict[str, Any]:
        """Get event-specific data for serialization.

        Returns:
            Dictionary of event-specific data
        """
        pass

    def __str__(self) -> str:
        """String representation of the event."""
        return f"{self.event_type}(id={self.event_id}, aggregate={self.aggregate_id})"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"{self.__class__.__name__}(event_id={self.event_id}, "
            f"occurred_at={self.occurred_at.isoformat()}, "
            f"aggregate_id={self.aggregate_id})"
        )


@dataclass(frozen=True)
class BarCollectionStarted(DomainEvent):
    """Event raised when bar collection begins for a symbol/date."""

    symbol: Symbol
    trading_date: date
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "bar_collection_started"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.trading_date.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True)
class BarCollectionCompleted(DomainEvent):
    """Event raised when bar collection for a symbol/date is complete."""

    symbol: Symbol
    trading_date: date
    bar_count: int
    has_gaps: bool = False
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "bar_collection_completed"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.trading_date.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "bar_count": self.bar_count,
            "has_gaps": self.has_gaps,
        }


@dataclass(frozen=True)
class ValidationFailed(DomainEvent):
    """Event raised when data validation fails."""

    symbol: Symbol
    timestamp: Timestamp
    error_message: str
    rule_id: Optional[str] = None
    severity: str = "error"
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "validation_failed"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.timestamp.trading_date().isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "timestamp": str(self.timestamp),
            "trading_date": self.timestamp.trading_date().isoformat(),
            "error_message": self.error_message,
            "rule_id": self.rule_id,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class IngestionJobStarted(DomainEvent):
    """Event raised when an ingestion job begins."""

    job_id: str
    symbol: Symbol
    trading_date: date
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "ingestion_job_started"

    @property
    def aggregate_id(self) -> str:
        return self.job_id

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True)
class IngestionJobCompleted(DomainEvent):
    """Event raised when ingestion job completes (successfully or with failure)."""

    job_id: str
    symbol: Symbol
    trading_date: date
    bars_processed: int
    success: bool
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "ingestion_job_completed"

    @property
    def aggregate_id(self) -> str:
        return self.job_id

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "bars_processed": self.bars_processed,
            "success": self.success,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True)
class MarketDataReceived(DomainEvent):
    """Event raised when raw market data is received from provider."""

    provider_id: str
    symbol: Symbol
    timestamp: Timestamp
    record_count: int
    data_feed: str
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "market_data_received"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.timestamp.trading_date().isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "symbol": str(self.symbol),
            "timestamp": str(self.timestamp),
            "trading_date": self.timestamp.trading_date().isoformat(),
            "record_count": self.record_count,
            "data_feed": self.data_feed,
        }


@dataclass(frozen=True)
class DataStored(DomainEvent):
    """Event raised when data has been successfully stored."""

    symbol: Symbol
    trading_date: date
    partition_path: str
    record_count: int
    file_size_bytes: int
    storage_format: str = "parquet"
    compression: str = "snappy"
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "data_stored"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.trading_date.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "partition_path": self.partition_path,
            "record_count": self.record_count,
            "file_size_bytes": self.file_size_bytes,
            "storage_format": self.storage_format,
            "compression": self.compression,
        }


@dataclass(frozen=True)
class RateLimitExceeded(DomainEvent):
    """Event raised when API rate limit is exceeded."""

    provider_id: str
    rate_limit_type: str
    current_usage: int
    limit_value: int
    reset_time: datetime
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "rate_limit_exceeded"

    @property
    def aggregate_id(self) -> str:
        return self.provider_id

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "rate_limit_type": self.rate_limit_type,
            "current_usage": self.current_usage,
            "limit_value": self.limit_value,
            "reset_time": self.reset_time.isoformat(),
        }


@dataclass(frozen=True)
class SymbolActivated(DomainEvent):
    """Event raised when a symbol is activated in the universe."""

    universe_id: str
    symbol: Symbol
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "symbol_activated"

    @property
    def aggregate_id(self) -> str:
        return self.universe_id

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "universe_id": self.universe_id,
            "symbol": str(self.symbol),
        }


@dataclass(frozen=True)
class SymbolDeactivated(DomainEvent):
    """Event raised when a symbol is deactivated in the universe."""

    universe_id: str
    symbol: Symbol
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "symbol_deactivated"

    @property
    def aggregate_id(self) -> str:
        return self.universe_id

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "universe_id": self.universe_id,
            "symbol": str(self.symbol),
        }


@dataclass(frozen=True)
class BackfillJobCompleted(DomainEvent):
    """Event raised when a per-symbol/day back-fill job completes successfully."""

    symbol: Symbol
    trading_date: date
    duration_seconds: float
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:  # noqa: D401
        return "backfill_job_completed"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.trading_date.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True)
class BackfillJobFailed(DomainEvent):
    """Event raised when a per-symbol/day back-fill job fails."""

    symbol: Symbol
    trading_date: date
    error_message: str
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:  # noqa: D401
        return "backfill_job_failed"

    @property
    def aggregate_id(self) -> str:
        return f"{self.symbol}_{self.trading_date.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "error": self.error_message,
        }


@dataclass(frozen=True)
class DataPruned(DomainEvent):
    """Event raised when data is pruned/deleted by retention policies."""

    data_type: str  # "parquet", "sqlite", etc.
    amount: int  # bytes for files, rows for database records
    cutoff: date  # cutoff date used for pruning
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    @property
    def event_type(self) -> str:
        return "data_pruned"

    @property
    def aggregate_id(self) -> str:
        return f"prune_{self.data_type}_{self.cutoff.isoformat()}"

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "data_type": self.data_type,
            "amount": self.amount,
            "cutoff": self.cutoff.isoformat(),
        }


# Event type registry for deserialization
EVENT_TYPE_REGISTRY = {
    "bar_collection_started": BarCollectionStarted,
    "bar_collection_completed": BarCollectionCompleted,
    "validation_failed": ValidationFailed,
    "ingestion_job_started": IngestionJobStarted,
    "ingestion_job_completed": IngestionJobCompleted,
    "market_data_received": MarketDataReceived,
    "data_stored": DataStored,
    "rate_limit_exceeded": RateLimitExceeded,
    "symbol_activated": SymbolActivated,
    "symbol_deactivated": SymbolDeactivated,
    "backfill_job_completed": BackfillJobCompleted,
    "backfill_job_failed": BackfillJobFailed,
    "data_pruned": DataPruned,
}


class IEventPublisher(ABC):
    """Interface for publishing domain events."""

    @abstractmethod
    async def publish(self, event: DomainEvent) -> None:
        """
        Publish a domain event.

        Args:
            event: The domain event to publish
        """
        pass

    @abstractmethod
    async def publish_many(self, events: list[DomainEvent]) -> None:
        """
        Publish multiple domain events.

        Args:
            events: List of domain events to publish
        """
        pass

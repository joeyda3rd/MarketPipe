# SPDX-License-Identifier: Apache-2.0
"""Ingestion domain entities."""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Set
from uuid import UUID, uuid4
import abc

from marketpipe.domain.entities import Entity, EntityId
from marketpipe.domain.events import DomainEvent
from marketpipe.domain.value_objects import Symbol, TimeRange
from .value_objects import IngestionConfiguration, IngestionPartition


class ProcessingState(Enum):
    """State of an ingestion job."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, eq=True, init=False)
class IngestionJobId:
    """Identifier for an ingestion job.

    This value object supports two representations for backwards-compatibility:

    1. **Composite key** – the preferred format which encodes the *symbol* and
       *trading day* using the pattern ``<SYMBOL>_<YYYY-MM-DD>``.
       Example: ``AAPL_2024-06-15``.

    2. **Opaque/legacy ID** – any arbitrary string (historically a UUID or a
       free-form text used by older tests). In this case the ``symbol`` and
       ``day`` properties return ``None``.
    """

    _raw: str

    def __init__(self, value_or_symbol: str | Symbol, day: str | None = None):
        """Create a new *IngestionJobId*.

        Args:
            value_or_symbol: Either a full identifier string *or* the symbol
                component when constructing a composite key.
            day: Optional trading day component (``YYYY-MM-DD``). If provided,
                a composite key will be created; otherwise *value_or_symbol*
                is treated as a fully-formed identifier.
        """

        # Resolve the raw identifier string that will be stored internally.
        if day is None:
            raw = str(value_or_symbol)
        else:
            # Ensure we have a valid ``Symbol`` instance for type safety.
            sym = value_or_symbol if isinstance(value_or_symbol, Symbol) else Symbol(str(value_or_symbol))
            raw = f"{sym}_{day}"

        object.__setattr__(self, "_raw", raw)

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------
    @classmethod
    def generate(cls) -> "IngestionJobId":
        """Generate a random opaque identifier (UUID4)."""
        return cls(str(uuid4()))

    @classmethod
    def from_string(cls, id_str: str) -> "IngestionJobId":
        """Create an *IngestionJobId* from its string representation."""
        return cls(id_str)

    # ------------------------------------------------------------------
    # Accessors / helpers
    # ------------------------------------------------------------------
    @property
    def value(self) -> str:  # Backwards compatibility alias
        """Return the underlying *raw* identifier string."""
        return self._raw

    # The repository now expects ``symbol`` and ``day`` attributes. These are
    # parsed on-demand and return *None* when the identifier does not conform
    # to the composite format.
    @property
    def symbol(self) -> Optional[Symbol]:
        """Extract the *symbol* component if available."""
        parts = self._raw.split("_", 1)
        return Symbol(parts[0]) if len(parts) == 2 else None

    @property
    def day(self) -> Optional[str]:
        """Extract the *day* (``YYYY-MM-DD``) component if available."""
        parts = self._raw.split("_", 1)
        return parts[1] if len(parts) == 2 else None

    # ------------------------------------------------------------------
    # String / representation helpers
    # ------------------------------------------------------------------
    def __str__(self) -> str:  # noqa: DunderStr
        return self._raw

    def __repr__(self) -> str:  # noqa: DunderRepr
        return f"IngestionJobId('{self._raw}')"


class IngestionJob(Entity):
    """
    Core entity representing an ingestion job.

    An ingestion job coordinates the collection of market data
    for a set of symbols over a specific time range.
    """

    def __init__(
        self,
        job_id: IngestionJobId,
        configuration: IngestionConfiguration,
        symbols: List[Symbol],
        time_range: TimeRange,
    ):
        super().__init__(EntityId.generate())
        self._job_id = job_id
        self._configuration = configuration
        self._symbols = symbols.copy()
        self._time_range = time_range
        self._state = ProcessingState.PENDING
        self._created_at = datetime.now(timezone.utc)
        self._started_at: Optional[datetime] = None
        self._completed_at: Optional[datetime] = None
        self._failed_at: Optional[datetime] = None
        self._error_message: Optional[str] = None
        self._processed_symbols: Set[Symbol] = set()
        self._completed_partitions: List[IngestionPartition] = []
        self._total_bars_processed = 0
        self._domain_events: List[DomainEvent] = []

        # Validate business rules
        self._validate_symbols()
        self._validate_time_range()

    @property
    def job_id(self) -> IngestionJobId:
        """Get the job identifier."""
        return self._job_id

    @property
    def configuration(self) -> IngestionConfiguration:
        """Get the ingestion configuration."""
        return self._configuration

    @property
    def symbols(self) -> List[Symbol]:
        """Get the symbols to be processed."""
        return self._symbols.copy()

    @property
    def time_range(self) -> TimeRange:
        """Get the time range for data collection."""
        return self._time_range

    @property
    def state(self) -> ProcessingState:
        """Get the current processing state."""
        return self._state

    @property
    def created_at(self) -> datetime:
        """Get when the job was created."""
        return self._created_at

    @property
    def started_at(self) -> Optional[datetime]:
        """Get when the job was started."""
        return self._started_at

    @property
    def completed_at(self) -> Optional[datetime]:
        """Get when the job was completed."""
        return self._completed_at

    @property
    def failed_at(self) -> Optional[datetime]:
        """Get when the job failed."""
        return self._failed_at

    @property
    def error_message(self) -> Optional[str]:
        """Get the error message if job failed."""
        return self._error_message

    @property
    def processed_symbols(self) -> Set[Symbol]:
        """Get the symbols that have been processed."""
        return self._processed_symbols.copy()

    @property
    def completed_partitions(self) -> List[IngestionPartition]:
        """Get the completed data partitions."""
        return self._completed_partitions.copy()

    @property
    def total_bars_processed(self) -> int:
        """Get the total number of bars processed."""
        return self._total_bars_processed

    @property
    def progress_percentage(self) -> float:
        """Calculate completion percentage based on symbols processed."""
        if not self._symbols:
            return 100.0
        return (len(self._processed_symbols) / len(self._symbols)) * 100.0

    @property
    def is_complete(self) -> bool:
        """Check if all symbols have been processed."""
        return len(self._processed_symbols) == len(self._symbols)

    @property
    def can_start(self) -> bool:
        """Check if the job can be started."""
        return self._state == ProcessingState.PENDING

    @property
    def can_complete(self) -> bool:
        """Check if the job can be marked as completed."""
        return self._state == ProcessingState.IN_PROGRESS and self.is_complete

    @property
    def can_fail(self) -> bool:
        """Check if the job can be marked as failed."""
        return self._state in (ProcessingState.PENDING, ProcessingState.IN_PROGRESS)

    @property
    def can_cancel(self) -> bool:
        """Check if the job can be cancelled."""
        return self._state in (ProcessingState.PENDING, ProcessingState.IN_PROGRESS)

    def start(self) -> None:
        """Start the ingestion job."""
        if not self.can_start:
            raise ValueError(f"Cannot start job in state {self._state}")

        self._state = ProcessingState.IN_PROGRESS
        self._started_at = datetime.now(timezone.utc)

        # Raise domain event
        from .events import IngestionJobStarted

        event = IngestionJobStarted(
            job_id=self._job_id,
            symbols=self._symbols,
            time_range=self._time_range,
            started_at=self._started_at,
        )
        self._add_domain_event(event)

    def complete(self) -> None:
        """Mark the job as completed."""
        if not self.can_complete:
            raise ValueError(f"Cannot complete job in state {self._state}")

        self._state = ProcessingState.COMPLETED
        self._completed_at = datetime.now(timezone.utc)

        # Raise domain event
        from .events import IngestionJobCompleted

        event = IngestionJobCompleted(
            job_id=self._job_id,
            symbols_processed=len(self._processed_symbols),
            total_bars_processed=self._total_bars_processed,
            partitions_created=len(self._completed_partitions),
            completed_at=self._completed_at,
        )
        self._add_domain_event(event)

    def fail(self, error_message: str) -> None:
        """Mark the job as failed with an error message."""
        if not self.can_fail:
            raise ValueError(f"Cannot fail job in state {self._state}")

        self._state = ProcessingState.FAILED
        self._failed_at = datetime.now(timezone.utc)
        self._error_message = error_message

        # Raise domain event
        from .events import IngestionJobFailed

        event = IngestionJobFailed(
            job_id=self._job_id,
            error_message=error_message,
            failed_at=self._failed_at,
            symbols_processed=len(self._processed_symbols),
        )
        self._add_domain_event(event)

    def cancel(self) -> None:
        """Cancel the job."""
        if not self.can_cancel:
            raise ValueError(f"Cannot cancel job in state {self._state}")

        self._state = ProcessingState.CANCELLED

        # Raise domain event
        from .events import IngestionJobCancelled

        event = IngestionJobCancelled(
            job_id=self._job_id,
            cancelled_at=datetime.now(timezone.utc),
            symbols_processed=len(self._processed_symbols),
        )
        self._add_domain_event(event)

    def mark_symbol_processed(
        self, symbol: Symbol, bars_count: int, partition: IngestionPartition
    ) -> None:
        """Mark a symbol as processed with its results."""
        if self._state != ProcessingState.IN_PROGRESS:
            raise ValueError(
                f"Cannot process symbol when job is in state {self._state}"
            )

        if symbol not in self._symbols:
            raise ValueError(f"Symbol {symbol} is not part of this job")

        if symbol in self._processed_symbols:
            raise ValueError(f"Symbol {symbol} already processed")

        self._processed_symbols.add(symbol)
        self._completed_partitions.append(partition)
        self._total_bars_processed += bars_count

        # Raise domain event
        from .events import IngestionBatchProcessed

        event = IngestionBatchProcessed(
            job_id=self._job_id,
            symbol=symbol,
            bars_processed=bars_count,
            partition=partition,
            processed_at=datetime.now(timezone.utc),
        )
        self._add_domain_event(event)

        # Auto-complete if all symbols processed
        if self.can_complete:
            self.complete()

    def estimate_remaining_time(
        self, average_processing_time_per_symbol: float
    ) -> Optional[float]:
        """Estimate remaining processing time in seconds."""
        if self._state != ProcessingState.IN_PROGRESS:
            return None

        remaining_symbols = len(self._symbols) - len(self._processed_symbols)
        return remaining_symbols * average_processing_time_per_symbol

    def get_processing_summary(self) -> dict:
        """Get a summary of processing results."""
        return {
            "job_id": str(self._job_id),
            "state": self._state.value,
            "symbols_total": len(self._symbols),
            "symbols_processed": len(self._processed_symbols),
            "bars_processed": self._total_bars_processed,
            "partitions_created": len(self._completed_partitions),
            "progress_percentage": self.progress_percentage,
            "created_at": self._created_at.isoformat(),
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "completed_at": (
                self._completed_at.isoformat() if self._completed_at else None
            ),
            "failed_at": self._failed_at.isoformat() if self._failed_at else None,
            "error_message": self._error_message,
        }

    @property
    def domain_events(self) -> List[DomainEvent]:
        """Get domain events for testing and publishing."""
        return self._domain_events.copy()

    def clear_domain_events(self) -> None:
        """Clear domain events (primarily for testing)."""
        self._domain_events.clear()

    def get_uncommitted_events(self) -> List[DomainEvent]:
        """Get domain events that haven't been published."""
        return self._domain_events.copy()

    def mark_events_committed(self) -> None:
        """Mark all events as committed (published)."""
        self._domain_events.clear()

    def _add_domain_event(self, event: DomainEvent) -> None:
        """Add a domain event to be published."""
        self._domain_events.append(event)

    def _validate_symbols(self) -> None:
        """Validate that symbols list is not empty and contains unique symbols."""
        if not self._symbols:
            raise ValueError("Ingestion job must have at least one symbol")

        if len(self._symbols) != len(set(self._symbols)):
            raise ValueError("Ingestion job cannot have duplicate symbols")

    def _validate_time_range(self) -> None:
        """Validate that the time range is reasonable for ingestion."""
        if self._time_range.start >= self._time_range.end:
            raise ValueError("Ingestion time range start must be before end")

        # Business rule: Don't allow ingestion jobs for future dates
        now = datetime.now(timezone.utc)
        if self._time_range.start.value > now:
            raise ValueError("Cannot create ingestion job for future dates")

    def __str__(self) -> str:
        return f"IngestionJob({self._job_id}, {len(self._symbols)} symbols, {self._state.value})"

    def __repr__(self) -> str:
        return (
            f"IngestionJob("
            f"job_id={self._job_id}, "
            f"symbols={len(self._symbols)}, "
            f"state={self._state.value}, "
            f"progress={self.progress_percentage:.1f}%"
            f")"
        )

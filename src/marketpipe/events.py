"""In-memory event bus and domain events."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, List, Type


class DomainEvent:
    """Base class for all domain events."""
    pass


Subscriber = Callable[[DomainEvent], None]


class EventBus:
    """Simple in-memory event bus for domain events."""
    
    _subs: Dict[Type[DomainEvent], List[Subscriber]] = defaultdict(list)

    @classmethod
    def subscribe(cls, etype: Type[DomainEvent], fn: Subscriber) -> None:
        """Subscribe a function to handle events of a specific type."""
        cls._subs[etype].append(fn)

    @classmethod
    def publish(cls, event: DomainEvent) -> None:
        """Publish an event to all subscribers."""
        for fn in cls._subs[type(event)]:
            fn(event)


class IngestionJobCompleted(DomainEvent):
    """Event raised when an ingestion job completes successfully."""
    
    def __init__(self, job_id: str):
        self.job_id = job_id


__all__ = ["DomainEvent", "EventBus", "IngestionJobCompleted"] 
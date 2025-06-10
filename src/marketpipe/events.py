# SPDX-License-Identifier: Apache-2.0
"""In-memory event bus and domain events."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, List, Type

# Import domain events from the domain layer
from .domain.events import (
    DomainEvent,
    BarCollectionStarted,
    BarCollectionCompleted,
    ValidationFailed,
    IngestionJobStarted,
    IngestionJobCompleted,
    MarketDataReceived,
    DataStored,
    RateLimitExceeded,
    SymbolActivated,
    SymbolDeactivated,
)


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


__all__ = [
    "DomainEvent", 
    "EventBus", 
    "BarCollectionStarted",
    "BarCollectionCompleted",
    "ValidationFailed",
    "IngestionJobStarted",
    "IngestionJobCompleted",
    "MarketDataReceived",
    "DataStored",
    "RateLimitExceeded",
    "SymbolActivated",
    "SymbolDeactivated",
] 
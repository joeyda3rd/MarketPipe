# SPDX-License-Identifier: Apache-2.0
"""In-memory event bus implementation.

This module provides a concrete implementation of the IEventBus protocol
using an in-memory message bus with simple synchronous delivery.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, List, Type

from marketpipe.domain.events import DomainEvent, IEventBus

Subscriber = Callable[[DomainEvent], None]


class InMemoryEventBus(IEventBus):
    """Simple in-memory event bus for domain events.

    This implementation uses class-level storage to maintain subscriptions
    across instances, preserving the singleton-like behavior of the original
    EventBus class while implementing the IEventBus protocol.
    """

    _subs: Dict[Type[DomainEvent], List[Subscriber]] = defaultdict(list)

    def subscribe(self, etype: Type[DomainEvent], fn: Subscriber) -> None:
        """Subscribe a function to handle events of a specific type.

        Args:
            etype: The type of domain event to subscribe to
            fn: Function that will handle events of this type
        """
        self._subs[etype].append(fn)

    def publish(self, event: DomainEvent) -> None:
        """Publish an event to all subscribers.

        Args:
            event: The domain event to publish
        """
        for fn in self._subs[type(event)]:
            fn(event)

    @classmethod
    def clear_subscriptions(cls) -> None:
        """Clear all subscriptions (useful for testing)."""
        cls._subs.clear()


# Legacy EventBus class for backward compatibility
class EventBus:
    """DEPRECATED: Legacy EventBus class.

    This class is deprecated. Use get_event_bus() from bootstrap.py to get
    an IEventBus instance, or inject IEventBus into your classes.
    """

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
    "InMemoryEventBus",
    "EventBus",  # Legacy class for backward compatibility
]

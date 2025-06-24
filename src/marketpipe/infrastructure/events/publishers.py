# SPDX-License-Identifier: Apache-2.0
"""Event publisher implementations for MarketPipe infrastructure.

This module contains concrete implementations of event publishers
that handle the technical aspects of event distribution and delivery.
"""

from __future__ import annotations

import asyncio
from typing import Dict, List

from marketpipe.domain.events import DomainEvent, IEventPublisher


class InMemoryEventPublisher(IEventPublisher):
    """
    Simple in-memory event publisher for development and testing.

    This publisher stores events in memory and provides basic
    event handling capabilities. For production use, consider
    implementing a persistent event store or message queue.
    """

    def __init__(self):
        self._events: List[DomainEvent] = []
        self._handlers: Dict[str, List] = {}

    async def publish(self, event: DomainEvent) -> None:
        """Publish a single domain event."""
        self._events.append(event)

        # Call registered handlers
        event_type = event.event_type
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        handler(event)
                except Exception as e:
                    # Log error but don't let handler failures break the publisher
                    print(f"Event handler error for {event_type}: {e}")

    async def publish_many(self, events: List[DomainEvent]) -> None:
        """Publish multiple domain events."""
        for event in events:
            await self.publish(event)

    def register_handler(self, event_type: str, handler) -> None:
        """Register an event handler for a specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def get_published_events(self) -> List[DomainEvent]:
        """Get all published events (useful for testing)."""
        return self._events.copy()

    def clear_events(self) -> None:
        """Clear all stored events (useful for testing)."""
        self._events.clear()

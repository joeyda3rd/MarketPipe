# SPDX-License-Identifier: Apache-2.0
"""Fake event publisher implementation for testing."""

from __future__ import annotations

from typing import Callable, Dict, List, Type

from marketpipe.domain.events import DomainEvent, IEventPublisher


class FakeEventPublisher(IEventPublisher):
    """Fake event publisher that captures events for testing."""

    def __init__(self):
        self._published_events: List[DomainEvent] = []
        self._handlers: Dict[str, List[Callable]] = {}
        self._publish_calls: List[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event."""
        self._published_events.append(event)
        self._publish_calls.append(event)

        # Call any registered handlers
        event_type = event.event_type
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                handler(event)

    async def publish_many(self, events: List[DomainEvent]) -> None:
        """Publish multiple domain events."""
        for event in events:
            await self.publish(event)

    def register_handler(self, event_type: str, handler: Callable) -> None:
        """Register an event handler for testing."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    # Test helpers
    def get_published_events(self) -> List[DomainEvent]:
        """Get all published events (for testing)."""
        return self._published_events.copy()

    def get_events_of_type(self, event_type: Type[DomainEvent]) -> List[DomainEvent]:
        """Get all published events of a specific type (for testing)."""
        return [event for event in self._published_events if isinstance(event, event_type)]

    def get_events_by_type_name(self, event_type_name: str) -> List[DomainEvent]:
        """Get all published events by type name (for testing)."""
        return [event for event in self._published_events if event.event_type == event_type_name]

    def has_event_of_type(self, event_type: Type[DomainEvent]) -> bool:
        """Check if any event of the specified type was published (for testing)."""
        return len(self.get_events_of_type(event_type)) > 0

    def get_event_count(self) -> int:
        """Get total number of published events (for testing)."""
        return len(self._published_events)

    def get_event_count_by_type(self, event_type: Type[DomainEvent]) -> int:
        """Get count of events of a specific type (for testing)."""
        return len(self.get_events_of_type(event_type))

    def clear_events(self) -> None:
        """Clear all captured events (for testing)."""
        self._published_events.clear()
        self._publish_calls.clear()

    def assert_event_published(self, event_type: Type[DomainEvent]) -> None:
        """Assert that an event of the specified type was published."""
        if not self.has_event_of_type(event_type):
            published_types = [event.event_type for event in self._published_events]
            raise AssertionError(
                f"Expected event of type {event_type.__name__} to be published. "
                f"Published events: {published_types}"
            )

    def assert_event_count(self, expected_count: int) -> None:
        """Assert the total number of published events."""
        actual_count = self.get_event_count()
        if actual_count != expected_count:
            raise AssertionError(
                f"Expected {expected_count} events to be published, "
                f"but {actual_count} were published"
            )

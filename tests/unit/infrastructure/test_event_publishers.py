"""Test event publishers functionality."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest
from marketpipe.domain.events import DomainEvent
from marketpipe.infrastructure.events.publishers import InMemoryEventPublisher


class MockDomainEvent(DomainEvent):
    """Mock domain event for testing."""

    def __init__(self, event_type: str = "test_event"):
        super().__init__()
        self._event_type = event_type
        self._aggregate_id = "test-aggregate-123"
        self._version = 1

    @property
    def event_type(self) -> str:
        return self._event_type

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def event_id(self) -> str:
        return str(self.id)

    @property
    def version(self) -> int:
        return self._version

    @property
    def occurred_at(self) -> str:
        return self.created_at.isoformat()

    def _get_event_data(self) -> dict:
        return {"test": "data"}


class TestInMemoryEventPublisher:
    """Test the InMemoryEventPublisher class."""

    def test_publisher_initialization(self):
        """Test publisher initializes correctly."""
        publisher = InMemoryEventPublisher()

        assert publisher._events == []
        assert publisher._handlers == {}

    @pytest.mark.asyncio
    async def test_publish_single_event(self):
        """Test publishing a single event."""
        publisher = InMemoryEventPublisher()
        event = MockDomainEvent("test_event")

        await publisher.publish(event)

        published_events = publisher.get_published_events()
        assert len(published_events) == 1
        assert published_events[0] == event
        assert published_events[0].event_type == "test_event"

    @pytest.mark.asyncio
    async def test_publish_many_events(self):
        """Test publishing multiple events."""
        publisher = InMemoryEventPublisher()
        events = [
            MockDomainEvent("event_1"),
            MockDomainEvent("event_2"),
            MockDomainEvent("event_3"),
        ]

        await publisher.publish_many(events)

        published_events = publisher.get_published_events()
        assert len(published_events) == 3
        assert published_events[0].event_type == "event_1"
        assert published_events[1].event_type == "event_2"
        assert published_events[2].event_type == "event_3"

    @pytest.mark.asyncio
    async def test_sync_event_handler(self):
        """Test registering and calling sync event handlers."""
        publisher = InMemoryEventPublisher()
        handler_mock = Mock()

        publisher.register_handler("test_event", handler_mock)

        event = MockDomainEvent("test_event")
        await publisher.publish(event)

        handler_mock.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_async_event_handler(self):
        """Test registering and calling async event handlers."""
        publisher = InMemoryEventPublisher()
        handler_called = False

        async def async_handler(event):
            nonlocal handler_called
            handler_called = True
            assert event.event_type == "test_event"

        publisher.register_handler("test_event", async_handler)

        event = MockDomainEvent("test_event")
        await publisher.publish(event)

        assert handler_called

    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_event_type(self):
        """Test multiple handlers for the same event type."""
        publisher = InMemoryEventPublisher()
        handler1_mock = Mock()
        handler2_mock = Mock()

        publisher.register_handler("test_event", handler1_mock)
        publisher.register_handler("test_event", handler2_mock)

        event = MockDomainEvent("test_event")
        await publisher.publish(event)

        handler1_mock.assert_called_once_with(event)
        handler2_mock.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_handler_exception_does_not_break_publisher(self):
        """Test that handler exceptions don't break the publisher."""
        publisher = InMemoryEventPublisher()

        def failing_handler(event):
            raise ValueError("Handler failed")

        def working_handler(event):
            working_handler.called = True

        working_handler.called = False

        publisher.register_handler("test_event", failing_handler)
        publisher.register_handler("test_event", working_handler)

        event = MockDomainEvent("test_event")
        await publisher.publish(event)

        # Event should still be published despite handler failure
        published_events = publisher.get_published_events()
        assert len(published_events) == 1

        # Working handler should still be called
        assert working_handler.called

    @pytest.mark.asyncio
    async def test_async_handler_exception_does_not_break_publisher(self):
        """Test that async handler exceptions don't break the publisher."""
        publisher = InMemoryEventPublisher()

        async def failing_async_handler(event):
            raise ValueError("Async handler failed")

        publisher.register_handler("test_event", failing_async_handler)

        event = MockDomainEvent("test_event")
        await publisher.publish(event)

        # Event should still be published despite handler failure
        published_events = publisher.get_published_events()
        assert len(published_events) == 1

    def test_get_published_events_returns_copy(self):
        """Test that get_published_events returns a copy."""
        publisher = InMemoryEventPublisher()

        # Get events before any are published
        events1 = publisher.get_published_events()
        assert events1 == []

        # Publish an event
        event = MockDomainEvent("test_event")
        asyncio.run(publisher.publish(event))

        # Original list should be unchanged
        assert events1 == []

        # New call should return the event
        events2 = publisher.get_published_events()
        assert len(events2) == 1

    def test_clear_events(self):
        """Test clearing published events."""
        publisher = InMemoryEventPublisher()

        # Publish some events
        events = [MockDomainEvent(f"event_{i}") for i in range(3)]
        asyncio.run(publisher.publish_many(events))

        # Verify events were published
        published_events = publisher.get_published_events()
        assert len(published_events) == 3

        # Clear events
        publisher.clear_events()

        # Verify events are cleared
        published_events = publisher.get_published_events()
        assert len(published_events) == 0

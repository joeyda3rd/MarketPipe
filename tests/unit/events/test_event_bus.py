# SPDX-License-Identifier: Apache-2.0
"""Unit tests for event bus."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from marketpipe.events import EventBus, DomainEvent


class SampleEvent(DomainEvent):
    """Test event implementation for testing event bus functionality."""

    def __init__(self, test_data: str = "test"):
        self.test_data = test_data
        self._event_id = uuid4()
        self._occurred_at = datetime.now(timezone.utc)
        self._version = 1

    @property
    def event_type(self) -> str:
        return "test_event"

    @property
    def aggregate_id(self) -> str:
        return "test_aggregate"

    @property
    def event_id(self) -> UUID:
        return self._event_id

    @property
    def occurred_at(self) -> datetime:
        return self._occurred_at

    @property
    def version(self) -> int:
        return self._version

    def _get_event_data(self) -> dict:
        return {"test_data": self.test_data}


def test_event_subscription_and_publishing():
    """Test event subscription and publishing."""
    hit = []
    EventBus.subscribe(SampleEvent, lambda e: hit.append(1))
    EventBus.publish(SampleEvent())
    assert hit == [1]


def test_multiple_subscribers():
    """Test multiple subscribers for same event type."""
    results = []

    def handler1(event):
        results.append("handler1")

    def handler2(event):
        results.append("handler2")

    EventBus.subscribe(SampleEvent, handler1)
    EventBus.subscribe(SampleEvent, handler2)
    EventBus.publish(SampleEvent())

    assert "handler1" in results
    assert "handler2" in results

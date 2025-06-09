"""Fake implementations for testing Domain-Driven Design patterns."""

from __future__ import annotations

from .repositories import (
    FakeIngestionJobRepository,
    FakeIngestionCheckpointRepository,
    FakeIngestionMetricsRepository
)
from .adapters import FakeMarketDataAdapter
from .events import FakeEventPublisher
from .validators import FakeDataValidator

__all__ = [
    "FakeIngestionJobRepository",
    "FakeIngestionCheckpointRepository", 
    "FakeIngestionMetricsRepository",
    "FakeMarketDataAdapter",
    "FakeEventPublisher",
    "FakeDataValidator",
]
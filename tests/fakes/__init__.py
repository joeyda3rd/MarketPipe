# SPDX-License-Identifier: Apache-2.0
"""Fake implementations for testing Domain-Driven Design patterns."""

from __future__ import annotations

from .adapters import FakeMarketDataAdapter
from .events import FakeEventPublisher
from .repositories import (
    FakeIngestionCheckpointRepository,
    FakeIngestionJobRepository,
    FakeIngestionMetricsRepository,
)
from .validators import FakeDataValidator

__all__ = [
    "FakeIngestionJobRepository",
    "FakeIngestionCheckpointRepository",
    "FakeIngestionMetricsRepository",
    "FakeMarketDataAdapter",
    "FakeEventPublisher",
    "FakeDataValidator",
]

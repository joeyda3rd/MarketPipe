# SPDX-License-Identifier: Apache-2.0
"""DEPRECATED: Legacy event bus module.

This module has been deprecated. The concrete EventBus has been moved to the
infrastructure layer. Domain events are now imported directly from the domain.

Use:
- from marketpipe.domain.events import DomainEvent, IEventBus, ...
- from marketpipe.bootstrap import get_event_bus
"""

from __future__ import annotations

import warnings

warnings.warn(
    "marketpipe.events is deprecated. "
    "Import domain events from marketpipe.domain.events and get event bus from bootstrap.get_event_bus()",
    DeprecationWarning,
    stacklevel=2
)

# Import domain events from the domain layer for backward compatibility
from .domain.events import (
    DomainEvent,
    IEventBus,
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

# Forward to infrastructure implementation for backward compatibility
from .infrastructure.messaging.in_memory_bus import EventBus


__all__ = [
    "DomainEvent",
    "IEventBus",
    "EventBus",  # Legacy - use get_event_bus() instead
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

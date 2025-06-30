# SPDX-License-Identifier: Apache-2.0
"""Validation domain events."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from marketpipe.events import DomainEvent

from .value_objects import ValidationResult


class ValidationCompleted(DomainEvent):
    """Event raised when validation completes for a symbol."""

    def __init__(self, result: ValidationResult):
        self.result = result
        self._event_id = uuid4()
        self._occurred_at = datetime.now(timezone.utc)
        self._version = 1

    @property
    def event_type(self) -> str:
        return "validation_completed"

    @property
    def aggregate_id(self) -> str:
        return str(self.result.symbol)

    @property
    def event_id(self) -> UUID:
        return self._event_id

    @property
    def occurred_at(self) -> datetime:
        return self._occurred_at

    @property
    def version(self) -> int:
        return self._version

    def _get_event_data(self) -> dict[str, Any]:
        return {
            "symbol": str(self.result.symbol),
            "total_bars": self.result.total,
            "error_count": len(self.result.errors),
            "has_errors": len(self.result.errors) > 0,
        }

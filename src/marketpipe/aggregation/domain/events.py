# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4
from typing import Dict, Any

from marketpipe.domain.events import DomainEvent


class AggregationCompleted(DomainEvent):
    """Event raised when aggregation is completed for a job."""

    def __init__(self, job_id: str, frames_processed: int):
        self.job_id = job_id
        self.frames_processed = frames_processed
        self._event_id = uuid4()
        self._occurred_at = datetime.now(timezone.utc)
        self._version = 1

    @property
    def event_type(self) -> str:
        return "aggregation_completed"

    @property
    def aggregate_id(self) -> str:
        return self.job_id

    @property
    def event_id(self) -> UUID:
        return self._event_id

    @property
    def occurred_at(self) -> datetime:
        return self._occurred_at

    @property
    def version(self) -> int:
        return self._version

    def _get_event_data(self) -> Dict[str, Any]:
        return {"job_id": self.job_id, "frames_processed": self.frames_processed}


class AggregationFailed(DomainEvent):
    """Event raised when aggregation fails for a job."""

    def __init__(self, job_id: str, error_message: str):
        self.job_id = job_id
        self.error_message = error_message
        self._event_id = uuid4()
        self._occurred_at = datetime.now(timezone.utc)
        self._version = 1

    @property
    def event_type(self) -> str:
        return "aggregation_failed"

    @property
    def aggregate_id(self) -> str:
        return self.job_id

    @property
    def event_id(self) -> UUID:
        return self._event_id

    @property
    def occurred_at(self) -> datetime:
        return self._occurred_at

    @property
    def version(self) -> int:
        return self._version

    def _get_event_data(self) -> Dict[str, Any]:
        return {"job_id": self.job_id, "error_message": self.error_message}

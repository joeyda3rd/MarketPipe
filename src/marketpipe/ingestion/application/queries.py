# SPDX-License-Identifier: Apache-2.0
"""Ingestion application queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..domain.entities import IngestionJobId, ProcessingState


@dataclass(frozen=True)
class GetJobStatusQuery:
    """Query to get the status of an ingestion job."""

    job_id: IngestionJobId


@dataclass(frozen=True)
class GetJobHistoryQuery:
    """Query to get job history with optional filtering."""

    limit: int = 100
    state_filter: ProcessingState | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


@dataclass(frozen=True)
class GetActiveJobsQuery:
    """Query to get all currently active jobs."""

    pass


@dataclass(frozen=True)
class GetJobMetricsQuery:
    """Query to get performance metrics for jobs."""

    job_id: IngestionJobId | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


@dataclass(frozen=True)
class GetJobProgressQuery:
    """Query to get detailed progress information for a job."""

    job_id: IngestionJobId


@dataclass(frozen=True)
class SearchJobsQuery:
    """Query to search jobs by various criteria."""

    symbols: list[str] | None = None
    state: ProcessingState | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    limit: int = 50

# SPDX-License-Identifier: Apache-2.0
"""Ingestion application queries."""

from __future__ import annotations
from typing import Optional

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
    state_filter: Optional[ProcessingState] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


@dataclass(frozen=True)
class GetActiveJobsQuery:
    """Query to get all currently active jobs."""

    pass


@dataclass(frozen=True)
class GetJobMetricsQuery:
    """Query to get performance metrics for jobs."""

    job_id: Optional[IngestionJobId] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


@dataclass(frozen=True)
class GetJobProgressQuery:
    """Query to get detailed progress information for a job."""

    job_id: IngestionJobId


@dataclass(frozen=True)
class SearchJobsQuery:
    """Query to search jobs by various criteria."""

    symbols: Optional[list[str]] = None
    state: Optional[ProcessingState] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    limit: int = 50

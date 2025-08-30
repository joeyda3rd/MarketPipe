# SPDX-License-Identifier: Apache-2.0
"""Ingestion domain module."""

from __future__ import annotations

from .entities import IngestionJob, IngestionJobId, ProcessingState
from .events import IngestionBatchProcessed, IngestionJobCompleted, IngestionJobStarted
from .repositories import IIngestionCheckpointRepository, IIngestionJobRepository
from .services import IngestionDomainService
from .storage import IDataStorage
from .value_objects import BatchConfiguration, IngestionConfiguration, IngestionPartition

__all__ = [
    # Entities
    "IngestionJob",
    "IngestionJobId",
    "ProcessingState",
    # Value Objects
    "IngestionConfiguration",
    "IngestionPartition",
    "BatchConfiguration",
    "IDataStorage",
    # Events
    "IngestionJobStarted",
    "IngestionJobCompleted",
    "IngestionBatchProcessed",
    # Repositories
    "IIngestionJobRepository",
    "IIngestionCheckpointRepository",
    # Services
    "IngestionDomainService",
]

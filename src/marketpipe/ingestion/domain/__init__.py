"""Ingestion domain module."""

from __future__ import annotations

from .entities import IngestionJob, IngestionJobId, ProcessingState
from .value_objects import IngestionConfiguration, IngestionPartition, BatchConfiguration
from .events import IngestionJobStarted, IngestionJobCompleted, IngestionBatchProcessed
from .repositories import IIngestionJobRepository, IIngestionCheckpointRepository
from .services import IngestionDomainService
from .storage import IDataStorage

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
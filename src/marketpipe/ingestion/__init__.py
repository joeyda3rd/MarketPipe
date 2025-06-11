# SPDX-License-Identifier: Apache-2.0
"""MarketPipe Ingestion (DDD)."""

from __future__ import annotations

# Domain-driven components
from .domain import (
    IngestionJob, IngestionJobId, ProcessingState,
    IngestionConfiguration, IngestionPartition, BatchConfiguration,
    IngestionJobStarted, IngestionJobCompleted, IngestionBatchProcessed,
    IIngestionJobRepository, IIngestionCheckpointRepository,
    IngestionDomainService
)
from .application import (
    IngestionCoordinatorService, IngestionJobService,
    CreateIngestionJobCommand, StartJobCommand, CancelJobCommand,
    GetJobStatusQuery, GetJobHistoryQuery
)
from .infrastructure import (
    AlpacaMarketDataAdapter,
    SqliteIngestionJobRepository, SqliteCheckpointRepository
)


__all__ = [
    # Domain layer
    "IngestionJob",
    "IngestionJobId", 
    "ProcessingState",
    "IngestionConfiguration",
    "IngestionPartition",
    "BatchConfiguration",
    "IngestionJobStarted",
    "IngestionJobCompleted", 
    "IngestionBatchProcessed",
    "IIngestionJobRepository",
    "IIngestionCheckpointRepository",
    "IngestionDomainService",
    
    # Application layer
    "IngestionCoordinatorService",
    "IngestionJobService",
    "CreateIngestionJobCommand",
    "StartJobCommand",
    "CancelJobCommand",
    "GetJobStatusQuery",
    "GetJobHistoryQuery",
    
    # Infrastructure layer
    "AlpacaMarketDataAdapter",
    "SqliteIngestionJobRepository",
    "SqliteCheckpointRepository",
]

# SPDX-License-Identifier: Apache-2.0
"""MarketPipe Ingestion (DDD)."""

from __future__ import annotations

from .application import (
    CancelJobCommand,
    CreateIngestionJobCommand,
    GetJobHistoryQuery,
    GetJobStatusQuery,
    IngestionCoordinatorService,
    IngestionJobService,
    StartJobCommand,
)

# Domain-driven components
from .domain import (
    BatchConfiguration,
    IIngestionCheckpointRepository,
    IIngestionJobRepository,
    IngestionBatchProcessed,
    IngestionConfiguration,
    IngestionDomainService,
    IngestionJob,
    IngestionJobCompleted,
    IngestionJobId,
    IngestionJobStarted,
    IngestionPartition,
    ProcessingState,
)
from .infrastructure import (
    AlpacaMarketDataAdapter,
    SqliteCheckpointRepository,
    SqliteIngestionJobRepository,
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

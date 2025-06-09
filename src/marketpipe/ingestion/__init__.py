"""Data ingestion with Domain-Driven Design patterns."""

from __future__ import annotations

# Legacy coordinator for backward compatibility
from .coordinator import IngestionCoordinator

# New domain-driven components
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


def ingest(config: str) -> None:
    """
    Run the ingestion pipeline from a YAML config (legacy mode).
    
    For new applications, consider using the domain-driven approach:
    - Create jobs using IngestionJobService
    - Execute using IngestionCoordinatorService
    - Monitor progress using application queries
    """
    coord = IngestionCoordinator(config)
    summary = coord.run()
    print(
        f"Ingested {summary['symbols']} symbols, {summary['rows']} rows, "
        f"wrote {summary['files']} parquet files."
    )


__all__ = [
    # Legacy support
    "IngestionCoordinator",
    "ingest",
    
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

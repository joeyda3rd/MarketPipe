"""Validation application services."""

from __future__ import annotations

from marketpipe.events import EventBus, IngestionJobCompleted
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from marketpipe.domain.value_objects import Symbol, TimeRange
from ..domain.services import ValidationDomainService
from ..infrastructure.repositories import CsvReportRepository


class ValidationRunnerService:
    """Application service for running validation on ingested data."""
    
    def __init__(self, loader, validator, reporter):
        self._loader = loader
        self._validator = validator
        self._reporter = reporter

    def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
        """Handle ingestion completion event by validating the data."""
        bars_by_symbol = self._loader.load_job_bars(event.job_id)
        for sym, bars in bars_by_symbol.items():
            result = self._validator.validate_bars(sym, bars)
            self._reporter.save(result)

    # Wiring helpers
    @classmethod
    def build_default(cls):
        """Build service with default dependencies."""
        return cls(
            loader=ParquetDataStorage("data/raw"),
            validator=ValidationDomainService(),
            reporter=CsvReportRepository(),
        )

    @classmethod
    def register(cls):
        """Register service to listen for ingestion completion events."""
        svc = cls.build_default()
        EventBus.subscribe(IngestionJobCompleted, svc.handle_ingestion_completed) 
"""Validation application services."""

from __future__ import annotations

from marketpipe.events import EventBus, IngestionJobCompleted
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.domain.value_objects import Symbol, TimeRange
from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Price, Volume, Timestamp
from ..domain.services import ValidationDomainService
from ..infrastructure.repositories import CsvReportRepository


class ValidationRunnerService:
    """Application service for running validation on ingested data."""
    
    def __init__(self, storage_engine, validator, reporter):
        self._storage_engine = storage_engine
        self._validator = validator
        self._reporter = reporter

    def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
        """Handle ingestion completion event by validating the data."""
        # Load DataFrames from storage engine
        symbol_dataframes = self._storage_engine.load_job_bars(event.job_id)
        
        for symbol_name, df in symbol_dataframes.items():
            # Convert DataFrame to domain objects
            bars = self._convert_dataframe_to_bars(df, symbol_name)
            
            # Validate using domain service
            result = self._validator.validate_bars(symbol_name, bars)
            
            # Save validation report
            self._reporter.save(result)

    def _convert_dataframe_to_bars(self, df, symbol_name: str) -> list:
        """Convert DataFrame to OHLCVBar domain objects."""
        bars = []
        symbol = Symbol.from_string(symbol_name)
        
        for _, row in df.iterrows():
            try:
                bar = OHLCVBar(
                    id=EntityId.generate(),
                    symbol=symbol,
                    timestamp=Timestamp.from_nanoseconds(int(row['ts_ns'])),
                    open_price=Price.from_float(float(row['open'])),
                    high_price=Price.from_float(float(row['high'])),
                    low_price=Price.from_float(float(row['low'])),
                    close_price=Price.from_float(float(row['close'])),
                    volume=Volume(int(row['volume']))
                )
                bars.append(bar)
            except Exception as e:
                # Skip invalid rows
                continue
        
        return bars

    # Wiring helpers
    @classmethod
    def build_default(cls):
        """Build service with default dependencies."""
        return cls(
            storage_engine=ParquetStorageEngine("data/raw"),
            validator=ValidationDomainService(),
            reporter=CsvReportRepository(),
        )

    @classmethod
    def register(cls):
        """Register service to listen for ingestion completion events."""
        svc = cls.build_default()
        EventBus.subscribe(IngestionJobCompleted, svc.handle_ingestion_completed) 
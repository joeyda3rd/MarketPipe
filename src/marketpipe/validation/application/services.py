# SPDX-License-Identifier: Apache-2.0
"""Validation application services."""

from __future__ import annotations

from marketpipe.bootstrap import get_event_bus
from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine

from ..domain.services import ValidationDomainService
from ..infrastructure.repositories import CsvReportRepository


class ValidationRunnerService:
    """Application service for running validation on ingested data."""

    def __init__(self, storage_engine, validator, reporter):
        self._storage_engine = storage_engine
        self._validator = validator
        self._reporter = reporter

    def _extract_provider_feed_info(self, event: IngestionJobCompleted) -> tuple[str, str]:
        """Extract provider and feed information from event or defaults."""
        # Try to get provider/feed from event attributes
        provider = getattr(event, "provider", "unknown")
        feed = getattr(event, "feed", "unknown")

        # If not available in event, try to infer from storage path or use defaults
        if provider == "unknown" or feed == "unknown":
            # For now, use defaults - in the future we could look up job configuration
            provider = "unknown"
            feed = "unknown"

        return provider, feed

    def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
        """Handle ingestion completion event by validating the data."""
        try:
            # Extract provider/feed info for metrics
            provider, feed = self._extract_provider_feed_info(event)

            # Record validation start metrics
            from marketpipe.metrics import record_metric

            record_metric("validation_jobs_started", 1, provider=provider, feed=feed)

            # Load DataFrames from storage engine
            symbol_dataframes = self._storage_engine.load_job_bars(event.job_id)

            total_errors = 0
            total_bars_validated = 0
            symbols_processed = 0

            for symbol_name, df in symbol_dataframes.items():
                try:
                    # Convert DataFrame to domain objects
                    bars = self._convert_dataframe_to_bars(df, symbol_name)
                    total_bars_validated += len(bars)

                    # Validate using domain service
                    result = self._validator.validate_bars(symbol_name, bars)

                    # Record validation metrics
                    error_count = len(result.errors)
                    total_errors += error_count

                    record_metric(
                        "validation_bars_processed", len(bars), provider=provider, feed=feed
                    )
                    record_metric(
                        f"validation_bars_{symbol_name}", len(bars), provider=provider, feed=feed
                    )

                    if error_count > 0:
                        record_metric(
                            "validation_errors_found", error_count, provider=provider, feed=feed
                        )
                        record_metric(
                            f"validation_errors_{symbol_name}",
                            error_count,
                            provider=provider,
                            feed=feed,
                        )
                        print(f"WARN Validation found {error_count} errors for {symbol_name}")
                    else:
                        record_metric("validation_success", 1, provider=provider, feed=feed)
                        record_metric(
                            f"validation_success_{symbol_name}", 1, provider=provider, feed=feed
                        )

                    # Save validation report with job_id
                    report_path = self._reporter.save(event.job_id, result)
                    print(f"INFO Validation report written: {report_path}")

                    symbols_processed += 1

                except Exception as symbol_error:
                    # Record symbol-specific validation failure
                    record_metric("validation_symbol_failures", 1, provider=provider, feed=feed)
                    record_metric(
                        f"validation_failure_{symbol_name}", 1, provider=provider, feed=feed
                    )
                    print(f"ERROR Failed to validate symbol {symbol_name}: {symbol_error}")

            # Record overall job validation metrics
            if total_errors == 0:
                record_metric("validation_jobs_success", 1, provider=provider, feed=feed)
                print(
                    f"INFO Validation completed successfully for job {event.job_id}: {symbols_processed} symbols, {total_bars_validated} bars"
                )
            else:
                record_metric("validation_jobs_with_errors", 1, provider=provider, feed=feed)
                record_metric("validation_total_errors", total_errors, provider=provider, feed=feed)
                print(
                    f"WARN Validation completed with {total_errors} total errors for job {event.job_id}"
                )

        except Exception as e:
            # Extract provider/feed for error metrics too
            provider, feed = self._extract_provider_feed_info(event)

            # Record overall validation failure
            from marketpipe.metrics import record_metric

            record_metric("validation_jobs_failed", 1, provider=provider, feed=feed)
            print(f"ERROR Validation failed for job {event.job_id}: {e}")
            raise

    def _convert_dataframe_to_bars(self, df, symbol_name: str) -> list:
        """Convert DataFrame to OHLCVBar domain objects."""
        bars = []
        symbol = Symbol.from_string(symbol_name)

        for _, row in df.iterrows():
            try:
                bar = OHLCVBar(
                    id=EntityId.generate(),
                    symbol=symbol,
                    timestamp=Timestamp.from_nanoseconds(int(row["ts_ns"])),
                    open_price=Price.from_float(float(row["open"])),
                    high_price=Price.from_float(float(row["high"])),
                    low_price=Price.from_float(float(row["low"])),
                    close_price=Price.from_float(float(row["close"])),
                    volume=Volume(int(row["volume"])),
                )
                bars.append(bar)
            except Exception:
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
        get_event_bus().subscribe(IngestionJobCompleted, svc.handle_ingestion_completed)

# SPDX-License-Identifier: Apache-2.0
"""OHLCV data aggregation commands."""

from __future__ import annotations

import asyncio

import typer

from marketpipe.aggregation.application.services import AggregationRunnerService
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


def _aggregate_impl(job_id: str):
    """Implementation of the aggregate functionality."""
    from marketpipe.bootstrap import bootstrap
    bootstrap()
    
    try:
        print(f"📊 Starting aggregation for job: {job_id}")

        # Setup aggregation service - use the build_default factory method
        aggregation_service = AggregationRunnerService.build_default()

        # Create and trigger the ingestion completed event  
        from marketpipe.domain.events import IngestionJobCompleted
        from marketpipe.domain.value_objects import Symbol
        from datetime import date
        
        # Create a mock event to trigger aggregation
        # Use dummy values since this is just to trigger the aggregation
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("DUMMY"),  # Placeholder symbol
            trading_date=date.today(),
            bars_processed=0,
            success=True
        )

        try:
            # Execute aggregation via event handler
            aggregation_service.handle_ingestion_completed(event)
            print("✅ All aggregations completed successfully!")
            print("📄 Check 'data/aggregated/' for aggregated data")
            
            # Refresh DuckDB views
            try:
                from marketpipe.aggregation.infrastructure.duckdb_views import refresh_views
                refresh_views()
                print("🔄 DuckDB views refreshed")
            except Exception as e:
                print(f"⚠️  Warning: Failed to refresh DuckDB views: {e}")
        except Exception as e:
            print(f"❌ Aggregation failed: {e}")
            raise typer.Exit(1)

    except Exception as e:
        print(f"❌ Aggregation failed: {e}")
        raise typer.Exit(1)


def aggregate_ohlcv(job_id: str):
    """Aggregate OHLCV data to multiple timeframes."""
    _aggregate_impl(job_id)


def aggregate_ohlcv_convenience(job_id: str):
    """Aggregate OHLCV data to multiple timeframes (convenience command)."""
    _aggregate_impl(job_id)


def aggregate_deprecated(job_id: str):
    """[DEPRECATED] Use 'aggregate-ohlcv' or 'ohlcv aggregate' instead."""
    print("⚠️  Warning: 'aggregate' is deprecated. Use 'aggregate-ohlcv' or 'ohlcv aggregate' instead.")
    _aggregate_impl(job_id) 
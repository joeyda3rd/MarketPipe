"""Tests for symbol pipeline Prometheus metrics."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY, generate_latest
from unittest.mock import Mock, patch
import datetime as dt
from pathlib import Path

from marketpipe.metrics import (
    SYMBOLS_ROWS,
    SYMBOLS_SNAPSHOT_RECORDS,
    SYMBOLS_NULL_RATIO,
)
from marketpipe.ingestion.pipeline.symbol_pipeline import run_symbol_pipeline, _update_null_ratio_metrics


class TestSymbolsMetrics:
    """Test symbol pipeline metrics collection."""

    def setup_method(self):
        """Clear metrics before each test."""
        # Note: Prometheus counters cannot be cleared/reset, 
        # so we'll work with incremental values in tests

    def test_metrics_are_registered(self):
        """Test that all symbol metrics are registered with Prometheus."""
        metrics_output = generate_latest(REGISTRY).decode()
        
        # Check that metric names appear in the output
        assert "mp_symbols_rows_total" in metrics_output
        assert "mp_symbols_snapshot_records_total" in metrics_output
        assert "mp_symbols_null_ratio" in metrics_output

    def test_snapshot_records_counter(self):
        """Test snapshot records counter increments correctly."""
        # Get initial value
        initial_output = generate_latest(REGISTRY).decode()
        initial_lines = [line for line in initial_output.split('\n') 
                        if 'mp_symbols_snapshot_records_total' in line and not line.startswith('#')]
        initial_value = 0.0
        if initial_lines:
            initial_value = float(initial_lines[0].split()[-1])
        
        # Record some snapshot records
        SYMBOLS_SNAPSHOT_RECORDS.inc(150)
        SYMBOLS_SNAPSHOT_RECORDS.inc(75)
        
        # Check the metric value increased
        metrics_output = generate_latest(REGISTRY).decode()
        lines = [line for line in metrics_output.split('\n') 
                if 'mp_symbols_snapshot_records_total' in line and not line.startswith('#')]
        assert len(lines) > 0, "Snapshot records metric not found"
        
        new_value = float(lines[0].split()[-1])
        assert new_value >= initial_value + 225.0, f"Expected value to increase by 225, got {new_value - initial_value}"

    def test_symbols_rows_counter_with_labels(self):
        """Test symbols rows counter with action labels."""
        # Record insert and update operations
        SYMBOLS_ROWS.labels(action="insert").inc(50)
        SYMBOLS_ROWS.labels(action="update").inc(25)
        SYMBOLS_ROWS.labels(action="insert").inc(10)  # Additional inserts
        
        # Check the metric values
        metrics_output = generate_latest(REGISTRY).decode()
        assert 'mp_symbols_rows_total{action="insert"}' in metrics_output
        assert 'mp_symbols_rows_total{action="update"}' in metrics_output

    def test_null_ratio_gauge_with_columns(self):
        """Test null ratio gauge with column labels."""
        # Set null ratios for different columns
        SYMBOLS_NULL_RATIO.labels(column="figi").set(0.15)
        SYMBOLS_NULL_RATIO.labels(column="sector").set(0.32)
        SYMBOLS_NULL_RATIO.labels(column="market_cap").set(0.08)
        
        # Check the metric values
        metrics_output = generate_latest(REGISTRY).decode()
        assert 'mp_symbols_null_ratio{column="figi"} 0.15' in metrics_output
        assert 'mp_symbols_null_ratio{column="sector"} 0.32' in metrics_output
        assert 'mp_symbols_null_ratio{column="market_cap"} 0.08' in metrics_output

    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.fetch_providers')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.diff_snapshot')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.run_scd_update')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.refresh')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline._update_null_ratio_metrics')
    def test_pipeline_records_snapshot_metric(
        self, 
        mock_update_metrics,
        mock_refresh,
        mock_scd_update,
        mock_diff,
        mock_normalize,
        mock_fetch,
        tmp_path
    ):
        """Test that pipeline records snapshot metrics."""
        # Setup mocks
        mock_records = []
        for i in range(123):
            mock_record = Mock()
            mock_record.model_dump.return_value = {
                "symbol": f"TEST{i}",
                "company_name": f"Test Company {i}",
                "exchange": "NYSE"
            }
            mock_records.append(mock_record)
        mock_fetch.return_value = mock_records
        mock_diff.return_value = (50, 25)  # 50 inserts, 25 updates
        
        # Run pipeline
        db_path = tmp_path / "test.db"
        data_dir = tmp_path / "data"
        
        insert_count, update_count = run_symbol_pipeline(
            db_path=db_path,
            data_dir=data_dir,
            provider_names=["polygon"],
            snapshot_as_of=dt.date(2024, 1, 15),
            dry_run=False,
            diff_only=False,
        )
        
        # Verify metrics were recorded
        metrics_output = generate_latest(REGISTRY).decode()
        assert 'mp_symbols_snapshot_records_total' in metrics_output
        
        # Verify other functions were called
        mock_update_metrics.assert_called_once()

    def test_update_null_ratio_metrics_with_mock_db(self):
        """Test null ratio calculation with mocked DuckDB connection."""
        # Create mock connection
        mock_conn = Mock()
        
        # Mock DESCRIBE result - returns [column_name, type, ...]
        mock_conn.sql.return_value.fetchall.side_effect = [
            # DESCRIBE v_symbol_latest
            [
                ("id", "INTEGER"),
                ("symbol", "VARCHAR"),
                ("company_name", "VARCHAR"),
                ("sector", "VARCHAR"),
                ("valid_from", "DATE"),
                ("valid_to", "DATE"),
            ],
            # COUNT(*) FROM v_symbol_latest
            None,  # Won't be called due to fetchone
            # COUNT(*) WHERE column IS NULL for each column
            None, None, None  # Won't be called due to fetchone
        ]
        
        # Mock fetchone results
        mock_conn.sql.return_value.fetchone.side_effect = [
            (1000,),  # total row count
            (150,),   # nulls in symbol
            (320,),   # nulls in company_name  
            (80,),    # nulls in sector
        ]
        
        # Call the function
        _update_null_ratio_metrics(mock_conn)
        
        # Check that metrics were set
        metrics_output = generate_latest(REGISTRY).decode()
        assert 'mp_symbols_null_ratio{column="symbol"} 0.15' in metrics_output
        assert 'mp_symbols_null_ratio{column="company_name"} 0.32' in metrics_output
        assert 'mp_symbols_null_ratio{column="sector"} 0.08' in metrics_output

    def test_update_null_ratio_metrics_handles_empty_table(self):
        """Test null ratio calculation handles empty table gracefully."""
        mock_conn = Mock()
        
        # Mock empty table
        mock_conn.sql.return_value.fetchall.return_value = [
            ("id", "INTEGER"),
            ("symbol", "VARCHAR"),
        ]
        mock_conn.sql.return_value.fetchone.return_value = (0,)  # zero rows
        
        # Should not raise an exception
        _update_null_ratio_metrics(mock_conn)
        
        # Since there are 0 rows, ratio should be 0/1 = 0.0
        metrics_output = generate_latest(REGISTRY).decode()
        assert 'mp_symbols_null_ratio{column="symbol"} 0.0' in metrics_output

    def test_update_null_ratio_metrics_handles_missing_view(self):
        """Test null ratio calculation handles missing view gracefully."""
        mock_conn = Mock()
        
        # Mock SQL error (view doesn't exist)
        mock_conn.sql.side_effect = Exception("view not found")
        
        # Should not raise an exception
        _update_null_ratio_metrics(mock_conn)
        
        # No metrics should be set since view doesn't exist
        # This is acceptable behavior for the function 
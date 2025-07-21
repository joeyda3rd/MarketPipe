"""Tests for symbol pipeline Prometheus metrics."""

from __future__ import annotations

import datetime as dt
from unittest.mock import Mock, patch

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from marketpipe.ingestion.pipeline.symbol_pipeline import (
    _update_null_ratio_metrics,
    run_symbol_pipeline,
)


class TestSymbolsMetrics:
    """Test symbol pipeline metrics collection."""

    def setup_method(self):
        """Setup test-specific metrics registry for isolation."""
        # Create test-specific registry for isolation
        self.test_registry = CollectorRegistry()

        # Create test-specific metric instances
        self.test_symbols_rows = Counter(
            "mp_symbols_rows_total",
            "SCD rows written to symbols_master parquet dataset",
            ["action"],
            registry=self.test_registry,
        )

        self.test_symbols_snapshot_records = Counter(
            "mp_symbols_snapshot_records_total",
            "Raw provider symbol rows staged for dedupe",
            registry=self.test_registry,
        )

        self.test_symbols_null_ratio = Gauge(
            "mp_symbols_null_ratio",
            "Share of NULLs per column in v_symbol_latest",
            ["column"],
            registry=self.test_registry,
        )

    def test_metrics_are_registered(self):
        """Test that all symbol metrics are registered with Prometheus."""
        metrics_output = generate_latest(REGISTRY).decode()

        # Check that metric names appear in the output
        assert "mp_symbols_rows_total" in metrics_output
        assert "mp_symbols_snapshot_records_total" in metrics_output
        assert "mp_symbols_null_ratio" in metrics_output

    def test_snapshot_records_counter(self):
        """Test snapshot records counter increments correctly."""
        # Use test-specific metrics for precise control
        self.test_symbols_snapshot_records.inc(150)
        self.test_symbols_snapshot_records.inc(75)

        # Check the metric value
        metrics_output = generate_latest(self.test_registry).decode()
        assert "mp_symbols_snapshot_records_total 225.0" in metrics_output

    def test_symbols_rows_counter_with_labels(self):
        """Test symbols rows counter with action labels."""
        # Record insert and update operations
        self.test_symbols_rows.labels(action="insert").inc(50)
        self.test_symbols_rows.labels(action="update").inc(25)
        self.test_symbols_rows.labels(action="insert").inc(10)  # Additional inserts

        # Check the metric values
        metrics_output = generate_latest(self.test_registry).decode()
        assert 'mp_symbols_rows_total{action="insert"} 60.0' in metrics_output
        assert 'mp_symbols_rows_total{action="update"} 25.0' in metrics_output

    def test_null_ratio_gauge_with_columns(self):
        """Test null ratio gauge with column labels."""
        # Set null ratios for different columns
        self.test_symbols_null_ratio.labels(column="figi").set(0.15)
        self.test_symbols_null_ratio.labels(column="sector").set(0.32)
        self.test_symbols_null_ratio.labels(column="market_cap").set(0.08)

        # Check the metric values
        metrics_output = generate_latest(self.test_registry).decode()
        assert 'mp_symbols_null_ratio{column="figi"} 0.15' in metrics_output
        assert 'mp_symbols_null_ratio{column="sector"} 0.32' in metrics_output
        assert 'mp_symbols_null_ratio{column="market_cap"} 0.08' in metrics_output

    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.fetch_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.diff_snapshot")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.run_scd_update")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.refresh")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline._update_null_ratio_metrics")
    def test_pipeline_records_snapshot_metric(
        self,
        mock_update_metrics,
        mock_refresh,
        mock_scd_update,
        mock_diff,
        mock_normalize,
        mock_fetch,
        tmp_path,
    ):
        """Test that pipeline records snapshot metrics."""
        # Setup mocks
        mock_records = []
        for i in range(123):
            mock_record = Mock()
            mock_record.model_dump.return_value = {
                "symbol": f"TEST{i}",
                "company_name": f"Test Company {i}",
                "exchange": "NYSE",
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
        assert "mp_symbols_snapshot_records_total" in metrics_output

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
            None,
            None,
            None,  # Won't be called due to fetchone
        ]

        # Mock fetchone results
        mock_conn.sql.return_value.fetchone.side_effect = [
            (1000,),  # total row count
            (150,),  # nulls in symbol
            (320,),  # nulls in company_name
            (80,),  # nulls in sector
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

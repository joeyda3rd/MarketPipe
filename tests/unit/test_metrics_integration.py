# SPDX-License-Identifier: Apache-2.0
"""Unit tests for metrics integration."""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone

import pytest
from marketpipe.metrics import (
    AGG_ROWS,
    INGEST_ROWS,
    PROCESSING_TIME,
    VALIDATION_ERRORS,
    MetricPoint,
    SqliteMetricsRepository,
    TrendPoint,
    record_metric,
)


@pytest.fixture
def temp_db():
    """Create temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def metrics_repo(temp_db):
    """Create metrics repository with temp database."""
    return SqliteMetricsRepository(temp_db)


@pytest.mark.asyncio
async def test_sqlite_metrics_repository_record_and_retrieve(metrics_repo):
    """Test basic record and retrieve functionality."""
    # Record some metrics
    await metrics_repo.record("test_metric", 42.5)
    await metrics_repo.record("test_metric", 35.0)
    await metrics_repo.record("another_metric", 100.0)

    # Retrieve metrics history
    points = await metrics_repo.get_metrics_history("test_metric")
    assert len(points) == 2

    # Check the values
    values = [p.value for p in points]
    assert 42.5 in values
    assert 35.0 in values

    # Check metric names
    assert all(p.metric == "test_metric" for p in points)


@pytest.mark.asyncio
async def test_sqlite_metrics_repository_list_metrics(metrics_repo):
    """Test listing available metrics."""
    # Initially empty
    metrics = await metrics_repo.list_metric_names()
    assert metrics == []

    # Add some metrics
    await metrics_repo.record("metric_a", 1.0)
    await metrics_repo.record("metric_b", 2.0)
    await metrics_repo.record("metric_a", 3.0)  # Duplicate name

    # Should list unique metric names
    metrics = await metrics_repo.list_metric_names()
    assert set(metrics) == {"metric_a", "metric_b"}


@pytest.mark.asyncio
async def test_sqlite_metrics_repository_averages(metrics_repo):
    """Test getting average metrics over time windows."""
    # Record metrics over time
    for i in range(10):
        await metrics_repo.record("cpu_usage", 50.0 + i * 2)  # Increasing values

    # Get average over a large window (should include all data)
    avg = await metrics_repo.get_average_metrics("cpu_usage", window_minutes=60)

    # Average of 50, 52, 54, ..., 68 = 59.0
    expected_avg = sum(50.0 + i * 2 for i in range(10)) / 10
    assert abs(avg - expected_avg) < 0.1


@pytest.mark.asyncio
async def test_sqlite_metrics_repository_performance_trends(metrics_repo):
    """Test getting performance trends with time buckets."""
    # Record some test data
    for i in range(20):
        await metrics_repo.record("response_time", 100.0 + i * 5)

    # Get trends with 5 buckets
    trends = await metrics_repo.get_performance_trends("response_time", buckets=5)

    assert len(trends) == 5
    assert all(isinstance(trend, TrendPoint) for trend in trends)
    assert all(trend.bucket_start < trend.bucket_end for trend in trends)


def test_record_metric_function_updates_prometheus_and_sqlite(temp_db):
    """Test that record_metric updates both Prometheus and SQLite."""
    # Set the global repository to use temp database
    import marketpipe.metrics as metrics_module

    # Temporarily replace the global repository
    original_repo = metrics_module._metrics_repo
    metrics_module._metrics_repo = SqliteMetricsRepository(temp_db)

    try:
        # Test recording metrics
        record_metric("ingest_test_metric", 123.45)
        record_metric("validation_test_metric", 67.89)

        # Check SQLite storage (run synchronously since record_metric handles async internally)
        repo = metrics_module.get_metrics_repository()

        # Give it a moment for async operations
        import time

        time.sleep(0.1)

        # Verify metrics were stored
        ingest_points = asyncio.run(repo.get_metrics_history("ingest_test_metric"))
        validation_points = asyncio.run(repo.get_metrics_history("validation_test_metric"))

        assert len(ingest_points) >= 1
        assert len(validation_points) >= 1

        # Check values
        assert any(p.value == 123.45 for p in ingest_points)
        assert any(p.value == 67.89 for p in validation_points)

    finally:
        # Restore original repository
        metrics_module._metrics_repo = original_repo


@pytest.mark.asyncio
async def test_metrics_repository_handles_empty_database(metrics_repo):
    """Test repository behavior with empty database."""
    # Should handle empty database gracefully
    assert await metrics_repo.list_metric_names() == []

    empty_history = await metrics_repo.get_metrics_history("nonexistent")
    assert empty_history == []

    zero_avg = await metrics_repo.get_average_metrics("nonexistent", window_minutes=60)
    assert zero_avg == 0.0


@pytest.mark.asyncio
async def test_metrics_repository_handles_invalid_queries(metrics_repo):
    """Test repository error handling for invalid queries."""
    # Should handle invalid parameters gracefully
    empty_result = await metrics_repo.get_metrics_history("")
    assert empty_result == []

    zero_avg = await metrics_repo.get_average_metrics("", window_minutes=1)
    assert zero_avg == 0.0


def test_prometheus_metrics_integration():
    """Test that new Prometheus metrics are properly defined."""
    # Test that new metrics exist and can be incremented
    initial_ingest = INGEST_ROWS.labels(symbol="TEST")._value._value
    initial_validation = VALIDATION_ERRORS.labels(symbol="TEST", error_type="test")._value._value
    initial_agg = AGG_ROWS.labels(frame="1m", symbol="TEST")._value._value

    # Increment metrics
    INGEST_ROWS.labels(symbol="TEST").inc(10)
    VALIDATION_ERRORS.labels(symbol="TEST", error_type="test").inc(2)
    AGG_ROWS.labels(frame="1m", symbol="TEST").inc(5)

    # Verify increments
    assert INGEST_ROWS.labels(symbol="TEST")._value._value == initial_ingest + 10
    assert (
        VALIDATION_ERRORS.labels(symbol="TEST", error_type="test")._value._value
        == initial_validation + 2
    )
    assert AGG_ROWS.labels(frame="1m", symbol="TEST")._value._value == initial_agg + 5

    # Test processing time summary
    PROCESSING_TIME.labels(operation="test").observe(1.5)

    # Check that it was recorded (summary metrics are more complex to test)
    summary_metric = PROCESSING_TIME.labels(operation="test")
    assert hasattr(summary_metric, "_count")
    assert summary_metric._count._value > 0


@pytest.mark.asyncio
async def test_metric_point_dataclass():
    """Test MetricPoint dataclass functionality."""
    now = datetime.now(timezone.utc)
    point = MetricPoint(timestamp=now, metric="test", value=42.0)

    assert point.timestamp == now
    assert point.metric == "test"
    assert point.value == 42.0

    # Test immutability
    with pytest.raises(AttributeError):
        point.value = 50.0


@pytest.mark.asyncio
async def test_trend_point_dataclass():
    """Test TrendPoint dataclass functionality."""
    start = datetime.now(timezone.utc)
    end = datetime.now(timezone.utc)

    trend = TrendPoint(bucket_start=start, bucket_end=end, average_value=25.5, sample_count=10)

    assert trend.bucket_start == start
    assert trend.bucket_end == end
    assert trend.average_value == 25.5
    assert trend.sample_count == 10

    # Test immutability
    with pytest.raises(AttributeError):
        trend.sample_count = 20


if __name__ == "__main__":
    pytest.main([__file__])

"""Regression tests for trading-day aggregation and exposed timeframes."""

from unittest.mock import Mock

import duckdb
import pandas as pd
import pytest

from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.domain.value_objects import DEFAULT_SPECS, FrameSpec
from marketpipe.aggregation.infrastructure import duckdb_views
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine


def _bars(timestamps):
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * len(timestamps),
            "ts_ns": [pd.Timestamp(timestamp).value for timestamp in timestamps],
            "open": [100.0] * len(timestamps),
            "high": [101.0] * len(timestamps),
            "low": [99.0] * len(timestamps),
            "close": [100.0] * len(timestamps),
            "volume": [100] * len(timestamps),
        }
    )


@pytest.mark.parametrize(
    "timestamp, expected_open",
    [
        ("2024-01-15T15:00:00Z", "2024-01-15T14:30:00Z"),
        ("2024-07-15T15:00:00Z", "2024-07-15T13:30:00Z"),
        ("2024-07-16T00:00:00Z", "2024-07-15T13:30:00Z"),
    ],
)
def test_daily_bars_use_new_york_date_and_market_open(timestamp, expected_open):
    bars = _bars([timestamp])
    with duckdb.connect(":memory:") as connection:
        connection.register("bars", bars)
        result = connection.execute(
            AggregationDomainService.duckdb_sql(FrameSpec("1d", 86400))
        ).fetch_df()
    assert result.iloc[0]["ts_ns"] == pd.Timestamp(expected_open).value


def test_multi_day_aggregation_writes_separate_date_partitions(tmp_path):
    engine = DuckDBAggregationEngine(tmp_path / "raw", tmp_path / "agg")
    engine._agg_storage.write = Mock()
    bars = _bars(["2024-01-15T14:30:00Z", "2024-01-16T14:30:00Z"])
    engine._write_aggregated_data(bars, "AAPL", FrameSpec("5m", 300), "test-job")
    calls = engine._agg_storage.write.call_args_list
    assert len(calls) == 2
    assert [call.kwargs["trading_day"].isoformat() for call in calls] == [
        "2024-01-15",
        "2024-01-16",
    ]
    assert all(len(call.args[0]) == 1 for call in calls)


def test_advertised_timeframes_can_be_queried(tmp_path, monkeypatch):
    assert {spec.name for spec in DEFAULT_SPECS} == {"5m", "15m", "30m", "1h", "4h", "1d"}
    monkeypatch.setattr(duckdb_views, "AGG_ROOT", tmp_path)
    for frame in ["30m", "4h"]:
        partition = tmp_path / f"frame={frame}" / "symbol=AAPL" / "date=2024-01-15"
        partition.mkdir(parents=True)
        _bars(["2024-01-15T14:30:00Z"]).to_parquet(partition / "data.parquet", index=False)
        result = duckdb_views.query(f"SELECT * FROM bars_{frame}")
        assert len(result) == 1
        assert result.iloc[0]["symbol"] == "AAPL"

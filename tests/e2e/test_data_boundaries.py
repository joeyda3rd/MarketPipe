"""Independent golden values exercise real storage and every aggregation frame."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.domain.value_objects import DEFAULT_SPECS
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize(
    "day,utc_open",
    [
        ("2024-03-08", "14:30"),
        ("2024-03-11", "13:30"),
        ("2024-11-01", "13:30"),
        ("2024-11-04", "14:30"),
    ],
)
def test_all_frames_use_literal_ohlcv_and_dst_trading_days(tmp_path, day, utc_open):
    timestamps = [pd.Timestamp(f"{day}T16:00:00Z").value, pd.Timestamp(f"{day}T16:01:00Z").value]
    rows = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "ts_ns": timestamps[1],
                "open": 101.0,
                "high": 104.0,
                "low": 100.0,
                "close": 103.0,
                "volume": 20,
            },
            {
                "symbol": "AAPL",
                "ts_ns": timestamps[0],
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 0,
            },
        ]
    )
    storage = ParquetStorageEngine(tmp_path / "raw")
    storage.append_to_job(
        rows, frame="1m", symbol="AAPL", trading_day=date.fromisoformat(day), job_id="golden"
    )
    storage.append_to_job(
        rows, frame="1m", symbol="AAPL", trading_day=date.fromisoformat(day), job_id="golden"
    )
    assert len(storage.load_job_bars("golden")["AAPL"]) == 2
    engine = DuckDBAggregationEngine(tmp_path / "raw", tmp_path / "agg")
    engine.aggregate_job(
        "golden", [(spec, AggregationDomainService.duckdb_sql(spec)) for spec in DEFAULT_SPECS]
    )
    assert len(list((tmp_path / "agg").rglob("*.parquet"))) == 6
    for spec in DEFAULT_SPECS:
        path = next((tmp_path / "agg" / f"frame={spec.name}").rglob("*.parquet"))
        result = pd.read_parquet(path)
        assert result[["open", "high", "low", "close", "volume"]].values.tolist() == [
            [100, 104, 99, 103, 20]
        ]
        expected = f"{day}T{utc_open}:00Z" if spec.name == "1d" else f"{day}T16:00:00Z"
        assert result["ts_ns"].tolist() == [pd.Timestamp(expected).value]


def test_corrupt_parquet_propagates_failure_through_installed_cli(cli):
    partition = cli.root / "data/raw/frame=1m/symbol=AAPL/date=2024-01-15"
    partition.mkdir(parents=True)
    (partition / "AAPL_2024-01-15.parquet").write_bytes(b"not a parquet file")
    cli.run("aggregate-ohlcv", "AAPL_2024-01-15", success=False)
    cli.run("validate-ohlcv", "AAPL_2024-01-15", success=False)
    assert not list((cli.root / "data/agg").rglob("*.parquet"))

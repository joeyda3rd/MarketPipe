"""Legacy coordinator flow tests - maintaining backward compatibility."""

import datetime as dt
from pathlib import Path

import yaml
import pyarrow.parquet as pq

from marketpipe.ingestion.coordinator import IngestionCoordinator


def make_test_ohlcv_bars_for_symbol(symbol: str) -> list[dict]:
    """Create test OHLCV bar data for a symbol."""
    rows = []
    start = dt.datetime(2023, 1, 2, 13, 30)
    for i in range(10):
        ts = int(start.timestamp() * 1_000_000_000) + i * 60_000_000_000
        rows.append(
            {
                "symbol": symbol,
                "timestamp": ts,
                "date": start.date(),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "trade_count": 1,
                "vwap": None,
                "session": "regular",
                "currency": "USD",
                "status": "ok",
                "source": "alpaca",
                "frame": "1m",
                "schema_version": 1,
            }
        )
    return rows


def test_legacy_ingestion_coordinator_processes_symbol_successfully(tmp_path, monkeypatch):
    cfg = {
        "alpaca": {"key": "k", "secret": "s", "base_url": "http://x"},
        "symbols": ["AAPL"],
        "output_path": str(tmp_path / "data"),
        "start": "2023-01-02",
        "end": "2023-01-02",
        "workers": 1,
        "compression": "zstd",
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.safe_dump(cfg))

    ohlcv_bars = make_test_ohlcv_bars_for_symbol("AAPL")

    def fake_alpaca_market_data_fetch(self, symbol, start_ts, end_ts):
        return ohlcv_bars

    monkeypatch.setattr(
        "marketpipe.ingestion.infrastructure.alpaca_client.AlpacaClient.fetch_batch",
        fake_alpaca_market_data_fetch,
    )

    monkeypatch.setenv("ALPACA_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET", "s")

    coordinator = IngestionCoordinator(str(cfg_file), state_path=str(tmp_path / "state.db"))
    ingestion_summary = coordinator.run()
    assert ingestion_summary["rows"] == len(ohlcv_bars)

    # Verify Hive-style partition structure for symbol
    partition_path = (
        tmp_path
        / "data"
        / "symbol=AAPL"
        / "year=2023"
        / "month=01"
        / "day=02.parquet"
    )
    assert partition_path.exists()
    parquet_file = pq.ParquetFile(partition_path)
    table = parquet_file.read()
    assert table.num_rows == len(ohlcv_bars)
    assert set(table.column("schema_version").to_pylist()) == {1}
    assert parquet_file.metadata.row_group(0).column(0).compression == "ZSTD"

    # Second ingestion run should skip because checkpoint was saved
    second_ingestion_summary = coordinator.run()
    assert second_ingestion_summary["rows"] == 0


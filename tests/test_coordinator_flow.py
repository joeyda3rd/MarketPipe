import datetime as dt
from pathlib import Path

import yaml
import pyarrow.parquet as pq

from marketpipe.ingestion.coordinator import IngestionCoordinator


def make_rows(symbol: str) -> list[dict]:
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


def test_coordinator_flow(tmp_path, monkeypatch):
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

    rows = make_rows("AAPL")

    def fake_fetch(self, symbol, start_ts, end_ts):
        return rows

    monkeypatch.setattr(
        "marketpipe.ingestion.connectors.alpaca_client.AlpacaClient.fetch_batch",
        fake_fetch,
    )

    monkeypatch.setenv("ALPACA_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET", "s")

    coord = IngestionCoordinator(str(cfg_file), state_path=str(tmp_path / "state.db"))
    summary = coord.run()
    assert summary["rows"] == len(rows)

    p = (
        tmp_path
        / "data"
        / "symbol=AAPL"
        / "year=2023"
        / "month=01"
        / "day=02.parquet"
    )
    assert p.exists()
    pf = pq.ParquetFile(p)
    table = pf.read()
    assert table.num_rows == len(rows)
    assert set(table.column("schema_version").to_pylist()) == {1}
    assert pf.metadata.row_group(0).column(0).compression == "ZSTD"

    # second run should skip because checkpoint saved
    summary2 = coord.run()
    assert summary2["rows"] == 0


# MarketPipe (MarketPype)

[![Live Metrics](https://img.shields.io/badge/Live%20Metrics-online-brightgreen)](docs/pipeline.md#metrics)
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)

MarketPipe is a lightweight, Python-native ETL framework focused on time
series market data.  It aims to provide a simple command line interface
for ingesting, aggregating and validating OHLCV data with baked in
DuckDB/Parquet storage.  The project is still in early scaffolding.

```mermaid
flowchart TD
    subgraph Universe
        U0["Weekly Universe Refresh (Cboe + OCC scrape)"] --> U1["Filtered Tickers (universe-YYYY-MM-DD.csv)"]
    end
    subgraph DailyPipeline
        subgraph Ingestion
            I0["Daily Ingest (1-min OHLCV, Finnhub API)"] --> I1["Raw Parquet Storage: frame=1m/symbol/date"]
            BF["Backfill Historical OHLCV (--start, --end)"] --> I1
        end

        subgraph Aggregation
            A0["Aggregate to 5m, 15m, 1h, 1d (DuckDB SQL)"] --> A1["Write Aggregated Parquet: frame=Xm/symbol/date"]
        end

        subgraph Validation
            V0["Validate 1d Close vs Polygon API"] --> V1["Validation Report CSV"]
            V1 --> QA["Emit QA Metrics"]
        end
    end

    subgraph Loader
        L0["load_ohlcv()"]
        L0 --> DB0["DuckDB parquet_scan"]
        DB0 --> DF["Filtered DataFrame (timestamp, symbol)"]
    end

    subgraph Schedule
        CRON1["18:10 ET → Ingest"] --> I0
        CRON2["18:20 ET → Aggregate"] --> A0
        CRON3["18:30 ET → Validate"] --> V0
        CRON4["Thu 20:00 ET → Universe Refresh"] --> U0
    end

    I1 --> A0
    A1 --> V0
    DF --> Quant["Signal/Backtest Consumers"]
    QA --> Quant
```

## Installation

```bash
pip install -e .
```

## Usage

### Running an ingestion job

MarketPipe supports two ways to configure ingestion jobs:

#### Option 1: Using YAML configuration file

```bash
# Basic usage with config file
marketpipe ingest --config examples/config/ingestion_example.yaml

# Override specific settings from config file
marketpipe ingest --config examples/config/ingestion_example.yaml --batch-size 500 --workers 8
```

#### Option 2: Using direct CLI flags

```bash
# Direct flag usage
marketpipe ingest --symbols AAPL,MSFT,NVDA --start 2025-01-01 --end 2025-01-07 --batch-size 1000
```

#### Configuration options

- `--config`: Path to YAML configuration file
- `--symbols`: Comma-separated list of stock symbols (e.g., AAPL,MSFT)  
- `--start`: Start date in YYYY-MM-DD format
- `--end`: End date in YYYY-MM-DD format
- `--batch-size`: Number of bars per API request (default: 1000)
- `--output`: Output directory for data files (default: ./data)
- `--workers`: Number of worker threads (default: 4)
- `--provider`: Market data provider (default: alpaca)
- `--feed-type`: Data feed type - 'iex' for free, 'sip' for paid (default: iex)

### Starting metrics server

```bash
# Start Prometheus metrics server
marketpipe metrics --port 8000
```

### General help

```bash
marketpipe --help
marketpipe ingest --help
```

## License

MarketPipe is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

The Apache 2.0 license permits commercial use, including the development of closed-source plugins, user interfaces, and hosted services based on this codebase. This enables flexible monetization strategies while keeping the core framework open source.

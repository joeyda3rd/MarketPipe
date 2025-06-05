# MarketPipe

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

```bash
marketpipe --help
```

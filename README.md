# MarketPipe (MarketPype)

[![Live Metrics](https://img.shields.io/badge/Live%20Metrics-online-brightgreen)](docs/pipeline.md#metrics)
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)

MarketPipe is a lightweight, Python-native ETL framework focused on time
series market data.  It aims to provide a simple command line interface
for ingesting, aggregating and validating OHLCV data with baked in
DuckDB/Parquet storage.  The project is still in early scaffolding.

```mermaid
flowchart TD
    %% ---------- Provider layer ----------
    subgraph "Provider Registry"
        direction LR
        REG["Registry<br/>(entry_points)"]:::adapter
        REG -. registers .-> PAD["Adapters:<br/>• Alpaca<br/>• Finnhub<br/>• Polygon<br/>• …"]:::adapter
    end

    %% ---------- Universe ----------
    subgraph "Universe"
        UB["Universe Builder<br/>(filters.yml)"]:::io --> UCSV["universe-YYYY-MM-DD.csv"]
    end

    %% ---------- Daily pipeline (single row) ----------
    subgraph "DailyPipeline"
        direction LR          %% keep the three stages side-by-side

        %% Ingestion
        subgraph "Ingestion"
            direction TB
            ING["mp ingest-ohlcv --provider &lt;id&gt;"]:::io --> RAW["Raw 1 m Parquet"]
            BF["mp backfill-ohlcv --provider &lt;id&gt;"]:::io --> RAW
        end

        %% Aggregation
        subgraph "Aggregation"
            direction TB
            AGG["mp aggregate-ohlcv"] --> AGGPK["Parquet 5 m / 15 m / 1 h / 1 d"]
        end

        %% Validation
        subgraph "Validation"
            direction TB
            VAL["mp validate-ohlcv --provider &lt;id&gt;"]:::io --> VCSV["Validation report"]
            VCSV --> QA["Emit QA metrics"]
        end
    end

    %% ---------- Loader (dropped one row) ----------
    subgraph "Loader"
        direction TB
        LD["load_ohlcv()"] --> SCAN["DuckDB parquet_scan"] --> DF["DataFrame"]
    end

    %% invisible spacer to steer arrow around Provider Registry
    SPACER(( )):::invis

    %% ---------- Scheduler ----------
    subgraph "Scheduler (crontab)"
        CR1["18:10 ET"] --> ING
        CR2["18:20 ET"] --> AGG
        CR3["18:30 ET"] --> VAL
        CR4["Thu 20:00 ET"] --> UB
    end

    %% ---------- Flow arrows ----------
    UCSV --> ING
    UCSV --> BF
    REG  --> ING
    REG  --> BF
    REG  --> VAL
    RAW  --> AGG
    AGGPK --> VAL
    QA   --> MON["Prometheus / Grafana"]

    %% bend DataFrame → Quant around Provider Registry
    DF --> SPACER
    SPACER --> QUANT["Quant / Backtest"]

    %% ---------- Styles ----------
    classDef io      fill:#ffeecc,stroke:#d88200;
    classDef adapter fill:#d0eaff,stroke:#0077cc;
    classDef invis   fill:#ffffff00,stroke:#ffffff00;   %% transparent node

    class ING,BF,UB,VAL io;
    class REG,PAD adapter;

```

## Installation

```bash
pip install -e .
```

## Usage

### Available Providers

First, check what providers are available:

```bash
# List all registered providers
marketpipe providers
```

### Running an ingestion job

MarketPipe supports direct CLI flag usage with multiple providers:

```bash
# Using fake provider (generates synthetic data)
marketpipe ingest --provider fake --symbols TEST --start 2024-01-01 --end 2024-01-02 --batch-size 10

# Using Alpaca provider (requires ALPACA_KEY and ALPACA_SECRET env vars)
marketpipe ingest --provider alpaca --symbols AAPL,MSFT --start 2024-01-01 --end 2024-01-02 --batch-size 1000

# Using IEX provider (requires IEX_TOKEN env var)
marketpipe ingest --provider iex --symbols AAPL --start 2024-01-01 --end 2024-01-02 --batch-size 500
```

#### Configuration options

- `--provider`: Market data provider (`fake`, `alpaca`, `iex`)
- `--symbols`: Comma-separated list of stock symbols (e.g., AAPL,MSFT)  
- `--start`: Start date in YYYY-MM-DD format
- `--end`: End date in YYYY-MM-DD format
- `--batch-size`: Number of bars per API request (default: 1000)
- `--output`: Output directory for data files (default: ./data)
- `--workers`: Number of worker threads (default: 4)

### Other Commands

```bash
# Validate ingested data
marketpipe validate

# Run ad-hoc queries on stored data
marketpipe query

# Aggregate data to different timeframes
marketpipe aggregate

# Start Prometheus metrics server
marketpipe metrics --port 8000

# Apply database migrations
marketpipe migrate
```

### General help

```bash
marketpipe --help
marketpipe ingest --help
```

## License

MarketPipe is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

The Apache 2.0 license permits commercial use, including the development of closed-source plugins, user interfaces, and hosted services based on this codebase. This enables flexible monetization strategies while keeping the core framework open source.

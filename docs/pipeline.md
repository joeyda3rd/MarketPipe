# Ingestion Pipeline

The first end-to-end pipeline pulls one trading day of minute bars from
Alpaca and writes validated Parquet files.  The flow is:

```mermaid
flowchart LR
    A[IngestionCoordinator] --> B[AlpacaClient]
    B --> C[SchemaValidator]
    C --> D[ParquetWriter]
    D --> E[SQLite Checkpoint]
```

Run it from the project root:

```bash
marketpipe ingest --config config/example_config.yaml
```

This writes partitioned files like
`data/symbol=AAPL/year=2025/month=06/day=04.parquet`.

The `compression` field in `config/example_config.yaml` controls the
Parquet codec (`snappy` or `zstd`).


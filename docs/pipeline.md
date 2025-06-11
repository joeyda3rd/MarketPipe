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

<<<<<<< HEAD
Compression defaults to **snappy** but can be overridden via the
`compression` field in ``config/example_config.yaml`` (e.g. ``zstd``).
=======
The `compression` field in `config/example_config.yaml` controls the
Parquet codec (`snappy` or `zstd`).

## Metrics

When `metrics.enabled: true` in the config, the coordinator exposes Prometheus
metrics on `http://localhost:8000/metrics`. Import `docs/grafana_dashboard.json`
into Grafana to view request rate, errors, latency, and backlog. Use the
Grafana UI's *Import Dashboard* feature and select the JSON file.

>>>>>>> df1d1238a09e3b6e79502b59c928819d7d544643


# API Client Design

This document captures the initial design for the MarketPipe API client layer.
It summarizes long‑term goals and the minimal feature set targeted for the
first release.

## Goals

* Provide a vendor‑agnostic interface so new connectors can be added easily.
* Offer both synchronous and asynchronous request methods for future scale.
* Centralize rate‑limit handling, authentication and pagination.
* Keep the base class lightweight and fully testable.

## Architecture Overview

```
ingestion/
└── connectors/
    ├── base_api_client.py
    ├── auth.py
    ├── rate_limit.py
    └── models.py
```

Each concrete connector (e.g. `AlpacaClient`) extends `BaseApiClient` and
implements request parameter construction, response parsing and retry logic.
Authentication strategies and rate limiters are pluggable objects injected at
runtime.

## Configuration

`ClientConfig` (a Pydantic model) defines all common configuration fields such
as API key, base URL, timeout and retry limits.  This avoids scattered keyword
arguments and provides validation at startup.

## Pagination and Fetch Flow

`BaseApiClient.paginate()` yields raw pages of JSON until the connector’s
`next_cursor()` method returns ``None``.  High‑level helpers `fetch_batch()` and
`async_fetch_batch()` accumulate and parse these pages into canonical OHLCV
rows.

## Testing Strategy

The abstract class is covered by unit tests verifying:

1. ABC enforcement when required methods are missing.
2. Validation errors from `ClientConfig`.
3. Deterministic pagination behaviour when `next_cursor()` drives the
   iteration.

Concrete clients will be tested separately using mock HTTP servers.

## Comprehensive Design Considerations

The API layer is envisioned as part of a much wider ingestion system.  Our long
term goals include dynamic runtime tuning, modular connectors and rich
observability.  Key design aspects are:

* **Smart adaptation** – workers dynamically tune batch size and pagination
  based on live metrics and historical performance.
* **Modular architecture** – each connector implements authentication,
  pagination and rate‑limit logic behind a common abstract interface.
* **Financial‑data focus** – a canonical OHLCV schema with partitions by symbol
  and date written to Parquet.
* **Resilience and fault tolerance** – exponential back‑off, checkpointing and a
  staging area to isolate bad data.
* **Observability and alerting** – structured JSON logs with per‑endpoint
  metrics.
* **Extensibility for alternative data** – the same connector interface will
  work for news, fundamentals or other future data sets.

These considerations guide the abstractions in `BaseApiClient` so that new
vendors slot in cleanly as the system matures.

## MVP Feature Set

Our minimal viable implementation targets a single well tested vendor while
keeping the architecture future proof.  The MVP includes:

* **Connectors** – one `AlpacaClient` built on top of `BaseApiClient` with token
  header authentication.
* **Interface** – synchronous request methods, cursor pagination and built in
  exponential back‑off.
* **Coordinator** – single process worker pool using Python threads, reading
  symbols from a YAML config.
* **Schema & Storage** – validated Pydantic models written to local Parquet
  partitioned by symbol/year/month/day.
* **Error Handling** – exponential back‑off with jitter and simple error
  counters via logging.
* **Checkpointing** – per symbol timestamp checkpoints stored in SQLite to
  enable resume after failure.

Everything else (distributed queues, complex rate limit registries, cross source
reconciliation) is deferred for later iterations.  This balance lets indie or
small quant teams get reliable one‑minute OHLCV data without a heavy ops
footprint.

# MarketPipe – Sprint TODO
Sprint window: 2025-06-16 → 2025-06-27
Owners: see each subsection. All items must keep ≥ 70 % test coverage and green CI.

## 1  Infrastructure & Monitoring
- [x] **Async Metrics Server** (Owner: Infra) ✅ **COMPLETED 2024-12-19**
  - [x] Audit current `src/marketpipe/metrics_server.py` usage throughout codebase.
  - [x] Build proof of concept using `asyncio.start_server` (or `uvicorn` with ASGI) that emits Prometheus text format.
  - [x] Refactor CLI startup and shutdown helpers to manage the async server lifecycle cleanly.
  - [x] Add integration test via `pytest-asyncio` that scrapes the endpoint with a custom `CollectorRegistry`.
  - [x] Update docs and sample `docker-compose.yml`.
  - [x] Add `event_loop_lag_seconds` gauge to confirm no blocking behaviour.
  - **Implementation Details**: Implemented `AsyncMetricsServer` class with `asyncio.start_server`, added `--legacy-metrics` CLI flag for backward compatibility, comprehensive test suite with 13 test cases, Docker Compose monitoring stack, and Grafana dashboard.

- [x] **Provider / Feed labels on Parquet and retention metrics** (Owner: Infra) ✅ **COMPLETED 2024-12-19**
  - [x] Extend `record_metric()` signature to accept `provider` and `feed` labels.
  - [x] Back-fill label arguments where existing counters or gauges are emitted.
  - [x] Migrate database schema if additional columns are required.
  - [x] Add regression tests verifying that the new labels are present.
  - **Implementation Details**: Extended `record_metric()` function with optional `provider` and `feed` keyword arguments (default "unknown"). Updated all metric emission sites in ingestion, validation, and aggregation services to include provider/feed labels. Added database migration (003) to add `provider` and `feed` columns to metrics table with backward compatibility. Created comprehensive regression test suite with 6 test cases covering all functionality. Fixed migration system to handle duplicate applications gracefully.
  - **Gap Fixes Applied**:
    - **Gap A**: Redefined Prometheus metrics (REQUESTS, ERRORS, LATENCY) with full label set (source, provider, feed) and added legacy metrics for backward compatibility
    - **Gap B**: Replaced brittle test detection with `MP_DISABLE_SQLITE_METRICS` environment variable for controlling SQLite persistence
    - **Gap C**: Enhanced migration 003 with explicit UPDATE statements to back-fill existing rows with default values
    - **Gap D**: Fixed `record_metric()` type hints by adding `source` parameter and implementing proper forwarding to Prometheus metrics with provider/feed labels

- [x] **Grafana dashboard JSON** (Owner: Infra) ✅ **COMPLETED 2024-12-19**
  - [x] Create panels for CPU usage, event-loop lag, rate-limit waits, ingestion throughput.
  - [x] Export dashboard as `grafana/marketpipe_dashboard.json`.
  - [x] Document import procedure in `README.md`.
  - **Implementation Details**: Created comprehensive Grafana dashboard with 8 panels covering request rates, error rates, latency percentiles, event loop lag, ingestion metrics, validation errors, rate limiter waits, and data quality tracking.

## 2  Configuration Management
- [x] **Config versioning** (Owner: Config) ✅ **COMPLETED 2024-12-19**
  - [x] Add `config_version` field to YAML schema (default `1`).
  - [x] Implement validator that warns on unknown versions and fails on unsupported versions.
  - [x] Unit-test forward and backward compatibility.
  - [x] Add CI guard that fails if schema changes without version bump.
  - **Implementation Details**: Added centralized configuration loader with version validation, updated IngestionJobConfig model with versioning support, implemented kebab-case to snake_case normalization, comprehensive test suite covering all scenarios, CI guard for version consistency, and documentation for migration and usage.

## 3  Core Runtime
- [ ] **PostgresIngestionJobRepository** (Owner: Infra)
  - [ ] Design table mirroring SQLite checkpoint structure.
  - [ ] Implement asyncpg CRUD with advisory locks.
  - [ ] Extend test matrix with `postgres` marker.
  - [ ] Wire into factory when `DATABASE_URL` targets Postgres.

## 4  Universe Management
- [ ] **Universe CLI + CSV export** (Owner: Domain)
  - [ ] `mp universe build` and `mp universe list` commands.
  - [ ] Accept `filters.yml`, call `UniverseBuilder`, allow `--export`.
  - [ ] Round-trip test: build → export → reload.

- [ ] **Dynamic universe filtering** (Owner: Domain)
  - [ ] Add `market_cap`, `volume`, `sector` keys to filter parser.
  - [ ] Predicate functions on `SymbolMeta`.
  - [ ] Update docs and examples; add tests.

## 5  Pipeline Commands
- [x] **Backfill command** (Owner: Pipeline) ✅ **COMPLETED 2025-06-20**
  - [x] CLI `mp ohlcv backfill`.
  - [x] Gap detector, batching, idempotency tests.

- [x] **Prune commands** (Owner: Pipeline) ✅ **COMPLETED 2024-12-19**
  - [x] CLI group `mp prune parquet|sqlite`, dry-run mode.
  - [x] Retention logic in Parquet engine and SQLite repo.
  - **Implementation Details**: Complete data retention utilities with age expression parsing (`30d`, `18m`, `5y`), multi-pattern file date extraction, SQLite VACUUM operations, Prometheus metrics integration, domain events, comprehensive testing, and production-ready error handling.

- [ ] **Scheduler integration** (Owner: Pipeline)
  - [ ] Provide sample cron and systemd timer files.
  - [ ] Optional helper `mp schedule generate`.

## 6  Provider Expansion
- [ ] **Finnhub provider** (Owner: Provider) … steps per roadmap.
- [ ] **Polygon provider** (Owner: Provider) … steps per roadmap.
- [ ] **Provider feature matrix** (Owner: Provider) … generate Markdown table.
- [ ] **Multi-provider reconciliation** (stretch) … design comparison service.

## 7  Developer Experience
- [ ] **Ruff + pre-commit** (Owner: Dev-Ex) … config and CI integration.
- [ ] **API documentation** (Owner: Dev-Ex) … MkDocs site to GitHub Pages.
- [ ] **Unified `load_ohlcv()` research API** (Owner: Dev-Ex) … façade plus notebook.

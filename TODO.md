# MarketPipe MVP Roadmap

**MVP Goal**: Deliver a production-ready ETL pipeline that ingests 1-minute OHLCV bars from Alpaca, persists data via SQLite+Parquet, aggregates to multiple timeframes, validates quality, and exposes CLI commands with ≥70% test coverage.

## Priority Legend
- 🔴 **Critical** - Blocks MVP completion
- 🟡 **High** - Required for production readiness  
- 🟢 **Medium** - Enhances reliability/usability
- 🔵 **Low** - Nice-to-have improvements

---

## 🏗️ Core Domain

- [ ] 🔴 **Delete duplicate root-level `events.py`** _(consolidate into single event system)_
- [ ] 🔴 **Implement SqliteSymbolBarsRepository** _(CRUD operations, unit tests ≥90% branch coverage)_
- [ ] 🔴 **Implement SqliteOHLCVRepository** _(streaming queries, batch inserts, error handling)_
- [ ] 🔴 **Implement SqliteCheckpointRepository** _(resume capability, concurrent access safety)_
- [ ] 🟡 **Complete SymbolBarsAggregate business rules** _(daily summary calculation, event emission)_
  - Depends on: SqliteSymbolBarsRepository
- [ ] 🟡 **Implement remaining domain services** _(OHLCVCalculationService, MarketDataValidationService)_
- [ ] 🟢 **Add domain event handlers** _(connect orphaned events to subscribers)_

## 📥 Ingestion Context  

- [ ] 🟡 **Complete AlpacaMarketDataAdapter error handling** _(retry logic, rate limiting, circuit breaker)_
- [ ] 🟡 **Implement IngestionCoordinatorService** _(parallel symbol processing, checkpointing)_
  - Depends on: SqliteCheckpointRepository
- [ ] 🟢 **Add IEX provider stub** _(reuse Alpaca schema, config-driven provider swap)_
- [ ] 🔵 **Remove legacy connectors folder** _(cleanup after adapter migration)_

## 📊 Aggregation Context

- [ ] 🔴 **Implement AggregationRunnerService** _(5m/15m/1h/1d timeframes, DuckDB queries)_
- [ ] 🔴 **Complete ParquetDataStorage** _(partitioning, compression, load APIs)_
- [ ] 🟡 **Add DuckDB view helpers** _(fast querying, time-based filtering)_
  - Depends on: ParquetDataStorage
- [ ] 🟡 **Emit AggregationCompleted/Failed events** _(wire to event bus)_
- [ ] 🟢 **Add aggregation domain tests** _(current coverage: 25%)_

## ✅ Validation Context

- [ ] 🔴 **Implement schema validation rules** _(OHLCV consistency, timestamp alignment)_  
- [ ] 🟡 **Add business rule validators** _(price reasonableness, volume sanity checks)_
- [ ] 🟡 **Implement CsvReportRepository** _(save validation reports per job)_
- [ ] 🟡 **Wire validation to CLI command** _(remove "TODO: wire up validation" comment)_
- [ ] 🟢 **Add validation integration tests** _(current coverage: 30%)_

## 🏭 Infrastructure

- [ ] 🔴 **Implement concrete repository classes** _(replace 45 pass statements in domain/repositories.py)_
- [ ] 🟡 **Complete ParquetStorageEngine** _(partitioned writes, concurrent reads)_
- [ ] 🟡 **Add SQLite migration system** _(schema versioning, upgrade paths)_
- [ ] 🟢 **Implement connection pooling** _(SQLite WAL mode, concurrent access)_

## 📈 Metrics & Monitoring

- [ ] 🟡 **Complete SqliteMetricsRepository** _(history tracking, performance trends)_
- [ ] 🟡 **Add metrics CLI command** _(simple performance reports)_
- [ ] 🟢 **Implement event bus monitoring** _(track published/consumed events)_
- [ ] 🔵 **Add Grafana dashboard config** _(visualization templates)_

## 🧑‍💻 Developer Experience

- [ ] 🔴 **Achieve ≥70% test coverage** _(current: ~45%)_
  - [ ] Add repository integration tests
  - [ ] Add aggregate/service unit tests  
  - [ ] Add end-to-end pipeline test
- [ ] 🟡 **Remove all NotImplementedError placeholders** _(production readiness)_
- [ ] 🟡 **Update README with architecture diagram** _(quick-start guide, config examples)_
- [ ] 🟢 **Add CONTRIBUTING.md** _(test instructions, development setup)_
- [ ] 🔵 **Add API documentation** _(domain model, CLI reference)_

---

**Current Test Coverage**: ~45% overall
- Ingestion: ~65% ✅  
- Infrastructure: ~70% ✅
- Domain Core: ~40% ⚠️
- Validation: ~30% ⚠️  
- Aggregation: ~25% ❌


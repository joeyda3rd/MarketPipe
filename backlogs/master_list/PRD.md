## Preliminary Design Review (PDR)

**Project**: Symbol Master + Updater Module for MarketPipe
**Audience**: Engineering, QA, Dev Ops, Product

---

### 1 Purpose

Create a single source of truth for all tradable instruments, with a pluggable adapter layer that ingests listings from multiple providers, writes a Slowly Changing Dimension (SCD-2) history table, and exposes “latest” and “full history” views for every downstream workflow.

### 2 Goals

* Land and normalize symbol data from at least one equity provider on day one, with room to add more feeds later.
* Guarantee stable integer keys (`id`) so options, fundamentals, and price pipelines can join reliably.
* Capture corporate-action driven changes without rewriting history, by closing old rows and inserting new ones.
* Offer a CLI command that lets ops run **update**, **backfill**, **dry-run**, or **diff-only** modes.
* Surface completeness metrics (null ratios) to Prometheus or the metrics CLI.

### 3 Out of Scope

* Corporate actions themselves (handled in a later module).
* Real-time ingestion of intraday symbol events.
* Non-equity asset classes beyond what the chosen provider supplies at launch.

### 4 Stakeholders

| Role           | Interest                                               |
| -------------- | ------------------------------------------------------ |
| Quant Research | Needs clean IDs and metadata for factor joins.         |
| Ingestion Team | Integrates the table into options and OHLCV loaders.   |
| Dev Ops        | Schedules the CLI in CI or Airflow and tracks metrics. |
| Product        | Monitors coverage gaps to plan provider contracts.     |

### 5 Functional Requirements

1. **Adapter API** (`SymbolProviderBase`) must provide `fetch_symbols(as_of: date) → list[SymbolRecord]`.
2. **Normalizer** must deduplicate on FIGI (fallback `(ticker, exchange_mic)`), assign IDs, and classify rows as insert, update, or unchanged.
3. **Updater** must write an SCD-2 Parquet table with `valid_from`, `valid_to` columns, and close rows on delist or metadata change.
4. **Views**:

   * `v_symbol_latest` returns the newest row where `valid_to IS NULL`.
   * `v_symbol_history` unions all historical rows.
5. **CLI** (`mp symbols update`) accepts arguments:
   `--provider`, `--backfill DATE`, `--dry-run`, `--diff-only`.
6. **Metrics**: emit total symbols ingested, insert count, update count, coverage ratio by column.

### 6 Non-functional Requirements

* **Performance**: normalizer must process 100 k symbols under 30 s on a laptop.
* **Reliability**: updater must be idempotent; retrying with the same snapshot produces no changes.
* **Extensibility**: adding a provider file must require only a new adapter subclass.
* **Observability**: Prometheus metrics and ERROR level logs on validation failures.
* **Security**: no provider secrets stored in code, read from standard env vars.

### 7 Data Model (DuckDB / Parquet)

| Column              | Type    | Nullable |
| ------------------- | ------- | -------- |
| id (PK)             | INT     | no       |
| ticker              | TEXT    | no       |
| figi                | TEXT    | yes      |
| cusip               | TEXT    | yes      |
| isin                | TEXT    | yes      |
| cik                 | TEXT    | yes      |
| exchange\_mic       | TEXT    | no       |
| asset\_class        | TEXT    | no       |
| currency            | TEXT(3) | no       |
| country             | TEXT(2) | yes      |
| sector              | TEXT    | yes      |
| industry            | TEXT    | yes      |
| first\_trade\_date  | DATE    | yes      |
| delist\_date        | DATE    | yes      |
| status              | TEXT    | no       |
| shares\_outstanding | BIGINT  | yes      |
| free\_float         | BIGINT  | yes      |
| company\_name       | TEXT    | yes      |
| meta                | JSON    | yes      |
| valid\_from         | DATE    | no       |
| valid\_to           | DATE    | yes      |

### 8 Interfaces

* **Provider adapters**: HTTP or CSV download, token via `SYMBOLS_<PROVIDER>_KEY`.
* **CLI**: invoked by human or scheduler, calls async adapters, then DuckDB normalizer SQL.
* **Downstream API**: `SymbolRepository` returns DataFrame or Pydantic models backed by `v_symbol_latest`.

### 9 Architecture Sketch

```
+-------------+      async fetch      +-------------------+
| CLI Command |  ───────────────────▶ | Symbol Provider(s)|
+-------------+                       +-------------------+
      │ list[SymbolRecord]                         │
      ▼                                           ▼
+------------------+   validate + diff   +---------------------+
| Normalizer       |──────────▶| SCD-2 Table (Parquet) |
+------------------+           +---------------------+
      │ create views
      ▼
+------------------+
| DuckDB Views     |---> queried by all other modules
+------------------+
```

### 10 Risks & Mitigations

| Risk                          | Mitigation                                      |
| ----------------------------- | ----------------------------------------------- |
| Provider field mismatch       | Pydantic validation with defaults and warnings. |
| Exploding history size        | Year-month partitioning on `valid_from`.        |
| Ticker reuse across exchanges | Combine MIC with ticker in uniqueness logic.    |

### 11 Acceptance Criteria

* Running `mp symbols update --provider polygon` loads at least 90 percent of NYSE and Nasdaq active listings.
* Re-running the same command without provider changes yields zero diff rows.
* Delisting a test symbol sets its `valid_to` and moves it out of `v_symbol_latest`.
* Metrics endpoint exposes `symbol_updates_total`, `symbol_inserts_total`, `symbol_null_ratio`.
* Unit and integration tests reach 90 percent coverage for the new module.


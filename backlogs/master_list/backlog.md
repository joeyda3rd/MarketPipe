## Product Backlog

### Epic A — Symbol Adapter Framework

| Story ID | Description                                          | Definition of Done                                |
| -------- | ---------------------------------------------------- | ------------------------------------------------- |
| A1       | Create `SymbolRecord` Pydantic model matching schema | Model validates sample JSON and CSV rows          |
| A2       | Implement `SymbolProviderBase` abstract class        | Base class passes mypy and unit tests             |
| A3       | Build `PolygonSymbolProvider` adapter                | Returns list of validated `SymbolRecord` from API |
| A4       | Build `NasdaqDailyListProvider` adapter              | Parses Daily List CSV into records                |

### Epic B — Normalizer + SCD-2 Writer

| Story ID | Description                                                | Definition of Done                          |
| -------- | ---------------------------------------------------------- | ------------------------------------------- |
| B1       | Write DuckDB SQL that dedupes and assigns surrogate IDs    | Script produces expected inserts on fixture |
| B2       | Implement diff logic (insert, update, unchanged)           | Unit tests cover each path                  |
| B3       | Write Parquet writer that adds `valid_from` and `valid_to` | Parquet files contain correct partitions    |
| B4       | Generate views `v_symbol_latest`, `v_symbol_history`       | Views return expected row counts            |

### Epic C — Updater CLI

| Story ID | Description                                              | Definition of Done                             |
| -------- | -------------------------------------------------------- | ---------------------------------------------- |
| C1       | Add `mp symbols update` command with Click or Typer      | Help text shows all flags                      |
| C2       | Wire CLI to async adapters and normalizer                | Command ingests snapshot to Parquet            |
| C3       | Implement flags `--dry-run`, `--diff-only`, `--backfill` | Dry-run prints planned changes without writing |

### Epic D — Observability & Validation

| Story ID | Description                                               | Definition of Done                   |
| -------- | --------------------------------------------------------- | ------------------------------------ |
| D1       | Add Prometheus counters for inserts, updates, null ratios | Metrics scrape shows non-zero values |
| D2       | Log validation errors with symbol identifier and field    | Integration test captures log entry  |

### Epic E — Testing & CI

| Story ID | Description                                                                  | Definition of Done                       |
| -------- | ---------------------------------------------------------------------------- | ---------------------------------------- |
| E1       | Unit tests for adapters with respx or httpx\_mock                            | 100 percent branch coverage for adapters |
| E2       | Integration test: ingest fixture snapshot, run second time, expect zero diff | Passes in CI                             |
| E3       | Add code-coverage gating to CI workflow for new module                       | Build fails if coverage < 90 percent     |

### Epic F — Documentation

| Story ID | Description                                                   | Definition of Done                   |
| -------- | ------------------------------------------------------------- | ------------------------------------ |
| F1       | Add README section “Symbol Master” with schema table          | Markdown builds without warnings     |
| F2       | Write CONTRIBUTING guidelines for adding new symbol providers | Docs merged and referenced in README |
| F3       | Publish usage example notebook querying `v_symbol_latest`     | Notebook shows sample pandas join    |

### Epic G — Post-launch Enhancements

| Story ID | Description                                   | Definition of Done                             |
| -------- | --------------------------------------------- | ---------------------------------------------- |
| G1       | Hook symbol updates to Slack notification     | Slack bot posts when delist detected           |
| G2       | Column completeness dashboard                 | Grafana panel displays null ratios             |
| G3       | Add country and ADR parent mapping enrichment | Extra columns populated for 80 percent of ADRs |


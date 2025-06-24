# Test Failure Analysis Matrix

## Summary
- **Total Tests**: 915
- **Failed**: 31
- **Passed**: 877
- **Skipped**: 7
- **Coverage**: 71.25% (exceeds 70% requirement)

## Root Cause Groups

### 1. Date/Time Hardcoding Issues (4 failures)
**Root Cause**: Tests have hardcoded expected dates that don't match current date (2025-06-24)

**Affected Tests**:
- `tests/cli/test_symbols_cli.py::TestSymbolsUpdateCommand::test_environment_variables_respected`
- `tests/ingestion/symbol_providers/test_nasdaq_dl.py::TestNasdaqDailyListProviderParsing::test_fetch_symbols_happy_path`
- `tests/ingestion/symbol_providers/test_nasdaq_dl.py::TestNasdaqDailyListProviderParsing::test_footer_with_extra_spaces`
- `tests/unit/infrastructure/test_alpaca_market_data_adapter.py::TestAlpacaMarketDataAdapterTranslation::test_translates_alpaca_bar_format_to_domain_ohlcv_bar`

**Stack Trace Sample**: 
```
assert datetime.date(2025, 6, 24) == datetime.date(2025, 6, 19)
```

### 2. CLI Exit Code Assertion Failures (15 failures)
**Root Cause**: CLI commands returning unexpected exit codes (1 or 2 instead of 0)

**Affected Tests**:
- `tests/cli/test_symbols_cli.py::TestSymbolsUpdateCommand::test_unknown_provider_exits`
- `tests/cli/test_symbols_cli.py::TestSymbolsUpdateCommand::test_all_providers_used_when_none_specified`
- `tests/cli/test_symbols_execute.py::TestSymbolsExecuteIntegration::test_full_execute_dummy`
- `tests/cli/test_symbols_execute.py::TestSymbolsExecuteIntegration::test_execute_creates_database_views`
- `tests/cli/test_symbols_execute.py::TestSymbolsExecuteIntegration::test_rerun_same_snapshot_adds_zero_rows`
- `tests/unit/cli/test_ingest_cli.py::test_ingest_cli_smoke`
- `tests/unit/cli/test_ingest_cli.py::test_ingest_cli_with_multiple_symbols`
- `tests/unit/cli/test_ingest_cli_config.py::TestIngestCliConfig::test_config_file_loading`
- `tests/unit/cli/test_ingest_cli_config.py::TestIngestCliConfig::test_config_override_with_flags`
- `tests/unit/cli/test_ingest_cli_config.py::TestIngestCliConfig::test_direct_flags_without_config`
- `tests/unit/cli/test_ingest_cli_config.py::TestIngestCliConfig::test_kebab_case_config_loading`
- `tests/unit/cli/test_ingest_output_handling.py::TestCLIOutputHandling::test_output_flag_creates_custom_directory`
- `tests/unit/cli/test_ingest_output_handling.py::TestCLIOutputHandling::test_default_output_path_when_no_flag`
- `tests/unit/cli/test_ingest_output_handling.py::TestProviderSuggestions::test_verification_error_handling`

**Stack Trace Sample**:
```
assert 1 == 0
+  where 1 = <Result SystemExit(1)>.exit_code
```

### 3. CLI Output Content Assertion Failures (6 failures)
**Root Cause**: CLI output doesn't contain expected strings/messages

**Affected Tests**:
- `tests/cli/test_symbols_modes.py::TestSymbolsModes::test_dry_run_with_execute_precedence`
- `tests/cli/test_symbols_modes.py::TestSymbolsModes::test_diff_only_error_combo`
- `tests/cli/test_symbols_modes.py::TestSymbolsModes::test_backfill_diff_only_error_combo`
- `tests/cli/test_symbols_modes.py::TestSymbolsModes::test_backfill_runs_multiple_days`
- `tests/unit/cli/test_ingest_cli_boundary_integration.py::TestIngestCLIBoundaryIntegration::test_help_shows_updated_descriptions`
- `tests/unit/cli/test_ingest_output_handling.py::TestProviderSuggestions::test_provider_suggestions_in_output`

**Stack Trace Sample**:
```
assert "✅ Finished 1 run(s)" in result.output
AssertionError: assert '✅ Finished 1 run(s)' in 'Both --dry-run and --execute specified...'
```

### 4. Data Processing/Storage Issues (3 failures)
**Root Cause**: Ingestion coordinator not writing expected number of records to storage

**Affected Tests**:
- `tests/integration/test_ingestion_coordinator_service_flow.py::TestIngestionCoordinatorEndToEndFlow::test_coordinator_creates_proper_partition_paths`
- `tests/integration/test_ingestion_coordinator_service_flow.py::TestIngestionCoordinatorEndToEndFlow::test_coordinator_emits_comprehensive_domain_events`
- `tests/integration/test_ingestion_coordinator_service_flow.py::TestIngestionCoordinatorEndToEndFlow::test_process_symbol_writes_parquet_partition`

**Stack Trace Sample**:
```
assert 0 == 10
+  where 0 = IngestionPartition(...).record_count
+  and   10 = len([OHLCVBar(...), ...])
```

### 5. Mock/Function Call Verification Issues (3 failures)
**Root Cause**: Expected function calls not being made or called wrong number of times

**Affected Tests**:
- `tests/unit/cli/test_ingest_cli_boundary_integration.py::TestIngestCLIBoundaryIntegration::test_boundary_check_called_after_ingestion`
- `tests/unit/cli/test_ingest_output_handling.py::TestCLIOutputHandling::test_verification_failure_exits_with_error`
- `tests/unit/cli/test_ingest_output_handling.py::TestCLIOutputHandling::test_verification_service_gets_correct_parameters`
- `tests/unit/cli/test_ingest_output_handling.py::TestIngestOutputHandling::test_boundary_check_accepts_correct_data`

**Stack Trace Sample**:
```
AssertionError: Expected '_check_boundaries' to be called once. Called 0 times.
```

## Async Connection Cleanup Warnings
Multiple tests show async generator cleanup issues:
```
ERROR    asyncio:base_events.py:1758 an error occurred during closing of asynchronous generator
RuntimeError: aclose(): asynchronous generator is already running
```

## Priority Assessment
1. **High Priority**: CLI exit code failures (15 tests) - likely environmental/configuration issues
2. **Medium Priority**: Date hardcoding (4 tests) - straightforward fixes
3. **Medium Priority**: Data processing issues (3 tests) - may indicate real bugs
4. **Low Priority**: CLI output content (6 tests) - likely related to CLI exit code issues
5. **Low Priority**: Mock verification issues (3 tests) - test implementation problems
# MarketPipe CLI Validation Framework

A comprehensive validation framework ensuring every MarketPipe command and option works correctly across all supported scenarios.

## Overview

This framework provides systematic testing and validation of the MarketPipe CLI to ensure:
- All commands work as expected
- Options and configurations are validated
- Backward compatibility is maintained
- Users can validate their installations
- Continuous integration catches regressions

## Framework Components

### Phase 1: CLI Command Matrix Testing Framework
**File**: `tests/integration/test_cli_command_matrix.py`

**Purpose**: Auto-discovers and validates all CLI commands
- **Command Discovery**: Automatically finds all available commands and subcommands
- **Help Validation**: Ensures all help commands work and are consistent
- **Side Effect Testing**: Verifies commands don't create unexpected files
- **Performance Monitoring**: Tracks command execution times

**Key Features**:
- Auto-discovery of main commands: `ingest-ohlcv`, `validate-ohlcv`, `aggregate-ohlcv`, etc.
- Sub-command testing: `ohlcv ingest`, `prune parquet`, `symbols update`
- Deprecated command validation with proper warnings
- Comprehensive reporting with success/failure metrics

### Phase 2: Option Validation Matrix
**File**: `tests/integration/test_cli_option_validation.py`

**Purpose**: Tests all valid option combinations and edge cases
- **Provider Testing**: Validates all 5 providers (alpaca, fake, finnhub, iex, polygon)
- **Date Validation**: Tests valid/invalid date ranges and formats
- **Symbol Format Testing**: Validates single/multiple symbol formats
- **Numeric Parameter Testing**: Tests worker counts, batch sizes, timeouts
- **Configuration Precedence**: Tests CLI > env > config > defaults

**Key Features**:
- Comprehensive option combination matrix testing
- Error handling validation for invalid inputs
- Configuration file override testing
- Environment variable integration testing

### Phase 3: End-to-End Pipeline Validation
**Files**:
- `tests/integration/test_pipeline_smoke_validation.py`
- `tests/integration/test_provider_specific_validation.py`

**Purpose**: Validates complete pipeline workflows
- **Smoke Tests**: Quick validation of core functionality
- **Provider Integration**: Tests each provider's specific features
- **Error Handling**: Validates graceful failure scenarios
- **Performance Baselines**: Establishes performance expectations

**Key Features**:
- Complete ingest → validate → aggregate workflows
- Provider-specific feature testing (rate limits, auth, feed types)
- Performance metrics collection and thresholds
- Multi-provider data collection scenarios

### Phase 4: Regression and Compatibility Testing
**Files**:
- `tests/integration/test_cli_backward_compatibility.py`
- `tests/integration/test_config_schema_validation.py`

**Purpose**: Ensures backward compatibility and configuration validity
- **Deprecated Command Testing**: Validates deprecated commands still work with warnings
- **Configuration Schema Testing**: Tests all valid/invalid config combinations
- **Migration Guidance**: Ensures clear upgrade paths from old commands
- **Schema Evolution**: Validates configuration file backward compatibility

**Key Features**:
- Deprecated command warnings and functionality preservation
- Configuration precedence rule validation
- Schema migration testing
- Error message consistency validation

### Phase 5: Continuous Validation Framework
**Files**:
- `src/marketpipe/cli/health_check.py`
- `.github/workflows/cli-validation.yml`

**Purpose**: Built-in validation and continuous monitoring
- **Health Check Command**: `marketpipe health-check` for user validation
- **CI/CD Integration**: Automated testing on every commit
- **Comprehensive Reporting**: Detailed validation reports
- **Performance Monitoring**: Tracks performance regressions

**Key Features**:
- Built-in health check command for users
- GitHub Actions workflow with matrix testing across Python versions
- Automated report generation and PR comments
- Weekly comprehensive validation including performance tests

## Usage

### Running Individual Test Suites

```bash
# Test CLI command matrix
python -m pytest tests/integration/test_cli_command_matrix.py -v

# Test option validation
python -m pytest tests/integration/test_cli_option_validation.py -v

# Test pipeline smoke tests
python -m pytest tests/integration/test_pipeline_smoke_validation.py -v

# Test provider validation
python -m pytest tests/integration/test_provider_specific_validation.py -v

# Test backward compatibility
python -m pytest tests/integration/test_cli_backward_compatibility.py -v

# Test configuration validation
python -m pytest tests/integration/test_config_schema_validation.py -v
```

### Running the Health Check

```bash
# Basic health check
marketpipe health-check

# Verbose health check
marketpipe health-check --verbose

# Health check with configuration file
marketpipe health-check --config config/example_config.yaml --verbose

# Save health check report
marketpipe health-check --verbose --output health_report.txt
```

### Running Tests Directly

Each test module can also be run directly for quick validation:

```bash
# Run command matrix validation
cd tests/integration
python test_cli_command_matrix.py

# Run option validation
python test_cli_option_validation.py

# Run pipeline smoke tests
python test_pipeline_smoke_validation.py

# Run provider validation
python test_provider_specific_validation.py
```

## Continuous Integration

The framework integrates with GitHub Actions to provide automated validation:

### On Every Commit/PR:
- CLI Command Matrix Validation (Python 3.8-3.11)
- Option Validation Matrix (Python 3.9, 3.11)
- Pipeline Smoke Tests (Python 3.9, 3.11)
- Provider Validation (fake provider only)
- Backward Compatibility Tests
- Configuration Schema Validation
- Health Check Validation

### Weekly Comprehensive Validation:
- Performance baseline tests
- Auth-required provider tests (with secrets)
- Extended smoke test scenarios
- Comprehensive health check reports

### Report Generation:
- Automatic report generation with pass/fail statistics
- PR comments with validation results
- Artifact uploads for detailed analysis
- Performance trend tracking

## Test Categories and Markers

### Pytest Markers:
- `@pytest.mark.slow` - Performance and long-running tests
- `@pytest.mark.auth_required` - Tests requiring API credentials
- No marker - Standard tests that run in CI

### Test Categories:
1. **Command Discovery** - Auto-detection of CLI structure
2. **Help Validation** - Consistency and completeness of help output
3. **Option Validation** - Parameter combinations and edge cases
4. **Pipeline Integration** - End-to-end workflow testing
5. **Provider Testing** - Vendor-specific functionality
6. **Backward Compatibility** - Deprecated command preservation
7. **Configuration Testing** - Schema and precedence validation
8. **Performance Testing** - Baseline and regression detection

## Benefits

### For Developers:
1. **Comprehensive Coverage**: Every command and option combination tested
2. **Regression Prevention**: Catches breaking changes before release
3. **Consistent Documentation**: Tests validate help text and documentation
4. **Performance Monitoring**: Establishes and monitors performance baselines

### For Users:
1. **Installation Validation**: Built-in health check command
2. **Configuration Validation**: Comprehensive config file testing
3. **Clear Error Messages**: Validated error handling and messaging
4. **Upgrade Confidence**: Backward compatibility guarantees

### For Operations:
1. **Automated Validation**: CI/CD integration prevents issues
2. **Performance Tracking**: Continuous performance monitoring
3. **Report Generation**: Detailed validation reporting
4. **Issue Detection**: Early detection of configuration and compatibility issues

## Architecture Integration

The CLI validation framework integrates with MarketPipe's Domain-Driven Design architecture:

### Bounded Context Integration:
- **CLI Context**: Command interface validation
- **Configuration Context**: Schema and precedence testing
- **Provider Context**: Provider-specific feature validation
- **Pipeline Context**: End-to-end workflow validation

### Event-Driven Testing:
- Validation events trigger comprehensive test suites
- Performance metrics collected as domain events
- Error scenarios tested through event simulation

### Repository Pattern Usage:
- Test result repositories for historical tracking
- Configuration repositories for test data management
- Report repositories for artifact storage

## Future Enhancements

### Planned Improvements:
1. **Interactive Testing**: Command-line test runner with selection
2. **Performance Profiling**: Detailed performance analysis tools
3. **Load Testing**: High-volume scenario testing
4. **Security Testing**: Input validation and security scanning
5. **Documentation Generation**: Auto-generated CLI documentation from tests

### Integration Opportunities:
1. **IDE Integration**: VS Code extension for validation
2. **Docker Testing**: Container-based validation scenarios
3. **Cloud Testing**: Multi-environment validation
4. **User Telemetry**: Real-world usage pattern validation

## Troubleshooting

### Common Issues:

1. **Test Timeouts**: Increase timeout values for slow environments
2. **Missing Dependencies**: Install test requirements with `pip install -e .[test]`
3. **Permission Issues**: Ensure write permissions for test data directories
4. **Provider Auth**: Skip auth-required tests or provide credentials

### Debug Commands:

```bash
# Verbose test output
python -m pytest tests/integration/ -v -s

# Run specific test method
python -m pytest tests/integration/test_cli_command_matrix.py::TestCLICommandMatrix::test_discover_all_commands -v

# Generate test coverage report
python -m pytest tests/integration/ --cov=marketpipe --cov-report=html

# Run health check for diagnostics
marketpipe health-check --verbose
```

This comprehensive CLI validation framework ensures MarketPipe's command-line interface remains reliable, consistent, and user-friendly across all supported scenarios and environments.

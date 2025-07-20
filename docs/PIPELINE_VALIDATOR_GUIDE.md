# MarketPipe Pipeline Validator Guide

## Overview

The MarketPipe Pipeline Validator is a comprehensive testing and validation script that exercises all critical MarketPipe commands and functionality to ensure proper behavior and identify potential issues. It's designed to be run in development, CI/CD, and production environments to validate system health and functionality.

## Features

- **Comprehensive Command Testing**: Tests all major CLI commands and options
- **Multiple Validation Modes**: Quick, critical, and full testing modes
- **Detailed Reporting**: JSON, YAML, and HTML report formats
- **Health Monitoring**: Identifies system health and configuration issues
- **Error Analysis**: Provides detailed error information and recommendations
- **Performance Metrics**: Tracks command execution times and performance
- **Flexible Configuration**: Customizable test parameters and skip options

## Installation and Setup

### Prerequisites

```bash
# Ensure MarketPipe is installed in development mode
pip install -e .

# Install validation script dependencies (if not already installed)
pip install typer rich pyyaml
```

### Running the Validator

The validator is located at `scripts/pipeline_validator.py` and can be run directly:

```bash
# Quick validation (essential commands only)
python scripts/pipeline_validator.py --mode quick

# Critical validation (recommended for CI/CD)
python scripts/pipeline_validator.py --mode critical --verbose

# Full validation (comprehensive testing)
python scripts/pipeline_validator.py --mode full --report-format html

# Dry run to see what would be tested
python scripts/pipeline_validator.py --mode critical --dry-run
```

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `critical` | Validation mode: `quick`, `critical`, or `full` |
| `--output-dir` | `./validation_output` | Directory for test outputs and reports |
| `--config-file` | Auto-generated | Custom configuration file for testing |
| `--skip-tests` | None | Comma-separated test categories to skip |
| `--verbose` | False | Enable detailed logging and error output |
| `--dry-run` | False | Show what would be tested without execution |
| `--report-format` | `json` | Report format: `json`, `yaml`, or `html` |

### Examples

```bash
# Basic validation with verbose output
python scripts/pipeline_validator.py --verbose

# Skip specific test categories
python scripts/pipeline_validator.py --skip-tests health,metrics

# Custom output directory and HTML report
python scripts/pipeline_validator.py --output-dir /tmp/validation --report-format html

# Full validation with custom configuration
python scripts/pipeline_validator.py --mode full --config-file my_test_config.yaml
```

## Validation Modes

### Quick Mode (`--mode quick`)
- **Purpose**: Fast validation for basic functionality
- **Duration**: 1-2 minutes
- **Tests**: Essential help commands, basic health checks, core provider functionality
- **Use Case**: Quick development checks, IDE integration

**Test Categories**:
- Help commands (3 tests)
- Basic health check (1 test)
- Provider listing (1 test)

### Critical Mode (`--mode critical`) - **Default**
- **Purpose**: Essential functionality for production readiness
- **Duration**: 3-5 minutes
- **Tests**: All critical commands that must work for basic operation
- **Use Case**: CI/CD pipelines, pre-deployment validation

**Test Categories**:
- Help commands (5 tests)
- Health checks (3 tests)
- Provider management (3 tests)
- Symbol operations (4 tests)
- Data validation (2 tests)

### Full Mode (`--mode full`)
- **Purpose**: Comprehensive system validation
- **Duration**: 10-15 minutes
- **Tests**: All available commands and functionality
- **Use Case**: Release validation, comprehensive system testing

**Additional Test Categories**:
- Data ingestion (2 tests)
- Data aggregation (2 tests)
- Job management (3 tests)
- Metrics and monitoring (1 test)
- Maintenance operations (2 tests)

## Understanding Test Results

### Status Codes

- **PASS**: ✅ Command executed successfully (exit code 0)
- **FAIL**: ❌ Command failed with error (exit code != 0)
- **SKIP**: ⏭️ Test was skipped (dry-run or excluded)
- **ERROR**: 💥 Test execution error (timeout, exception)

### Exit Codes

The validator exits with different codes based on results:

- **0**: All tests passed (≥90% pass rate)
- **1**: Tests completed with issues (70-89% pass rate)
- **2**: Critical failures detected (<70% pass rate)
- **130**: User interrupted (Ctrl+C)

### Report Interpretation

#### Summary Metrics
```json
{
  "overall_status": "PASS|FAIL|CRITICAL",
  "pass_rate": "85.5%",
  "total_tests": 17,
  "passed": 15,
  "failed": 2,
  "duration_seconds": 45.2
}
```

#### Category Breakdown
Shows pass rates for each command category:
```
help: 5/5 (100% pass)        # All help commands work
health: 2/3 (67% pass)        # Some health check issues
providers: 3/3 (100% pass)    # Provider system healthy
symbols: 1/4 (25% pass)       # Symbol operations have issues
validation: 2/2 (100% pass)   # Data validation working
```

#### Critical Failures
Lists commands that failed in critical categories (health, providers):
```
Critical Failures:
• marketpipe health-check --verbose
• marketpipe providers --validate
```

## Test Categories Explained

### Help Commands (`help`)
Tests that CLI help system is working:
- `marketpipe --help` - Main help
- `marketpipe ohlcv --help` - Subcommand help
- `marketpipe symbols --help` - Module help

**Expected Results**: All should PASS
**Failure Indicates**: CLI structure issues, import problems

### Health Checks (`health`)
Tests system health and configuration:
- `marketpipe health-check` - Basic health check
- `marketpipe health-check --verbose` - Detailed diagnostics
- `marketpipe health-check --format json` - Structured output

**Expected Results**: May FAIL with specific issues identified
**Failure Indicates**: Missing dependencies, configuration problems, database issues

### Provider Management (`providers`)
Tests data provider system:
- `marketpipe providers` - List available providers
- `marketpipe providers --list` - Detailed provider listing
- `marketpipe providers --validate` - Validate provider configurations

**Expected Results**: Should mostly PASS
**Failure Indicates**: Provider configuration issues, API connectivity problems

### Symbol Operations (`symbols`)
Tests symbol management functionality:
- `marketpipe symbols list` - List available symbols
- `marketpipe symbols validate AAPL` - Validate specific symbol
- `marketpipe symbols validate INVALID_SYMBOL` - Invalid symbol handling
- `marketpipe symbols search tech` - Symbol search

**Expected Results**: Mix of PASS/FAIL (invalid symbol test should FAIL)
**Failure Indicates**: Symbol service issues, data connectivity problems

### Data Validation (`validation`)
Tests data validation pipeline:
- `marketpipe validate-ohlcv --dry-run` - Validation dry run
- `marketpipe ohlcv validate --dry-run` - Alternative command

**Expected Results**: Should PASS in dry-run mode
**Failure Indicates**: Configuration issues, validation system problems

### Data Ingestion (`ingestion`)
Tests data ingestion pipeline (full mode only):
- `marketpipe ingest-ohlcv --dry-run` - Ingestion dry run
- `marketpipe ohlcv ingest --dry-run` - Alternative command

**Expected Results**: Should PASS in dry-run mode
**Failure Indicates**: Provider configuration, authentication issues

### Job Management (`jobs`)
Tests job management system:
- `marketpipe jobs list` - List jobs
- `marketpipe jobs status` - Job status
- `marketpipe jobs clean` - Cleanup jobs

**Expected Results**: Should mostly PASS
**Failure Indicates**: Database connectivity, job system issues

## Troubleshooting Common Issues

### MarketPipe Installation Issues

**Symptoms**:
```
❌ marketpipe health-check
Error: MarketPipe modules not importable
```

**Solutions**:
```bash
# Reinstall MarketPipe in development mode
pip install -e .

# Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# Verify installation
python -c "import marketpipe; print('MarketPipe installed successfully')"
```

### Missing Dependencies

**Symptoms**:
```
❌ Required dependencies
Error: Missing required packages: pyyaml, python-dotenv
```

**Solutions**:
```bash
# Install missing packages
pip install pyyaml python-dotenv

# Install all dependencies
pip install -e .

# Check for virtual environment issues
which python
pip list | grep -E "(pyyaml|python-dotenv)"
```

### Database Connectivity Issues

**Symptoms**:
```
❌ Database connectivity
Error: Could not connect to database
```

**Solutions**:
```bash
# Check database permissions
ls -la data/

# Initialize database if needed
python -m marketpipe migrate

# Check database file
sqlite3 data/marketpipe.db ".tables"
```

### Provider Configuration Issues

**Symptoms**:
```
❌ Provider registry
Error: No providers configured
```

**Solutions**:
```bash
# Check configuration files
ls -la config/

# Validate configuration
python -m marketpipe providers --validate

# Check environment variables
echo $ALPACA_KEY
echo $ALPACA_SECRET
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Pipeline Validation
on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -e .

      - name: Run pipeline validation
        run: |
          python scripts/pipeline_validator.py --mode critical --verbose --report-format html

      - name: Upload validation report
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: validation-report
          path: validation_output/
```

### Jenkins Pipeline Example

```groovy
pipeline {
    agent any

    stages {
        stage('Setup') {
            steps {
                sh 'pip install -e .'
            }
        }

        stage('Validate Pipeline') {
            steps {
                sh 'python scripts/pipeline_validator.py --mode critical --report-format html'
            }
            post {
                always {
                    archiveArtifacts artifacts: 'validation_output/**', fingerprint: true
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'validation_output',
                        reportFiles: '*.html',
                        reportName: 'Pipeline Validation Report'
                    ])
                }
                failure {
                    mail to: 'team@example.com',
                         subject: 'Pipeline Validation Failed',
                         body: 'MarketPipe pipeline validation failed. Check the report for details.'
                }
            }
        }
    }
}
```

## Customizing Tests

### Adding New Test Categories

1. **Extend the validator class**:
```python
def get_test_categories(self) -> Dict[str, List[str]]:
    categories = {
        # ... existing categories ...
        "custom": [
            "marketpipe custom-command --option1",
            "marketpipe custom-command --option2"
        ]
    }
    return categories
```

2. **Add category logic**:
```python
def _get_command_category(self, command: str) -> str:
    if "custom-command" in command:
        return "custom"
    # ... existing logic ...
```

### Custom Configuration Files

Create custom test configurations:

```yaml
# custom_test_config.yaml
config_version: "1"

alpaca:
  key: "test_key"
  secret: "test_secret"
  base_url: "https://data.alpaca.markets/v2"
  rate_limit_per_min: 200
  feed: "iex"

symbols:
  - AAPL
  - GOOGL
  - MSFT

start: "2024-01-02"
end: "2024-01-03"
output_path: "./test_data"
compression: "snappy"
workers: 1

metrics:
  enabled: false
  port: 8000
```

Then use it:
```bash
python scripts/pipeline_validator.py --config-file custom_test_config.yaml
```

## Best Practices

### Development Workflow

1. **Quick check during development**:
   ```bash
   python scripts/pipeline_validator.py --mode quick
   ```

2. **Pre-commit validation**:
   ```bash
   python scripts/pipeline_validator.py --mode critical --verbose
   ```

3. **Release validation**:
   ```bash
   python scripts/pipeline_validator.py --mode full --report-format html
   ```

### Performance Considerations

- **Quick mode**: Use for rapid iteration during development
- **Critical mode**: Balance between coverage and speed for CI/CD
- **Full mode**: Use for comprehensive testing before releases
- **Parallel execution**: Tests run sequentially but are optimized for speed

### Report Management

```bash
# Organize reports by date
mkdir -p reports/$(date +%Y%m%d)
python scripts/pipeline_validator.py --output-dir reports/$(date +%Y%m%d)

# Keep historical reports
find reports/ -name "*.json" -mtime +30 -delete  # Clean old reports
```

## Advanced Usage

### Automated Monitoring

Set up regular validation checks:

```bash
#!/bin/bash
# validation_cron.sh
cd /path/to/marketpipe
python scripts/pipeline_validator.py --mode critical --report-format json > /dev/null
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "Pipeline validation failed with exit code $EXIT_CODE"
    # Send alert email, slack notification, etc.
fi
```

Add to crontab:
```bash
# Run validation every 6 hours
0 */6 * * * /path/to/validation_cron.sh
```

### Integration with Monitoring Systems

Export metrics to monitoring systems:

```python
# Custom metrics exporter
import json

with open('validation_output/validation_report_latest.json') as f:
    report = json.load(f)

# Export to Prometheus
print(f"marketpipe_validation_pass_rate {report['summary']['pass_rate'].replace('%', '')}")
print(f"marketpipe_validation_total_tests {report['total_tests']}")
print(f"marketpipe_validation_duration_seconds {report['total_duration']}")
```

## Conclusion

The MarketPipe Pipeline Validator is a comprehensive tool for ensuring system reliability and functionality. Regular use of the validator helps:

- **Catch issues early** in the development process
- **Ensure system health** in production environments
- **Validate changes** before deployment
- **Monitor system degradation** over time
- **Provide documentation** of system capabilities

For questions or issues with the validator, check the generated reports for detailed error information and recommendations.

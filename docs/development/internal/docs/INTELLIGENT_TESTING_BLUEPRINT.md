# Intelligent Testing Blueprint - MarketPipe

## Overview

MarketPipe implements a **smart testing blueprint** that shrinks feedback loops without hiding real defects. This system combines intelligent scope detection, parallelization safety, test quarantine, and performance coaching to create an optimal testing experience for both developers and AI coding agents.

## The Smart Testing Philosophy

| **It DOES (Smart Features)**                                                                                           | **It DOES NOT (Red Lines)**                                                                                                    |
| ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| ✅ **Auto-detect scope**: Watch file changes, dependency graphs, and recent failures to run smallest relevant subset  | ❌ Assume passing subset means whole suite is green. Before merge/release, entire suite must run                                |
| ✅ **Parallelize intelligently**: Spin tests across cores, falling back to serial for unsafe fixtures                 | ❌ Force parallelism on every run. Some races only appear under load; others break under xdist                                  |
| ✅ **Tag and throttle**: Enforce markers like `unit`, `integration`, `slow`. Local excludes slow; CI includes all     | ❌ Scatter ad-hoc `@skip` everywhere. Skips pile up and silently rot code                                                      |
| ✅ **Snapshot and cache**: Store run metadata, auto-purge on branch changes, make `--lf`/`--ff` useful                | ❌ Keep stale cache forever, leading to "nothing ran" surprises after rebases                                                  |
| ✅ **Fail fast with context**: Stop after 3 failures, show minimal tracebacks with source links                      | ❌ Dump full tracebacks or hide the stack frames you need                                                                      |
| ✅ **Flaky-test quarantine**: Track flaky tests, mark with `@pytest.mark.flaky`, exclude from regular runs            | ❌ Let flaky tests pollute CI. Quarantine provides escape valve for "works on my laptop" problems                              |
| ✅ **Performance coaching**: Weekly reports on slowest tests, suggest optimization targets                             | ❌ Over-optimize microseconds prematurely; only optimize what the profile reveals                                              |

## Quick Start

### Basic Commands (Recommended Developer Workflow)

```bash
# Primary workflow - intelligent fast feedback
make test-intelligent                # Auto-detect scope, smart parallel, fast mode
make test-intelligent-dry           # See test plan without execution
make test-intelligent-all           # Full intelligent suite (includes slow tests)

# Diagnostics and maintenance
make test-diagnose                  # Environment diagnosis ("works on my laptop")
make test-flaky-alert              # Check for tests needing quarantine
make test-cache-status             # Check cache validity and branch status

# Workflows for different scenarios
make test-workflow-fast            # Complete fast workflow with auto-diagnosis
make test-workflow-ci              # CI workflow (full suite + reporting)
```

### Integration with Existing Commands

```bash
# Legacy commands that now use intelligent system
make test                          # Alias for test-intelligent
make test-smart                    # Alias for test-intelligent

# Enhanced smart commands
make test-smart-cmd               # Get pytest command for IDE integration
make test-smart-all               # Dry run analysis
```

## Core Features

### 1. Auto-Scope Detection

The system automatically determines which tests to run based on:

- **File changes**: Maps changed source files to relevant test files
- **Dependency graphs**: Includes tests affected by transitive dependencies
- **Recent failures**: Includes tests that failed in last 24 hours
- **Critical path protection**: Changes to core files trigger broader test coverage

```python
# Example: Changes to src/marketpipe/ingestion/coordinator.py automatically include:
tests/test_coordinator_flow.py        # Direct mapping
tests/test_metrics.py                 # Dependency: coordinator uses metrics
tests/test_alpaca_client.py          # Integration: coordinator orchestrates clients
```

### 2. Intelligent Parallelization

Tests are analyzed for parallel safety:

```python
# Parallel-safe tests (run with -n auto)
tests/test_alpaca_client.py          # Unit tests with HTTP mocks
tests/test_base_client.py            # Abstract base class tests
tests/test_validation.py             # Schema validation tests

# Parallel-unsafe tests (run sequentially)
tests/test_coordinator_flow.py       # Integration tests with shared state
tests/test_database_*.py             # Database isolation issues
tests/test_filesystem_*.py           # File system conflicts
```

**Detection Logic:**
- Filename patterns: `integration`, `database`, `e2e`, `system`
- Test markers: `@pytest.mark.parallel_unsafe`
- File analysis: Tests that create files, databases, or external processes

### 3. Marker-Based Test Categorization

Enhanced pytest markers enforce consistent test categorization:

```python
@pytest.mark.unit
def test_price_validation():
    """Fast, isolated unit test."""
    pass

@pytest.mark.integration
@pytest.mark.parallel_unsafe
def test_full_ingestion_pipeline():
    """Integration test requiring sequential execution."""
    pass

@pytest.mark.slow
def test_large_dataset_processing():
    """Excluded from fast feedback loops."""
    pass

@pytest.mark.flaky
@pytest.mark.reruns(3)
def test_external_api_timeout():
    """Quarantined flaky test with automatic retries."""
    pass
```

**Smart Throttling:**
- **Fast mode**: `-m "not slow and not integration"` (default for development)
- **CI mode**: Runs all tests including slow ones
- **Quarantine mode**: `-m "not flaky"` excludes problematic tests

### 4. Branch-Aware Caching

Intelligent cache management prevents stale results:

```python
# Cache validity tracking
branch_cache = {
    "main": {"last_commit": "abc1234", "last_invalidation": "2024-01-15T10:30:00Z"},
    "feature/new-client": {"last_commit": "def5678", "last_invalidation": "2024-01-15T11:45:00Z"}
}

# Auto-purge triggers
- Branch switches → Clear cache
- New commits → Validate cache
- Rebases/merges → Force refresh
```

**Cache Benefits:**
- `--lf` (last failed) works reliably across branch switches
- `--ff` (fail first) provides immediate feedback on known issues
- No "nothing ran" surprises after git operations

### 5. Flaky Test Quarantine System

Track and isolate unreliable tests:

```python
# Flaky test detection
flaky_score = (failure_count / total_runs) * exponential_decay_factor
if flaky_score > 0.05:  # 5% failure rate threshold
    alert_for_quarantine(test_name, flaky_score)

# Quarantine process
1. Add @pytest.mark.flaky to unreliable tests
2. Exclude from regular runs: pytest -m "not flaky"
3. Test separately with retries: pytest -m "flaky --reruns 3"
4. Investigate root cause during dedicated time
```

**Benefits:**
- Prevents flaky tests from blocking development
- Provides escape valve for "works on my laptop" issues
- Maintains visibility of flaky tests without disruption
- Enables systematic fixing of reliability issues

## Developer Experience

### Pleasant Workflows

**Fast Development Loop:**
```bash
# 1. Make changes to source code
vim src/marketpipe/ingestion/coordinator.py

# 2. Run intelligent tests (1-3 seconds feedback)
make test-intelligent
# 🧠 Running intelligent test system...
# 🚀 Parallel execution enabled
# 📚 Using cached test results
# ⚡ Fast mode: excluding slow and integration tests
# ✅ Tests passed in 2.1s

# 3. If failures, automatic diagnosis
make test-workflow-fast
# ❌ Tests failed, running diagnostics...
# 🔧 Environment diagnosis...
# 🚨 Flaky test alerts...
```

**Pre-Commit Safety:**
```bash
# Automatic via git hooks
git commit -m "Add new coordinator feature"
# 🔍 Analyzing changed files...
# 🎯 Running 12 relevant tests...
# ✅ All tests passed, proceeding with commit
```

**CI Integration:**
```bash
# Full suite with reporting
make test-workflow-ci
# 🏗️ CI testing workflow...
# 🧠 Running full intelligent test suite...
# 📊 Performance report updated
# ✅ CI tests passed
```

### IDE Integration

Get pytest commands for IDE test runners:

```bash
make test-smart-cmd
# python -m pytest tests/test_coordinator.py tests/test_metrics.py -n auto --maxfail=3 --tb=short -q --disable-warnings -m "not slow and not integration" --lf --ff
```

### Self-Diagnosis Features

Comprehensive environment checking:

```bash
make test-env-check
# 🔍 Comprehensive environment check...
# 🔧 Environment Diagnosis:
#   python_version: 3.11.5
#   virtual_env: ✅ marketpipe-venv
#   git_branch: feature/intelligent-testing
#   git_commit: a1b2c3d4
#   pytest_version: 7.4.3
#   cache_status: ✅ Valid
#
# 📦 Key test dependencies:
#   pytest: 7.4.3
#   pytest-xdist: 3.5.0
#   pytest-asyncio: 0.21.1
```

## Configuration

### pytest.ini Enhancement

```ini
[tool:pytest]
# Smart testing markers
markers =
    unit: Unit tests - fast, isolated, no external dependencies
    integration: Integration tests - may require database, external services
    slow: Tests that take >2 seconds (excluded from fast feedback loops)
    flaky: Tests with known reliability issues (quarantined for special handling)
    parallel_unsafe: Tests that cannot run safely in parallel

# Timeout protection (prevents hanging tests)
timeout = 30
timeout_method = thread

# Async test support
asyncio_mode = auto

# Cache configuration
cache_dir = .pytest_cache
```

### Environment Variables

```bash
# Control test automation
export SKIP_TESTS=1                    # Skip pre-commit tests
export PYTEST_ARGS="--verbose"         # Additional pytest arguments
export INTELLIGENT_TEST_MODE="full"    # Override fast mode

# Performance tuning
export MAX_PARALLEL_WORKERS=4          # Limit parallel execution
export TEST_TIMEOUT=60                 # Custom timeout for slow environments
```

## Benefits

### For Developers

1. **Faster Feedback**: 1-3 second test cycles instead of 30+ seconds
2. **Intelligent Scope**: Only runs tests relevant to changes
3. **Reliable Results**: Branch-aware caching prevents stale results
4. **Pleasant Experience**: Clear output, fail-fast, auto-diagnosis
5. **Safety Nets**: Multiple fallbacks prevent missed tests

### For AI Coding Agents

1. **Context Awareness**: Understands which tests matter for each change
2. **Efficient Resource Use**: Avoids unnecessary test execution
3. **Clear Feedback**: Structured output for decision making
4. **Performance Insights**: Data-driven optimization suggestions
5. **Escape Hatches**: Options for urgent fixes and debugging

### For Teams

1. **Consistent Quality**: Enforced markers and categorization
2. **Performance Visibility**: Regular optimization opportunities
3. **Reliability Tracking**: Systematic handling of flaky tests
4. **CI Efficiency**: Smart parallelization reduces build times
5. **Knowledge Sharing**: Self-documenting test organization

## Troubleshooting

### Common Issues

**"Nothing ran" after git operations:**
```bash
# Check cache status
make test-cache-status

# Manual cache clear if needed
make test-cache-clear
```

**Parallel test failures:**
```bash
# Check for parallel-unsafe tests
grep -r "parallel_unsafe" tests/

# Add marker to problematic tests
@pytest.mark.parallel_unsafe
```

**Environment issues:**
```bash
# Comprehensive diagnosis
make test-env-check

# Verify dependencies
python -m pytest --version
python -c "import pytest_xdist, pytest_asyncio; print('All dependencies available')"
```

---

*This system embodies the principle: "Act like a performance coach - highlight weak spots but never skip the full workout when it matters."*

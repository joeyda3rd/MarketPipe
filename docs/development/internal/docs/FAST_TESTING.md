# Fast Pytest Loop - MarketPipe Development Guide

## 📋 Purpose

Speed up local development cycles while ensuring the full test suite guards the main branch. Fast feedback is essential to productive development.

## ⚡ Quick Commands

### Smart Test Commands (🆕 Recommended)
```bash
# Run tests relevant to your changed files (smartest choice)
make test-smart

# See what tests would run without executing them
make test-smart-all

# Get pytest command for copy-paste or IDE integration
make test-smart-cmd
```

### Local Development (Fast Feedback)
```bash
# Default fast loop: last failed tests, fail-fast, parallel, skip slow tests
make test

# Alternative: no cache, first failure mode
make test-fast

# Unit tests only (fastest)
make test-unit

# Integration tests (slower, but thorough)
make test-integration

# Show test timing (run weekly to identify bottlenecks)
make test-timing
```

### Before Pushing (Complete Validation)
```bash
# ALWAYS run this before pushing to main
make test-all

# Or run full CI simulation
make ci-check
```

### Direct pytest Commands
```bash
# Fast local loop
pytest -q --disable-warnings --lf --maxfail=3 -n auto -m "not slow and not integration"

# Full suite (CI equivalent)
pytest -q

# First failure mode (no last-failed cache)
pytest -q --ff --maxfail=3 -n auto -m "not slow and not integration"

# Show slowest tests
pytest --durations=20 -q
```

## 🏷️ Test Markers

MarketPipe uses these pytest markers for categorization:

| Marker | Purpose | Typical Runtime | Local Dev |
|--------|---------|----------------|-----------|
| `unit` | Fast, isolated tests | < 100ms | ✅ Include |
| `integration` | E2E tests with services | 1-30s | ❌ Skip by default |
| `slow` | Performance/load tests | > 1s | ❌ Skip by default |
| `postgres` | Requires PostgreSQL | Variable | ❌ Skip by default |
| `flaky` | May need reruns | Variable | ⚠️ Use with care |

### Usage Examples
```bash
# Run only fast unit tests
pytest -m "unit"

# Skip slow and integration tests (default for fast loop)
pytest -m "not slow and not integration"

# Run only integration tests (when needed)
pytest -m "integration"

# Skip database-dependent tests
pytest -m "not postgres"
```

## 📊 Performance Guidelines

### Fast Loop Targets
- **Unit tests**: < 100ms each
- **Total fast loop**: < 30 seconds
- **Parallel execution**: 2-8 cores (auto-detected)

### When to Use Each Command

| Scenario | Command | Rationale |
|----------|---------|-----------|
| TDD/debugging specific feature | `pytest -q --lf -k "test_feature"` | Focus on current work |
| After fixing failing tests | `make test` | Verify fixes with fast feedback |
| Before committing | `make test-all` | Ensure nothing is broken |
| Weekly performance check | `make test-timing` | Identify performance regressions |
| Debugging race conditions | Remove `-n auto` | Disable parallel execution |

## 🛠️ Configuration Details

### pytest.ini Optimizations
```ini
[pytest]
# Optimized for fast feedback
addopts = -ra --strict-markers --tb=short -q
xfail_strict = true
cache_dir = .pytest_cache  # Enables --lf (last failed)
```

**Key settings:**
- `-ra`: Show all results except passed (faster output)
- `--strict-markers`: Catch typos in marker names
- `--tb=short`: Concise tracebacks
- `xfail_strict = true`: Expected failures must actually fail

### Parallel Execution
```bash
# Auto-detect CPU cores (recommended)
pytest -n auto

# Specific core count
pytest -n 4

# Disable parallel (for debugging race conditions)
pytest  # No -n flag
```

## 🚨 CI Rules and Guardrails

### CI Must Run Full Suite
```bash
# ✅ Correct CI command
pytest -q

# ❌ Never use these in CI
pytest --lf    # Would skip tests
pytest -n auto  # May hide race conditions
pytest -m "not slow"  # Would skip important tests
```

### Guardrails
1. **Use `skipif` over `skip`** for conditional test execution
2. **Filter warnings precisely** - don't blanket ignore everything
3. **Prune `.pytest_cache` only to debug flakiness** - normally leave it alone
4. **Always run `make test-all` before pushing**

## 📁 File Organization

### Test Structure
```
tests/
├── unit/           # Fast, isolated tests (< 100ms each)
├── integration/    # E2E tests with services (> 1s each)
├── cli/           # CLI-specific tests
├── fakes/         # Test doubles and mocks
└── resources/     # Test data and fixtures
```

### Marker Usage
```python
# Unit test (fast)
def test_price_calculation():
    assert calculate_price(100, 0.1) == 110

# Integration test (slow)
@pytest.mark.integration
def test_full_pipeline_flow():
    # Test that takes 5+ seconds

# Slow test (performance)
@pytest.mark.slow
def test_large_dataset_processing():
    # Test with large data sets

# Flaky test (needs retry)
@pytest.mark.flaky
def test_external_api_integration():
    # May fail due to network issues
```

## 🔧 Debugging Tips

### Common Issues

**Tests taking too long?**
```bash
# Identify slow tests
make test-timing

# Run without parallel execution to debug
pytest -q --disable-warnings --lf --maxfail=3 -m "not slow and not integration"
```

**Race conditions in parallel mode?**
```bash
# Disable parallel execution
pytest -q --disable-warnings --lf --maxfail=3 -m "not slow and not integration"  # Remove -n auto
```

**Want to focus on specific test?**
```bash
# Use keyword filtering
pytest -q -k "test_alpaca" --lf

# Or run specific file
pytest tests/unit/test_alpaca_client.py -q --lf
```

**Cache causing issues?**
```bash
# Clear pytest cache
rm -rf .pytest_cache

# Or use first-failure mode
pytest -q --ff
```

## 📈 Development Workflow

### Recommended TDD Cycle
1. **Write failing test**: `pytest -q -k "test_new_feature" --lf`
2. **Make it pass**: `pytest -q --lf --maxfail=1`
3. **Refactor**: `make test` (run fast suite)
4. **Before commit**: `make test-all` (full validation)
5. **Weekly**: `make test-timing` (performance check)

### Performance Monitoring
```bash
# Check test performance weekly
make test-timing

# Look for tests taking > 1 second
pytest --durations=0 | grep -E "\s+[1-9][0-9]*\.[0-9]+s"
```

## 🎯 Best Practices

### Test Writing
- Mark tests appropriately with `@pytest.mark.{unit|integration|slow}`
- Keep unit tests under 100ms each
- Use `@pytest.mark.flaky` sparingly and fix underlying issues
- Prefer `pytest.mark.skipif(condition)` over `pytest.skip()`

### Development Workflow
- Use `make test` for rapid iteration
- Always run `make test-all` before pushing
- Run `make test-timing` weekly to catch performance regressions
- Use `-k` filtering to focus on specific areas during development

### CI/CD
- CI must run `pytest -q` (no shortcuts)
- Never use `--lf`, `-m` filters, or `-n auto` in CI
- Set up branch protection to require full test suite passage

This approach ensures you get fast feedback during development while maintaining comprehensive test coverage for production code.

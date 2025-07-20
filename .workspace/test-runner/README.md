# Smart Test Runner

This directory contains the smart test runner used by the pre-commit hook to provide fast feedback during development.

## Purpose

The `smart_test.py` script runs a curated subset of fast tests before each commit to catch basic issues quickly without running the full test suite.

## What it runs

- Core unit tests from `tests/unit/domain/` and `tests/unit/config/`
- Key API client tests (`tests/test_base_client.py`)
- Main module tests (`tests/unit/test_main.py`)
- DDD validation tests (`tests/unit/test_ddd_guard_rails.py`)

The runner specifically excludes:
- Async tests (marked with `asyncio`) to reduce overhead
- Integration tests that require external dependencies
- Slow tests that take more than a few seconds

## Usage

The script is automatically run by the pre-commit hook, but you can also run it manually:

```bash
python .workspace/test-runner/smart_test.py
```

## Performance

The test runner is optimized for speed:
- Runs ~126 tests in ~2 seconds
- Uses `--maxfail=1` to stop on first failure
- Disables coverage collection for speed
- Uses minimal output formatting

## Full testing

For comprehensive testing, use:
```bash
make test           # Run all tests
pytest              # Run all tests with default settings
pytest tests/unit/  # Run just unit tests
```

## Disabling

If the pre-commit hook becomes annoying:
```bash
# Skip once
git commit --no-verify

# Disable permanently
rm .git/hooks/pre-commit
``` 
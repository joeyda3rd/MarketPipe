# Coverage Fix Summary

## Problem

GitHub Actions CI was stalling for 4½ hours due to coverage.py throwing:
```
coverage.exceptions.DataError: Can't combine statement coverage data with branch data
```

The exception left Python processes alive, causing CI runners to never exit.

## Solution

Switched to **parallel, branch-aware coverage** with dedicated combination job.

## Changes Made

### 1. Created `.coveragerc`
- Enabled `parallel = True` and `branch = True`
- Set data files to `tmp/coverage/.coverage`
- Moved all coverage configuration from `pyproject.toml` to `.coveragerc`

### 2. Updated `pyproject.toml`
- Pinned `coverage>=7.5.0` and `pytest-cov>=5.0.0`
- Removed old coverage configuration to avoid conflicts

### 3. Modified `.github/workflows/ci.yml`
- Added `workflow_dispatch` trigger for manual runs
- Added `timeout-minutes: 30` to prevent infinite hangs
- Removed `--cov-append` flags (causes the original issue)
- Added `--cov-branch` flag for consistent branch coverage
- Changed artifact paths to `tmp/coverage` directory
- Created dedicated `coverage-report` job that:
  - Downloads all coverage artifacts
  - Combines `.coverage.*` files using `coverage combine`
  - Generates XML and HTML reports
  - Uploads combined artifacts

## Expected Results

- Test jobs finish in minutes, no hangs
- Each job creates separate `.coverage.*` files in `tmp/coverage`
- `coverage-report` job combines all data and generates unified reports
- Total workflow time < 10 minutes
- Ready for optional Codecov integration

## Usage

```bash
# Manual trigger
gh workflow run ci.yml

# Check latest run
gh run list --limit 1

# Re-run if needed
gh run rerun --last
``` 
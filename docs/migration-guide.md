# Migration Guide: Pre-commit Framework & Test Organization

This guide helps existing MarketPipe developers migrate to the new pre-commit framework and test organization.

## What Changed

### Pre-commit Framework
- **Old**: Manual git hooks + manual quality checks
- **New**: Managed pre-commit framework with comprehensive hooks
- **Benefits**: Automatic code formatting, consistent quality checks, faster feedback

### Test Organization
- **Old**: Path-based test selection
- **New**: Marker-based test organization with clear categories
- **Benefits**: Flexible test execution, faster development feedback

### New Test Scripts
- `scripts/pre-commit-tests` - Ultra-fast tests (~2s) for pre-commit hooks
- `scripts/test-fast` - Fast tests (~3s) for development feedback
- `scripts/test-full` - Complete test suite with coverage
- `scripts/test-ci` - Simulate CI environment locally

## Migration Steps

### 1. Install Pre-commit Framework

```bash
# Install pre-commit
pip install pre-commit

# Install hooks (this will backup your existing pre-commit hook)
pre-commit install

# If you had a custom pre-commit hook, check:
ls -la .git/hooks/pre-commit*
# Your old hook is saved as .git/hooks/pre-commit.legacy
```

### 2. Update Your Development Workflow

#### Old Workflow
```bash
# Before
git add .
black src/ tests/
ruff check src/ tests/
mypy src/marketpipe/
pytest tests/unit/
git commit -m "changes"
```

#### New Workflow
```bash
# After - much simpler!
git add .
git commit -m "changes"  # All quality checks run automatically!

# Or during development:
scripts/test-fast        # Quick feedback
scripts/format           # Manual formatting if needed
```

### 3. Understanding the New Test Markers

Update your test commands:

#### Speed-based Testing
```bash
# Old way - path-based
pytest tests/unit/                    # All unit tests
pytest tests/test_base_client.py      # Specific file

# New way - marker-based
pytest -m fast                        # Ultra-fast tests only
pytest -m unit                        # Unit tests
pytest -m integration                 # Integration tests
```

#### Domain-specific Testing
```bash
# New marker categories
pytest -m api_client                  # API client tests
pytest -m config                      # Configuration tests
pytest -m database                    # Database tests
pytest -m cli                        # CLI tests
```

### 4. Update Your Scripts and Automation

#### CI/CD Scripts
```bash
# Old
pytest tests/unit/ -x --tb=short

# New options
scripts/test-fast                     # Development feedback
scripts/test-full                     # Complete suite
scripts/test-ci                       # CI simulation
pytest -m fast -x --tb=short         # Marker-based
```

#### Local Development
```bash
# Replace old commands with new scripts
alias test-quick="scripts/test-fast"
alias test-all="scripts/test-full"
alias ci-local="scripts/test-ci"
```

## New Pre-commit Hooks

The pre-commit framework now runs these checks automatically:

### Code Quality
- **Black**: Code formatting (line length 100)
- **isort**: Import sorting
- **Ruff**: Linting (replaces flake8)
- **MyPy**: Type checking (src/ only)

### Security & Safety
- **Bandit**: Security vulnerability scanning
- **AST check**: Python syntax validation
- **Debug statements**: Prevent debug statements in commits

### File Quality
- **Trailing whitespace**: Automatically removed
- **End of file**: Ensures files end with newline
- **Large files**: Prevents accidental large file commits
- **YAML/JSON**: Syntax validation

### Project-specific
- **Fast tests**: Runs ultra-fast test suite (~2s)
- **Config validation**: Validates YAML configuration files
- **No placeholders**: Prevents placeholder values in configs

## Performance Comparison

### Before
```bash
# Manual quality checks
black src/ tests/          # ~5s
ruff check src/ tests/     # ~3s
mypy src/marketpipe/       # ~10s
pytest tests/unit/         # ~30s
# Total: ~48s
```

### After
```bash
# Pre-commit hooks (parallel execution)
git commit                 # ~15s total (all checks)

# Development workflow
scripts/test-fast          # ~3s
scripts/pre-commit-tests   # ~2s
# Much faster feedback!
```

## Troubleshooting

### Pre-commit Installation Issues
```bash
# If hooks don't install properly
pre-commit uninstall
pre-commit install --install-hooks

# Update hook repositories
pre-commit autoupdate
```

### Hook Failures
```bash
# See what failed
git status

# Run specific hook manually
pre-commit run black
pre-commit run ruff
pre-commit run fast-tests

# Skip hooks for emergency commits
git commit --no-verify -m "Emergency fix"
```

### Test Marker Issues
```bash
# If no tests are selected
pytest -m fast --collect-only        # See what tests match

# List all available markers
pytest --markers

# Run without markers (old way still works)
pytest tests/unit/test_main.py
```

### Performance Issues
```bash
# Clean pre-commit cache
pre-commit clean

# Run with timing information
pre-commit run --all-files --verbose
```

## Migration Checklist

For existing developers:

- [ ] Install pre-commit: `pip install pre-commit`
- [ ] Install hooks: `pre-commit install`
- [ ] Test the workflow: Make a small change and commit
- [ ] Update your scripts to use new `scripts/test-*` commands
- [ ] Learn the new test markers: `pytest --markers`
- [ ] Update CI/CD scripts if needed
- [ ] Share this guide with your team!

## Rollback (If Needed)

If you need to temporarily roll back:

```bash
# Uninstall pre-commit hooks
pre-commit uninstall

# Restore your old hook (if you had one)
mv .git/hooks/pre-commit.legacy .git/hooks/pre-commit

# Use old testing methods
pytest tests/unit/
black src/ tests/
ruff check src/ tests/
```

## Getting Help

- **Documentation**: See [pre-commit.md](pre-commit.md) for detailed setup
- **Issues**: Open a GitHub issue if you encounter problems
- **Questions**: Ask in team discussions or code reviews

The new workflow is designed to make development faster and more consistent. After a brief adjustment period, you should find it much more productive!

---
description: Smart test automation system for MarketPipe - ensures appropriate tests are never missed
globs:
  - 'src/**/*.py'
  - 'tests/**/*.py'
  - 'scripts/*.py'
alwaysApply: true
priority: high
---

# Testing Automation - Simplified Smart Testing

## Objective
Provide fast feedback loops for MarketPipe development while maintaining comprehensive test coverage protection.

## Primary Testing Commands

### Main Commands (Use These)
```bash
make test           # Smart test runner - auto-detects relevant tests
make test-all       # Complete test suite (use before pushing)
make test-show      # Show what tests would run (dry run)
make test-cmd       # Get pytest command for IDE integration
```

### Specialized Commands
```bash
make test-thorough  # Detailed output for debugging
make test-unit      # Unit tests only
make test-integration # Integration tests only
make test-timing    # Performance analysis
```

### Cache Management
```bash
make test-cache-status  # Check cache and git status
make test-cache-clear   # Clear pytest cache
```

## Smart Test Selection

The `scripts/test_runner.py` automatically detects which tests to run based on:

1. **File mappings**: Direct relationships between source and test files
2. **Dependency analysis**: Transitive dependencies and imports
3. **Safety nets**: Fallback to broader test suites for critical files
4. **Git integration**: Detects changed files since last commit

## Test Organization

Use pytest markers to organize tests:

```python
@pytest.mark.unit          # Fast, isolated tests
@pytest.mark.integration   # Slower tests with external dependencies
@pytest.mark.slow          # Tests taking >5 seconds
@pytest.mark.async         # Async tests
@pytest.mark.parallel_unsafe # Cannot run in parallel
@pytest.mark.flaky         # Quarantined unreliable tests
```

## Common Workflows

### Development Loop
```bash
# 1. Make changes to code
# 2. Run relevant tests (1-3 seconds)
make test

# 3. If tests pass, continue development
# 4. Before pushing, run full suite
make test-all
```

### Debugging Failed Tests
```bash
# 1. See what tests would run
make test-show

# 2. Run with detailed output
make test-thorough

# 3. Get pytest command for IDE
make test-cmd
```

### Performance Analysis
```bash
# Weekly: Check for slow tests
make test-timing

# Check cache status
make test-cache-status
```

## Parallel Testing with Async Code

The system automatically handles parallel execution:

- **Safe by default**: Integration tests run sequentially
- **Auto-detection**: Unit tests run in parallel when safe
- **Override available**: Use `@pytest.mark.parallel_unsafe` for edge cases
- **Async support**: `asyncio_mode = auto` handles async tests properly

## IDE Integration

Get pytest commands for your IDE:
```bash
make test-cmd
# Outputs: python -m pytest tests/unit/test_specific.py -v
```

## Safety and Coverage

### Multiple Safety Nets
1. **Pattern matching**: File changes → specific tests
2. **Dependency analysis**: Import relationships → related tests
3. **Safety fallbacks**: Unknown files → unit test coverage
4. **Critical path protection**: Core files → full test suite
5. **Human oversight**: Dry run shows test plan before execution

### Never Assumes Subset = Complete
- Smart selection for **development speed**
- Always run `make test-all` before pushing to main
- CI runs complete suite regardless of local optimizations

## Advanced Features (Optional)

For power users who want advanced testing capabilities, see `.workspace/README.md` for:

- Intelligent test selection with ML-based optimization
- Flaky test detection and quarantine
- Performance profiling and analytics
- Advanced coverage analysis
- Historical test performance tracking

The workspace tools enhance but don't replace the main testing system.

## Troubleshooting

### Tests Not Running as Expected
```bash
# 1. Check what tests would run
make test-show

# 2. Check cache status
make test-cache-status

# 3. Clear cache if needed
make test-cache-clear

# 4. Run specific tests directly
python -m pytest tests/specific_test.py -v
```

### Performance Issues
```bash
# Check for slow tests
make test-timing

# Run unit tests only (fastest)
make test-unit
```

### Environment Issues
```bash
# Check basic setup
python -m pytest --version
python -c "import pytest_xdist; print('Parallel execution available')"
python -c "import pytest_asyncio; print('Async support available')"
```

## Migration Notes

### From Previous Complex Setup
- `make test-smart` → `make test` (simplified)
- `make test-intelligent` → `make test` (consolidated)
- Multiple overlapping commands → Single clean interface
- Complex configuration → Simple, focused pytest.ini

### Preserving Workflows
- All essential functionality preserved
- Faster execution through simplification
- Advanced features moved to optional workspace
- No disruption to contributor workflows

## Best Practices

1. **Default to `make test`** for development
2. **Always `make test-all`** before pushing
3. **Use `make test-show`** when unsure what will run
4. **Check `make test-timing`** weekly for performance
5. **Keep workspace separate** for experimental features

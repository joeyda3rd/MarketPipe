# Documentation

## Getting Started

- **[Getting Started Guide](GETTING_STARTED.md)** - Complete setup and first steps
- **[Main README](../README.md)** - Project overview and quick start
- **[Contributing Guide](../CONTRIBUTING.md)** - Development workflow and guidelines

## Development

- **[Pre-commit Framework](pre-commit.md)** - Code quality and automated checks setup
- **[Migration Guide](migration-guide.md)** - Migrating to new pre-commit and test framework
- **[CLI Commands Reference](CLI_COMMANDS_REFERENCE.md)** - Complete command documentation
- **[Testing](#testing)** - Test organization and markers

## Configuration

- **[Environment Variables](ENVIRONMENT_VARIABLES.md)** - Complete reference for all environment variables
- **[Environment Variables Quick Reference](ENV_VARIABLES_QUICK_REFERENCE.md)** - Quick lookup table
- **[Provider Environment Map](provider_env_map.yaml)** - YAML reference for data provider credentials

## Monitoring

- **[Monitoring Guide](MONITORING.md)** - Observability, metrics, and monitoring setup
- **[Grafana Dashboard](grafana_dashboard.json)** - Pre-built monitoring dashboard

## Testing

MarketPipe uses a comprehensive test suite with clear organization:

### Test Markers
Tests are organized with pytest markers for flexible execution:

```bash
# Speed-based markers
pytest -m fast          # Ultra-fast tests (<2s total)
pytest -m unit           # Unit tests (fast, isolated)
pytest -m integration    # Integration tests (slower)
pytest -m slow           # Tests taking >5 seconds

# Domain-specific markers
pytest -m api_client     # API client and connector tests
pytest -m config         # Configuration tests
pytest -m database       # Database interaction tests
pytest -m cli           # Command-line interface tests
```

### Test Scripts
```bash
scripts/pre-commit-tests  # Ultra-fast (~2s) - used by pre-commit hooks
scripts/test-fast        # Fast tests (~3s) - development feedback
scripts/test-full        # Complete suite with coverage
scripts/test-ci          # Simulate CI environment locally
```

## Examples

Check out [examples/](../examples/) for usage examples.

## Code Structure

The code is organized into:
- `src/marketpipe/` - Main source code following Domain-Driven Design
- `tests/` - Tests mirror the source structure with clear markers
- `scripts/` - Development and testing scripts
- `tools/` - Database and development utilities

## Need Help?

- Start with the [Getting Started Guide](GETTING_STARTED.md)
- Look at the code - it's designed to be readable
- Check the examples directory
- Review the [pre-commit setup](pre-commit.md) for development
- Open an issue for questions

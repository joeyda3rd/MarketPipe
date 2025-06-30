# Development Scripts

Useful scripts for MarketPipe development and usage.

## Quick Start

```bash
scripts/setup       # One-command setup for new contributors
scripts/demo        # Quick demo with sample data
scripts/health-check # Verify everything is working
```

## Available Scripts

### Setup & Health
- **`scripts/setup`** - Complete development environment setup
  - Installs dependencies
  - Creates sample config and .env files
  - Runs health check
- **`scripts/health-check`** - Verify installation and dependencies

### Development Workflow
- **`scripts/format`** - Format code with Black/Ruff
- **`scripts/watch`** - Auto-run tests when files change
- **`scripts/clean`** - Remove generated files and caches

### Demo & Usage
- **`scripts/demo`** - Quick demo with fake data (no API keys needed)

## Usage Examples

```bash
# New contributor setup
scripts/setup

# Development workflow
scripts/watch        # In one terminal
scripts/format       # Before committing

# Try MarketPipe
scripts/demo

# Clean up
scripts/clean
scripts/clean --all  # Including workspace
```

## Advanced Tools

Advanced development tools are in `.workspace/dev-tools/` - these are optional and not required for contributing. 
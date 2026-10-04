# MarketPipe Development Commands

.PHONY: test test-all test-unit test-integration test-timing test-coverage test-watch test-ci fmt lint type-check arch-check clean help install dev-setup demo

# Default target
help:
	@echo "MarketPipe Development Commands:"
	@echo ""
	@echo "Testing:"
	@echo "  make test           - Run tests (with common optimizations)"
	@echo "  make test-all       - Run complete test suite"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make test-timing    - Show test timings"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make test-watch     - Auto-run tests on file changes"
	@echo "  make test-ci        - Simulate CI environment locally"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt           - Format code with black"
	@echo "  make lint          - Run linting"
	@echo "  make type-check    - Run type checking with mypy"
	@echo "  make arch-check    - Check architecture boundaries"
	@echo "  make check         - Run all code quality checks"
	@echo ""
	@echo "Development:"
	@echo "  make dev-setup     - Complete development setup"
	@echo "  make demo          - Run quick demo"
	@echo "  make install       - Install in development mode"
	@echo "  make clean         - Clean up cache files"

# Primary test command - fast, smart defaults
test:
	@python -m pytest -x --ff --tb=short

# Complete test suite
test-all:
	@python -m pytest --tb=short

# Unit tests only
test-unit:
	@python -m pytest tests/ -m "unit" --tb=short

# Integration tests only
test-integration:
	@python -m pytest tests/ -m "integration" --tb=short

# Show test timing information
test-timing:
	@python -m pytest --durations=10 --tb=no -q

# Run tests with coverage report
test-coverage:
	@if python3 -c "import pytest_cov" 2>/dev/null; then \
		echo "📊 Running tests with coverage..."; \
		python -m pytest --cov=src/marketpipe --cov-report=html --cov-report=term-missing; \
		echo "📈 Coverage report: htmlcov/index.html"; \
	else \
		echo "⚠️  pytest-cov not found - install with: pip install pytest-cov"; \
		echo "   Or install test dependencies: pip install -e .[test]"; \
	fi

# Auto-run tests on file changes (using scripts/watch)
test-watch:
	@scripts/watch

# Simulate CI environment locally
test-ci:
	@echo "🤖 Simulating CI environment..."
	@python -m pytest --tb=short --strict-markers --cov=src/marketpipe --cov-fail-under=80

# Format code
fmt:
	@if command -v black >/dev/null 2>&1; then \
		echo "🎨 Formatting with Black..."; \
		black src/ tests/ examples/ --line-length 100; \
	else \
		echo "⚠️  Black not found - install with: pip install black"; \
	fi

# Lint code
lint:
	@if command -v ruff >/dev/null 2>&1; then \
		echo "🔍 Linting with Ruff..."; \
		ruff check src/ tests/ examples/; \
	else \
		echo "⚠️  Ruff not found - install with: pip install ruff"; \
	fi

# Type checking with mypy
type-check:
	@if command -v mypy >/dev/null 2>&1; then \
		echo "🔍 Running type checks..."; \
		mypy src/marketpipe --ignore-missing-imports; \
	elif python3 -c "import mypy" 2>/dev/null; then \
		echo "🔍 Running type checks..."; \
		python3 -m mypy src/marketpipe --ignore-missing-imports; \
	else \
		echo "⚠️  mypy not found - install with: pip install mypy"; \
	fi

# Check architecture boundaries
arch-check:
	@if command -v lint-imports >/dev/null 2>&1; then \
		echo "🏗️  Checking architecture boundaries..."; \
		lint-imports --config pyproject.toml; \
	else \
		echo "⚠️  import-linter not found - install with: pip install import-linter"; \
	fi

# Run all code quality checks
check:
	@echo "🔧 Running code quality checks..."
	@$(MAKE) fmt
	@$(MAKE) lint
	@$(MAKE) type-check
	@$(MAKE) arch-check
	@echo "✅ Code quality checks complete"

# Complete development setup (using scripts/setup)
dev-setup:
	@scripts/setup

# Run demo (using scripts/demo)
demo:
	@scripts/demo

# Clean cache files
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf .coverage htmlcov/ 2>/dev/null || true
	@rm -rf build/ dist/ *.egg-info/ 2>/dev/null || true

# Install in development mode
install:
	@pip install -e .

# Deterministic installed distribution journeys; no provider credentials required.
.PHONY: test-e2e test-integration-full release-rehearsal
test-e2e:
	@python -m pytest tests/e2e -m "not postgres" --strict-markers --timeout=90 --durations=15

test-integration-full:
	@python -m pytest tests/integration -m "not postgres" --strict-markers --timeout=120 --durations=25

release-rehearsal:
	@python -m build
	@python -m twine check --strict dist/*
	@python -m pytest tests/e2e -m "not postgres" --strict-markers --timeout=90

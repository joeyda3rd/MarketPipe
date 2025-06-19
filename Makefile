# MarketPipe Development Makefile

.PHONY: help install-hooks coverage test lint format clean

help: ## Show this help message
	@echo "MarketPipe Development Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Cleanup
clean: ## Clean up temporary files and development artifacts
	@bash scripts/cleanup-root.sh

# Development Setup
install-hooks: ## Install Git hooks for development
	@echo "🔗 Installing Git hooks..."
	@echo "✅ Git hooks ready (no hooks currently configured)"

# Testing & Quality
test: ## Run all tests
	python -m pytest tests/ -v

coverage: ## Run tests with coverage report
	python -m pytest --cov=src/marketpipe --cov-report=html --cov-report=term tests/
	@echo "📊 Coverage report generated in htmlcov/"

lint: ## Run linting and type checking
	python -m black src/ tests/ --check
	python -m ruff check src/ tests/
	python -m mypy src/

format: ## Format code with black and ruff
	python -m black src/ tests/
	python -m ruff check src/ tests/ --fix

# CLI Commands
ingest: ## Run ingestion with example config (requires config file)
	python -m marketpipe ingest --config config/example_config.yaml

validate: ## Run validation on existing data
	python -m marketpipe validate --input ./data

metrics: ## Start metrics server
	python -m marketpipe metrics --port 8000

# Development Workflow
dev-setup: install-hooks ## Complete development setup
	@echo "🚀 Development environment ready!"
	@echo "Run 'make help' to see available commands"

# Continuous Integration simulation
ci-check: lint test coverage ## Run all CI checks locally
	@echo "✅ All CI checks passed!" 
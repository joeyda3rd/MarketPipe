# Repository Guidelines

## Project Structure & Module Organization
- Source code: `src/marketpipe` organized by DDD layers: `domain/`, `application/`, `infrastructure/`, plus feature areas like `ingestion/`, `validation/`, `aggregation/`, and `cli/`.
- Tests: `tests/` with `unit/`, `integration/`, and domain-specific suites; shared fixtures in `tests/conftest.py`.
- Config & assets: `config/`, `data/`, `schema/`, `alembic/` (DB migrations), `docker/`, `docs/`, `examples/`, `scripts/`.
- CLI entry points: `mp` and `marketpipe` (see `src/marketpipe/cli`).

## Build, Test, and Development Commands
- `pip install -e .[dev]`: Source install with dev tools (contributors).
- `make install`: Editable install.
- `make dev-setup`: One-time setup (tools, hooks). Use `scripts/setup.sh` if calling directly.
- `make test` / `make test-all`: Run fast/default or full pytest suite.
- `make test-unit` / `make test-integration`: Filtered suites by marker.
- `make test-coverage`: Coverage report (`htmlcov/index.html`).
- `make fmt` / `make lint` / `make type-check` / `make check`: Format, lint, type-check, and all checks.
- `make arch-check`: Validate DDD boundaries (import-linter via `pyproject.toml`).
- CLI help: `marketpipe --help` or `python -m marketpipe.cli --help`.

## Coding Style & Naming Conventions
- Python 3.9+; type hints encouraged. Line length 100.
- Format with Black; import order via isort (Black profile); lint with Ruff; type-check with mypy.
- Run locally: `pre-commit install` then commit; or `make check`.
- Naming: packages/modules `snake_case`, classes `PascalCase`, functions/variables `snake_case`; tests `test_*.py`.

## Testing Guidelines
- Framework: pytest with markers (`unit`, `integration`, `fast`, `slow`, etc.).
- Naming: files `test_*.py`, classes `Test*`, functions `test_*` (see `pyproject.toml`).
- Quick runs: `make test`; targeted: `pytest -m "unit and not slow"`.
- CI parity: `make test-ci` enforces `--strict-markers` and coverage threshold (~80%).

## Architecture Rules
- Domain layer must not import `infrastructure/` or application services.
- Contracts live in `pyproject.toml` (`[tool.importlinter.*]`).
- Prefer `make arch-check` (equivalent to `lint-imports --config pyproject.toml`).

## Commit & Pull Request Guidelines
- Conventional Commits (examples from history): `feat: ...`, `fix: ...`, `docs: ...`, `style: ...`.
- PRs should include: clear description, linked issue, test coverage for changes, docs/CLI help updates if applicable, and passing CI (`make check`, `make test-all`).

## Security & Configuration Tips
- Copy `.env.example` to `.env`; never commit secrets. Use `config/` for YAML/JSON settings.
- Run `make dev-setup` and `scripts/health-check.sh` to validate environment (script filenames end with `.sh`).
- Providers: set credentials as env vars (`ALPACA_KEY`, `ALPACA_SECRET`, `IEX_TOKEN`).
- Databases: SQLite by default; PostgreSQL helper at `tools/database/setup_postgres.sh`.
- Migrations: `alembic upgrade head`; create via `alembic revision --autogenerate -m "msg"`.
- Note: top-level `import marketpipe` requires core deps (e.g., `duckdb`). For CLI/tests, ensure deps installed (`pip install -e .[dev]`).

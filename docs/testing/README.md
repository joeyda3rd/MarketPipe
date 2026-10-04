# Running integration and end to end tests

Install development dependencies with `python -m pip install -e '.[dev]'`.
Use the same interpreter for pytest and dependencies.

| Check | Command | Scope |
| --- | --- | --- |
| Fast regression checks | `python -m pytest tests/unit` | Python 3.9–3.13 matrix in CI |
| Required E2E | `make test-e2e` | Installed wheel, fixed HTTP responses, recovery, concurrency, cleanup, data boundaries |
| Full offline integration | `make test-integration-full` | All integration tests except explicitly marked PostgreSQL cases |
| Release rehearsal | `make release-rehearsal` | Wheel/sdist build, strict metadata validation, installed journeys |
| PostgreSQL | `MARKETPIPE_TEST_POSTGRES_URL=postgresql://localhost/marketpipe_test python -m pytest tests/e2e/test_migrations.py tests/integration/test_postgres_migrations.py` | Fresh schemas, historical data upgrades, concurrent job claims |

The PostgreSQL URL must identify a dedicated test service with permission to create
schemas and disposable databases. Tests never use production `DATABASE_URL`.
E2E subprocesses receive explicit temporary paths and synthetic credentials. Provider
contracts use a local HTTP server; no live accounts are required. The wheel fixture
installs package code in a separate environment, verifies its import location, and
runs outside the checkout. Runtime dependencies are shared with the test environment;
the CI security job separately installs the runtime into a clean runner environment.

`CI Summary` requires the unit matrix, installed E2E job, and security scan. Type
checking remains advisory while existing type debt is addressed. Extended Tests runs
nightly and can be dispatched manually; it covers oldest/latest Python integration,
PostgreSQL, and persistence with bounded descriptors and numerical worker threads.
Test timeouts bound failures. CI artifacts include XML, CLI logs, reports and relevant
E2E data; none contain live provider credentials.

Timing benchmarks use the existing `--benchmark` opt-in. Run CLI startup timings
serially with `python -m pytest tests/integration/test_cli_enhanced_matrix.py -k performance_benchmarks --benchmark`
so concurrent workers do not distort the two-second startup budget. The benchmark
also requires each command to succeed.

Release defaults to `dry_run: true` when dispatched. It validates the requested
version, executes the offline suites, builds distributions, and stores them as CI
artifacts. Release creation, tagging and Test PyPI uploads run only for a tag event or
an explicitly selected non-dry run. Publication uses the exact validated artifacts.

Live Provider Probe is manual only. Choose Alpaca or Polygon and a past trading date;
it issues exactly one AAPL request with no pagination or retries. Configure repository
secrets for that provider first. It reports HTTP status or failure category without
printing headers, URLs with credentials, or response bodies. Do not use it as a merge
gate. Live probes have not been run as part of offline validation.

For controlled Alpaca contract environments, set `ALPACA_BASE_URL`; its default is
`https://data.alpaca.markets/v2`. Polygon uses `POLYGON_BASE_URL`. Request intervals are
half open, overlapping pages are deduplicated, and malformed successful responses fail.

The complete delivery plan and execution record are in
[E2E_HARDENING_PLAN.md](E2E_HARDENING_PLAN.md).

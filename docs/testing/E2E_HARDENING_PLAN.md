# MarketPipe end to end testing and hardening plan

Created: 2026-10-04. Status: implementation in progress.

## Objective and scope

A passing required build must demonstrate that an installed MarketPipe package can
ingest, validate, aggregate, query, inspect jobs, and clean up selected job metadata.
Recovery tests must demonstrate that interruptions and concurrent activity cannot
silently lose persisted bars or advance checkpoints past stored data. Provider tests
must exercise real adapters against controlled HTTP responses. Release rehearsal
must validate distributions and migrations without publishing anything.

The user approved execution of the complete plan. The agreed test boundaries are
the installed CLI, public storage and repository interfaces, coordinator execution,
provider HTTP APIs, aggregation and validation services, and release artifacts.
Internal services remain real; external HTTP responses, clocks, and operating-system
failures may be controlled at their boundaries. Literal expected data is independent
of the implementation under test. Existing developer files and credentials are not
test inputs, and all databases and output live under temporary directories.

## 1 Establish trustworthy required checks

- Audit the existing E2E tests for conditional assertions, obsolete skips, broad
  success assertions, wrong subprocess interpreters, and inherited credentials.
- Create an isolated subprocess harness with explicit paths for raw data, aggregates,
  job database, checkpoint database, and reports. Use fixed dates and small datasets.
- Build a wheel, install it into a separate environment, and execute from outside
  the source checkout without `PYTHONPATH` or an editable install.
- Execute ingestion, validation, aggregation, querying, job inspection, cleanup
  preview, and filtered cleanup. Assert exact data and metadata and rerun ingestion
  to verify idempotency. Exercise documented command forms and failure exit codes.
- Run the deterministic E2E suite as a separate required CI job with no tolerated
  failures. Retain the Python 3.9–3.13 unit matrix. Preserve XML results, logs, and
  relevant output artifacts on failure. Require the CI summary before merging.

Acceptance: a broken pipeline or missing output makes CI red; the required suite
contains no skipped core journey and targets less than five minutes on GitHub.

## 2 Verify recovery and concurrent persistence

- Interrupt a worker before replacement, after replacement, and before checkpoint
  persistence. Restart against the same real data and repository paths.
- Verify resumed output contains all unique timestamps, checkpoint ordering is safe,
  failed work is reported, and already persisted files remain readable.
- Exercise separate writer processes on a shared partition, readers during writes,
  storage permission and disk-full errors, database contention, and partial symbol
  failures. Use bounded waits and explicit process synchronization rather than sleeps.
- Verify filtered cleanup deletes only selected job/checkpoint metadata; preview and
  unfiltered execution preserve unrelated jobs, checkpoints, and raw data.

Acceptance: no missing or duplicate bars after recovery or concurrent appends;
failed operations retain the previous valid file and cannot report success.

## 3 Exercise real provider contracts and data boundaries

- Run Alpaca and Polygon adapters against a local HTTP server, including multiple
  pages, overlapping data, repeated cursors, empty pages, malformed responses,
  authorization failures, rate limits, server errors, and connection/read timeouts.
- Assert bounded retries, complete data, half-open date boundaries, deterministic
  failure propagation, and masking of synthetic credentials in all captured output.
- Cover New York DST transitions, market session and date boundaries, all supported
  aggregation frames, duplicate/unordered timestamps, zero volumes, invalid rows,
  and corrupt input files using independent expected OHLCV values.

Acceptance: an incomplete provider response never masquerades as complete ingestion;
golden aggregation and validation results match the fixtures exactly.

## 4 Expand integration execution and diagnostics

- Run the full offline integration suite with individual test timeouts, a job timeout,
  timing reports, and failures retained as artifacts. Repair reproducible failures;
  do not suppress failures to make the suite green.
- Add scheduled and manually dispatched full integration, PostgreSQL, recovery,
  concurrency, and resource-limit coverage. Test supported oldest/latest Python
  versions where useful. Keep external-provider probes out of merge gates.
- Provide a manually dispatched live provider probe using explicit credentials,
  a small fixed request budget, no credential logging, and clear failure reporting.
  Live service requests require an opt-in run and are not used for local validation.

Acceptance: the full offline suite has a recorded result; PostgreSQL has a real
service-backed result; failures include useful diagnostics and bounded execution.

## 5 Harden migrations, releases, and security gates

- Validate a fresh database and upgrade of an existing database with preserved rows;
  verify supported configuration upgrades and invalid configuration failures.
- Rehearse wheel/sdist builds, metadata checks, fresh installation, installed CLI
  journeys, and packaged migration resources. Gate publication on these checks.
- Make release dry runs side-effect free: no GitHub release, tag, or package upload.
  Correct release output wiring and upload actual distribution files.
- Triage current dependency and high-confidence/high-severity static findings.
  Gate actionable findings and represent any necessary exception explicitly with
  a reason and expiry; scanner errors must not look like a clean scan.

Acceptance: a release rehearsal creates only local/CI artifacts; existing data
survives migrations; required security checks cannot silently tolerate failures.

## Delivery and verification

Implement in small behavior-focused slices and commits, recording failing regression
signals before fixing discovered defects. Run formatting, lint, architecture contracts,
unit tests, the new E2E suite, and relevant integration suites. Push the implementation,
verify remote required and extended checks, merge after success, and synchronize main.
Do not claim optional live probes were executed without an explicit opt-in run.

## Execution record

- Planning: saved this complete plan before implementation.
- Baseline: main is synchronized at `a25b1ba`; local draft/tooling/CSV/workspace files
  are preserved outside the implementation commits.
- Added installed wheel/sdist journeys with literal OHLCV assertions, multi-symbol
  multi-day discovery, failure exit codes, idempotency and filtered cleanup.
- Recovery tests kill real workers before replacement and after replacement (before
  checkpoint persistence), then use the supported `jobs doctor --fix` recovery flow.
  Separate-process appends, concurrent readers, disk/permission failures, corrupt
  partitions, partial provider failure and real SQLite write contention are covered.
- HTTP regressions exposed and fixed repeated cursors, malformed successful
  envelopes, duplicate pages, exclusive end handling, and the default async HTTP
  adapter calling the synchronous client. Synthetic credentials are checked for leaks.
- Historical upgrade and fresh-schema tests passed on a temporary PostgreSQL 14
  service and SQLite. Fixed dialect-specific index queries and nanosecond integer
  overflow; concurrent PostgreSQL claims are verified against disposable databases.
- Golden tests cover all six frames, unordered/duplicate bars, zero volume and both
  New York DST changes. README command workflows now execute without obsolete skips.
- Full integration baseline found 14 failures. Fixed obsolete symbols/expectations
  and unsafe subprocess isolation; follow-up exposed shared event subscriptions and
  duplicate CLI startup costs, which are corrected through public cleanup and one
  isolated help invocation. Final combined results will be recorded after validation.
- Dependency audit after upgrading bootstrap pip/setuptools: zero vulnerabilities;
  high severity/high confidence Bandit scan: zero findings and zero scanner errors.
  No vulnerability exceptions were introduced.
- Wheel/sdist strict Twine checks passed; packaged Alembic resources are asserted.
  GitHub workflows pass actionlint. Release dry runs guard every public mutation and
  publication reuses validated artifacts. Optional live probes remain unexecuted.
- Required checks, nightly/manual integrations, PostgreSQL and bounded-resource
  jobs are implemented. Remote execution and final delivery are pending.

MarketPipe DDD Refactor – Guiding Document  
================================================

Author: Architecture Working Group  
Audience: Any developer tasked with executing the DDD clean-up.  
Version: 0.1 (2025-06-13)

--------------------------------------------------------------------
A. Mission Statement
--------------------------------------------------------------------
"Elevate MarketPipe from a 'mostly-DDD' codebase to a **clean, enforceable Domain-Driven Design architecture**, isolating the domain core from infrastructure, eliminating legacy shortcuts, and future-proofing the project for new bounded contexts."

Key goals  
1. Preserve *behavioural parity*: every existing CLI command, metric, and test must still pass.  
2. Enforce *clear boundaries*: domain layer must not depend on infrastructure libraries or concrete technology.  
3. Provide *smooth migration*: deprecations before deletions; no week-long feature branches.  
4. Embed *guard-rails*: static-analysis rules & regression tests to ensure boundaries stay clean.

--------------------------------------------------------------------
B. Overarching To-Do List
--------------------------------------------------------------------
1. **Extract metrics handlers from domain → infrastructure.monitoring**  
2. **Introduce IEventBus interface & move concrete bus to infrastructure**  
3. **Retire / refactor `cli_old.py`; converge on new Typer CLI**  
4. **Consolidate duplicate package roots (`marketpipe/` vs `src/marketpipe/`)**  
5. **Remove `.to_dict()` helpers & logging statements from domain objects**  
6. **Purge Prometheus/SQLite/httpx imports from domain layer**  
7. **Add guard-rail tests & import-linter contracts**  
8. **Update docs, ADRs & release notes**

*Tip – iterate vertically* (slice by slice) and merge to `main` frequently.

--------------------------------------------------------------------
C. Detailed Step-By-Step Instructions
--------------------------------------------------------------------

⚠️  Legend  
• **Action** – something to implement  
• **Check** – verification step  
• **Caution** – common pitfall

--------------------------------------------------
1. Move metrics handlers out of the domain
--------------------------------------------------
Action 1.1  Create `src/marketpipe/infrastructure/monitoring/` with:  
```
__init__.py
event_handlers.py   # contains Prometheus & SQLite logic
```
Action 1.2  Cut/paste all Prometheus-touching functions from  
`src/marketpipe/metrics_event_handlers.py` **and**  
`src/marketpipe/domain/event_handlers.py` into the new module.

Action 1.3  Add public helper:  

```python
def register() -> None:
    from marketpipe.events import EventBus
    …  # subscribe handlers here
```

Action 1.4  Call `monitoring.event_handlers.register()` inside `bootstrap.bootstrap()` **after** metrics and EventBus are initialised.

Check  
• `grep -R "REQUESTS.labels" src/marketpipe/domain` returns nothing.  
• Run `pytest tests/test_metrics.py` – counters still increment.

Caution  
Event handlers previously registered at import-time will now need explicit registration; failing to do so silently drops metrics.

--------------------------------------------------
2. Introduce EventBus abstraction
--------------------------------------------------
Action 2.1  In `src/marketpipe/domain/events.py` add:

```python
from typing import Protocol, Callable, Type
class IEventBus(Protocol):
    def subscribe(self, etype: Type[DomainEvent], fn: Callable[[DomainEvent], None]) -> None: ...
    def publish(self, event: DomainEvent) -> None: ...
```

Action 2.2  Move current `EventBus` class to  
`src/marketpipe/infrastructure/messaging/in_memory_bus.py` implementing `IEventBus`.

Action 2.3  Replace domain imports:  
`from marketpipe.events import EventBus` → **domain never imports concrete bus**.  Use `from typing import TYPE_CHECKING` if only for type hints.

Action 2.4  Add singleton accessor in `bootstrap.py`:

```python
_EVENT_BUS: IEventBus | None = None
def get_event_bus() -> IEventBus:
    global _EVENT_BUS
    if _EVENT_BUS is None:
        from marketpipe.infrastructure.messaging.in_memory_bus import InMemoryEventBus
        _EVENT_BUS = InMemoryEventBus()
    return _EVENT_BUS
```

Refactor `publish()` calls in application services to `get_event_bus().publish(evt)`.

Check  
• Domain package has **zero** imports of `infrastructure` or Prometheus.  
• Unit test: publishing an event triggers subscribed handler.

Caution  
Constructor signatures may explode with additional parameters.  Inject bus only at the application layer; use the singleton accessor elsewhere to avoid boiler-plate.

--------------------------------------------------
3. Retire / refactor `cli_old.py`
--------------------------------------------------
Action 3.1  Enumerate commands:  
`grep "@app.command" src/marketpipe/cli_old.py`

Action 3.2  For each command not present in the new CLI (`src/marketpipe/cli/`):  
• Implement thin wrapper under `cli/` that delegates to application services.  
• Ensure no repository/provider wiring inside CLI code.

Action 3.3  Replace body of `cli_old.py` with:

```python
import warnings, sys
warnings.warn("cli_old is deprecated; use `marketpipe` CLI", DeprecationWarning)
from marketpipe.cli import app  # re-export
if __name__ == "__main__":
    sys.exit(app())
```

Action 3.4  Update `pyproject.toml`:

```toml
[project.scripts]
marketpipe = "marketpipe.cli:app"
```

Check  
• CLI smoke test: `marketpipe ingest --help` works.  
• `pytest -Werror::DeprecationWarning` passes.

Caution  
Avoid dual business logic paths; move behaviour first, then add deprecation shim in the *same* commit.

--------------------------------------------------
4. Consolidate duplicate package roots
--------------------------------------------------
Action 4.1  Audit old root:  
`ls marketpipe/`  

Action 4.2  For each module only present in old root:  
• `git mv marketpipe/<path> src/marketpipe/<same-context>/`  

Action 4.3  Adjust imports if package path changed; run `ruff --fix` to auto-sort.

Action 4.4  Delete empty legacy directory.

Check  
• `pytest` green.  
• `python -c "import marketpipe, inspect, sys; print(marketpipe.__file__)"` path points to `src/…`.

Caution  
Use `git mv` not copy → preserves history, easier review.

--------------------------------------------------
5. Remove `.to_dict()` & logging from domain
--------------------------------------------------
Action 5.1  Search helpers:  
`grep -R "def to_dict" src/marketpipe/domain`

Action 5.2  Add mapper modules:  
`src/marketpipe/infrastructure/serialization/ohlcv.py` etc.

Action 5.3  Replace calls in repositories or adapters to use mapper functions.

Action 5.4  Remove `import logging` inside domain; if business event must be logged, publish a `SomethingHappened` DomainEvent instead.

Check  
• Static search shows no `.to_dict(` in domain.  
• No `logging.getLogger` in domain.

Caution  
Tests comparing dictionaries must now use the mapper; update fixtures accordingly.

--------------------------------------------------
6. Purge concrete-tech imports from domain
--------------------------------------------------
Action 6.1  Run:

```bash
grep -R "prometheus_client\|sqlite3\|httpx" src/marketpipe/domain
```

Action 6.2  If any, move that code to infrastructure; inject via repository interfaces.

Check  
• Search above returns nothing.

Caution  
A stray `from httpx import Response` in a type hint still drags infra into domain; use `if TYPE_CHECKING:` block.

--------------------------------------------------
7. Add guard-rail static checks
--------------------------------------------------
Action 7.1  Add `import-linter` config `.importlinter`:

```
[importlinter]
root_package = marketpipe

[contract: Enforce hexagonal]
name = domain_must_not_import_infrastructure
type = forbidden
source_modules = marketpipe.domain
forbidden_modules = marketpipe.infrastructure, prometheus_client, httpx, sqlite3
```

Action 7.2  Add pre-commit hook:

```yaml
-   repo: https://github.com/seddonym/import-linter
    rev: v1.7.0
    hooks:
      - id: import-linter
```

Action 7.3  Add unit test to verify metrics increment (guarding regression from §1).

Check  
• `import-linter --check` passes in CI.

Caution  
Treat violations as blocking; otherwise rules won't stick.

--------------------------------------------------
8. Update Documentation
--------------------------------------------------
Action 8.1  Create ADR-12 "Introduce IEventBus and monitoring context".  
Action 8.2  Update README paths & diagrams.  
Action 8.3  Add migration guide (`docs/migration/0.4-to-0.5.md`) for external consumers.

--------------------------------------------------------------------
D. Cautionary Advice (General)
--------------------------------------------------------------------
• **Small, reviewable PRs** – cap at ~600 LOC; use "stacked" PR technique.  
• **Regression tests first** – snapshot end-to-end ingestion, validation & metrics flows before refactor.  
• **Feature flags** – if in doubt, leave legacy path behind a flag for quick rollback.  
• **Avoid star-imports & wild re-exports** – they hide dependency direction.  
• **Run mypy & ruff in "CI fail" mode** – structural errors surface early.  
• **Communicate deprecations** – add `DeprecationWarning` + CHANGELOG line.

--------------------------------------------------------------------
E. Success Definition
--------------------------------------------------------------------
• All unit, integration & e2e tests pass.  
• `import-linter` contract passes.  
• No Prometheus/SQLite/httpx imports in `src/marketpipe/domain`.  
• Metrics counters & CLI commands behave identically in a "before vs after" run on a real ingestion job.  
• Code reviewers can locate infrastructure-specific code in `infrastructure/*` and domain logic in `domain/*` with *no overlap*.

Execute this plan iteratively and MarketPipe will achieve a clean, maintainable DDD architecture without losing momentum or stability. 
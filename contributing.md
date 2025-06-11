# Contributing to **MarketPipe**

Welcome — and thanks for taking the time to help improve *MarketPipe*, a DDD‑styled ETL framework for market‑data workflows. This guide explains how to get a local dev environment running, the coding standards we follow, and the steps for submitting patches.

---

## 1 · Project Philosophy

*  **Modular & Pluggable** – every bounded context (ingestion, aggregation, validation…) is replaceable and unit–testable.
*  **Production‑first** – SQLite migrations, WAL, connection pooling, metrics and logging in every layer.
*  **Test‑driven** – MVP requires ≥ 70 % overall coverage; new code should land with ≥ 85 % branch coverage.
*  **Open to New Providers** – any REST/WS feed can be added via the `marketpipe.providers` entry‑point.

---

## 2 · Development Environment

```bash
# 1. Clone fork
$ git clone https://github.com/<your‑handle>/MarketPipe.git
$ cd MarketPipe

# 2. Create and activate virtual‑env
$ python -m venv .venv
$ source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# 3. Install dependencies with dev extras
$ pip install -e .[dev]
```

### 2.1 Optional Tools

* **`Docker`** – compose file supplies DuckDB + Grafana stack.
* **`direnv`** – auto‑activates venv and env‑vars.

---

## 3 · Running Tests & Coverage

```bash
# quick unit pass
$ pytest -q

# full suite + coverage
$ pytest --cov=marketpipe --cov-report=term-missing
```

> **Target**: maintain project coverage ≥ 70 %. Add tests alongside new modules.

### 3.1 Integration Tests

Marked `@pytest.mark.integration`; run with:

```bash
pytest -m integration
```

They spin temporary Parquet/SQLite files in `tmp_path` — no network calls.

---

## 4 · Linting & Formatting

* **ruff** – style & static checks
* **black** – code formatter (line length = 100)

```bash
$ ruff .  # auto‑fixable issues: ruff --fix .
$ black .
```

Pre‑commit config is provided; install with:

```bash
pre-commit install
```

---

## 5 · Branching & Pull Requests

| Step                 | Command / Action                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------- |
| **1. Branch**        | `git checkout -b feat/<topic>`                                                                           |
| **2. Code & tests**  | Implement feature + unit/integration tests                                                               |
| **3. Format & lint** | `black . && ruff .`                                                                                      |
| **4. Run all tests** | `pytest -q`                                                                                              |
| **5. Commit**        | Use [Conventional Commits](https://www.conventionalcommits.org/) (e.g. `feat(storage): add GCS backend`) |
| **6. Push & PR**     | Open PR against `main`; fill PR template                                                                 |
| **7. CI**            | GitHub Actions will run lint + test + coverage gate                                                      |
| **8. Review**        | Address feedback, squash → merge                                                                         |

> **Merge policy** – at least 1 approving review and green CI.

---

## 6 · Adding a New Market‑Data Provider

1. Create adapter class implementing `IMarketDataProvider`.
2. Decorate with `@provider("myfeed")` **or** add entry‑point under:

   ```toml
   [project.entry-points."marketpipe.providers"]
   myfeed = mypkg.my_module:MyFeedAdapter
   ```
3. Provide `.from_cfg(cfg)` or `__init__(**kwargs)` so loader can instantiate.
4. Add unit tests with mocked HTTP responses.

---

## 7 · Database Migrations

* Migration SQL files live under `src/marketpipe/migrations/versions/` and follow `NNN_description.sql` naming.
* Run locally with:

  ```bash
  marketpipe migrate --path data/db/core.db
  ```
* **Never** alter existing migrations; create a new version instead.

---

## 8 · Metrics & Monitoring

* Counters & summaries defined in `metrics.py` (Prometheus).
* Historical metrics stored in SQLite for trend queries.
* Start the server:

  ```bash
  python -m marketpipe.metrics_server
  ```

---

## 9 · Issue Reporting & Discussions

Please file GitHub issues with **repro steps** and logs. Use **Discussions** tab for design ideas.

---

## 10 · AI‑Assisted Contributions

We welcome patches produced with the help of **AI coding assistants** (ChatGPT, Copilot, Cursor, etc.) as long as the following rules are observed:

1. **Review the generated code** — you are responsible for its correctness, originality, and license compatibility.
2. **Note AI usage in the PR description** — e.g. “Portions of this code were drafted with \[AI Coder] then manually reviewed.”
3. **No sensitive or proprietary prompts** — do not include secrets, keys, or private data in prompts or committed files.
4. **Follow project style & test coverage** — AI‑generated code must pass linters, unit tests, and meet coverage targets just like human‑written code.
5. **Respect upstream licenses** — ensure any AI‑suggested snippets are permissibly licensed.
6. **Use the provided rules files** — Make sure that your AI uses the provided rules files. Feel free to improve these with suggestions.

PRs that do not comply may be returned for revision.

---

## 11 · Community Standards · Community Standards

Be respectful, inclusive, and constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

Happy coding! 🎉

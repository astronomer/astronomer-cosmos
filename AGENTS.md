# AGENTS.md

This file provides project-wide guidance for AI coding agents (Claude Code, Cursor, Codex, Copilot, etc.) working in this repository. Tool-specific overrides — Claude-only behavior, IDE-only shortcuts, etc. — belong in a sibling file (e.g., [`CLAUDE.md`](CLAUDE.md)), which is expected to defer to this document for the common parts.

## What is Astronomer Cosmos

An Apache Airflow provider library that orchestrates dbt projects as Airflow DAGs and Task Groups. It parses dbt manifests/projects, converts the dbt node graph into Airflow task graphs, and executes dbt commands via various execution modes.

## Commands

### Running Tests

Unit tests (excluding integration/perf/dbtfusion):
```bash
hatch run tests.py3.11-2.10-1.9:test
hatch run tests.py3.11-2.10-1.9:test-cov   # with coverage
```

Run a single test file or test:
```bash
hatch run tests.py3.11-2.10-1.9:test tests/perf/test_dag_generation_perf.py
hatch run tests.py3.11-2.10-1.9:test -k "test_function_name"
```

Integration tests:
```bash
hatch run tests.py3.11-2.10-1.9:test-integration
```

Type checking:
```bash
hatch run tests.py3.10-3.1-1.9:type-check
```

**Always run the type-check before committing or opening a PR** — mypy is enforced in CI and skipping it locally wastes a CI round-trip. The `type-check` script is an alias for `pre-commit run mypy --files cosmos/**/*`, so if the hatch env's `pre-install-airflow.sh` fails to bootstrap on your machine, fall back to:
```bash
pre-commit run mypy --all-files
```

Other available matrix versions: Python `3.10`, `3.11`, `3.12`, `3.13` × Airflow `2.9`, `2.10`, `2.11`, `3.0`, `3.1`, `3.2` × dbt `1.5`–`2.0`.

### Building and Serving Docs

```bash
hatch run docs:build              # build HTML into docs/_build
hatch run docs:serve              # sphinx-autobuild with live reload
hatch run docs:serve-no-reload    # static build served on http://127.0.0.1:8000
```

Always run `hatch run docs:build` before committing any change that touches `docs/`, and verify the build succeeds without warnings or errors.

### Documentation Style

When writing or editing files under `docs/`, follow the Cosmos lightweight style rules in this section:

- **Heading underlines** use this canonical hierarchy per file: page title `=`, H1 `~`, H2 `+`, H3 `^`. The remap is positional per file (first level encountered becomes `=`, second `~`, etc.). Underline length must be at least the title's UTF-8 byte length.
- **Headings** use sentence case (e.g., "Choose an execution mode"), not Title Case.
- **Bullet points** use `-`, not `*` or `+`.
- **Decorative separator lines** (e.g., `-----` or `=====` not attached to a heading) should not be added.
- **DAG identifier** stays uppercase in code identifiers (`DAG` class, `DAG_FOLDER`, `AIRFLOW__DAG_*`) and in log-line samples that must match the code output (e.g., the `cosmos/converter.py` log message "for DAG using ...").
- A subset of these rules is enforced by `scripts/check_docs_style.py` in pre-commit.
- Per-profile pages under `docs/reference/profiles/` are auto-generated from `docs/reference/templates/profile_mapping.rst.jinja2` by `docs/generate_mappings.py` at Sphinx build time. The generated `.rst` files are gitignored. Edit the template, not the generated files.

### Linting and Formatting

**Always run the full pre-commit suite locally and ensure it passes before committing or pushing.** This includes the formatters, linters, type checks, and style checks (`scripts/check_docs_style.py` for `docs/`, configured via `.pre-commit-config.yaml`). The same hooks run in CI; catching failures locally avoids a wasted CI round-trip and a noisy PR history.

```bash
pre-commit run --all-files
```

Pre-commit's configured tools (Black, Ruff, mypy, codespell, check_docs_style, etc.) are listed in `.pre-commit-config.yaml`.

Individual tools, if you need to iterate on a single one:
```bash
black --line-length 120 cosmos/
ruff check cosmos/
mypy cosmos/
```

### Local Development Setup

```bash
python3 -m venv env && source env/bin/activate
pip install ".[test]"
pip install -e ".[dbt-postgres,dbt-bigquery]"
export AIRFLOW_HOME=$(pwd)/dev/
export AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow standalone
```

Or via Docker Compose:
```bash
docker compose -f dev/docker-compose.yaml up -d --build
```

## Commit Messages

Follow the [seven rules of a great Git commit message](https://cbea.ms/git-commit/):

1. Separate subject from body with a blank line
2. Aim for a subject line of 50 characters; do not exceed 72 characters
3. Capitalize the subject line
4. Do not end the subject line with a period
5. Use the imperative mood in the subject line ("Add feature" not "Added feature")
6. Wrap the body at 72 characters
7. Use the body to explain *what* and *why*, not *how*

Do not use Conventional Commits type prefixes (`feat:`, `fix:`, `chore:`, etc.).

### AI agent attribution

When a commit or PR description is drafted with the help of an AI coding agent (Claude Code, Cursor, Codex, Copilot, etc.), give the agent credit, but **do not add the agent as a `Co-Authored-By` trailer**. The Git `Co-Authored-By` trailer is reserved for human collaborators — it surfaces in GitHub's contributor graph and `git shortlog -sn` as if the agent were a person, which it isn't. Those signals are reserved for humans.

Instead, mark the assistance with a plain line at the end of the commit body or PR description (separated from the rest by a blank line):

```
🤖 Generated with Claude Code (https://claude.com/claude-code)
```

Substitute the equivalent for whichever agent was used. The convention is "visible credit, no person-shaped artefact" — the marker keeps the assist discoverable in `git log` without polluting human contribution stats.

## Python Coding Standards

### Logging

Get loggers via `cosmos.log.get_logger`, not the stdlib `logging` module. This adds the `(astronomer-cosmos)` prefix when `rich_logging` is enabled and respects scoped log-level configuration.

Yes:
```python
from cosmos.log import get_logger

logger = get_logger(__name__)
```

No:
```python
import logging

logger = logging.getLogger(__name__)
logging.error(...)  # never call the root logger directly either
```

Use **lazy logging**: pass the format string and arguments separately. Do not embed f-strings, `.format()`, or `%` interpolation in log messages — the logger formats them only when the record passes the level filter.

Yes:
```python
logger.info("Parsed %s nodes in %.3gs", node_count, elapsed)
```

No:
```python
logger.info(f"Parsed {node_count} nodes in {elapsed:.3g}s")
```

Applies to every `logger.{debug,info,warning,error,exception}` call. f-strings are fine everywhere else (exception messages, return values, etc.).

## Architecture

### Core Flow

1. User creates a `DbtDag` or `DbtTaskGroup` with `ProjectConfig`, `ProfileConfig`, `RenderConfig`, and `ExecutionConfig`.
2. `cosmos/converter.py` orchestrates the conversion from dbt project → Airflow DAG/TaskGroup.
3. `cosmos/dbt/graph.py` parses the dbt manifest or runs `dbt ls` to build a node graph.
4. `cosmos/dbt/selector.py` applies dbt-style selectors (tags, paths, model names) to filter nodes.
5. `cosmos/airflow/graph.py` renders the filtered node graph as Airflow tasks with correct dependencies.
6. Each task uses an operator from `cosmos/operators/` matching the chosen `ExecutionMode`.

### Key Modules

| Module | Purpose |
|---|---|
| `cosmos/config.py` | `ProjectConfig`, `ProfileConfig`, `RenderConfig`, `ExecutionConfig` — the four user-facing config classes |
| `cosmos/constants.py` | Enums: `ExecutionMode`, `LoadMode`, `TestBehavior`, `SourceRenderingBehavior`, etc. |
| `cosmos/converter.py` | Entry point: converts dbt project config into Airflow graph |
| `cosmos/dbt/graph.py` | Parses dbt manifest/runs `dbt ls`; builds `DbtGraph` with nodes and edges |
| `cosmos/dbt/selector.py` | Implements dbt selector grammar for filtering nodes |
| `cosmos/airflow/graph.py` | Converts `DbtGraph` nodes into Airflow `BaseOperator` instances and task dependencies |
| `cosmos/operators/` | One operator class per execution mode (`local.py`, `docker.py`, `kubernetes.py`, `virtualenv.py`, cloud variants) |
| `cosmos/profiles/` | Database-specific dbt profile generators (Postgres, Snowflake, BigQuery, Databricks, etc.) |
| `cosmos/cache.py` | Caches manifest parsing and `dbt ls` results to speed up DAG generation |
| `cosmos/listeners/` | Airflow event listeners for DAG run and task instance lifecycle events |
| `cosmos/plugin/` | Registers Cosmos as an Airflow provider plugin; includes cluster retry policy, dbt docs plugin etc |

### Execution Modes

Each mode in `ExecutionMode` enum corresponds to an operator subclass:
- `LOCAL` → `cosmos/operators/local.py` — runs dbt on the Airflow worker either in the same venv as Airflow or in a dedicated one
- `WATCHER` → `cosmos/operators/watcher.py` - more efficient than local, tries to run a single dbt command, while keeping models lineage and granularity in Airflow
- `AIRFLOW_ASYNC` → `cosmos/operators/airflow_async.py` - uses dbt to pre-compile the SQL transformations and leverage Airflow tasks to execute them (BigQuery only)
- `VIRTUALENV` → `cosmos/operators/virtualenv.py` — subprocess venv per task
- `DOCKER` → `cosmos/operators/docker.py`
- `KUBERNETES` → `cosmos/operators/kubernetes.py`
- `WATCHER_KUBERNETES` → `cosmos/operators/watcher_kubernetes.py` - a combination of `WATCHER` and `KUBERNETES`
- `AWS_EKS`, `AWS_ECS`, `AZURE_CONTAINER_INSTANCE`, `GCP_CLOUD_RUN_JOB` — cloud variants

Operators use lazy loading (`cosmos/operators/lazy_load.py`) so optional dependencies (Docker SDK, Kubernetes client) are only imported when the corresponding mode is used.

### Load Modes

How the dbt project is parsed, controlled by `LoadMode` (see the `LoadMode` enum for full details):
- `AUTOMATIC` — lets Cosmos choose an appropriate loading strategy based on available artifacts and configuration
- `DBT_LS` — runs `dbt ls` at DAG parse time
- `DBT_LS_FILE` — uses previously generated `dbt ls` output from a file
- `DBT_LS_CACHE` — uses cached `dbt ls` results across parses
- `DBT_MANIFEST` — reads a pre-compiled `manifest.json`
- `CUSTOM` — (deprecated!) Cosmos custom loader

### Test Behavior

Controlled by `TestBehavior` enum:
- `AFTER_EACH` — create a task group for each dbt model that has tests, dbt test task after each model task
- `BUILD` - each dbt node represents both the dbt node and the tests associated to it
- `AFTER_ALL` — single test task at the end
- `NONE` — no test tasks

### Code Quality Settings

- Line length: **120** characters
- Python minimum: **3.10**
- mypy: strict mode
- Ruff rules: C901 (complexity), D300, I (imports), F (pyflakes); max complexity 10
- pytest markers: `integration`, `perf`, `dbtfusion`

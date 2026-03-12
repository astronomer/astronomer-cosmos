# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
hatch run tests.py3.10-2.10-1.9:type-check
```

Other available matrix versions: Python `3.10`, `3.11`, `3.12`, `3.13` × Airflow `2.9`, `2.10`, `2.11`, `3.0`, `3.1` × dbt `1.5`–`2.0`.

### Linting and Formatting

Pre-commit runs Black (formatter), Ruff (linter), mypy, isort, and codespell:
```bash
pre-commit run --all-files
```

Individual tools:
```bash
black --line-length 120 cosmos/
ruff check cosmos/
mypy cosmos/
```

### Local Development Setup

```bash
python3 -m venv env && source env/bin/activate
pip install "apache-airflow[cncf.kubernetes,openlineage]"
pip install -e ".[dbt-postgres,dbt-databricks]"
export AIRFLOW_HOME=$(pwd)/dev/
export AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow standalone
```

Or via Docker Compose:
```bash
docker compose -f dev/docker-compose.yaml up -d --build
```

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
| `cosmos/plugin/` | Registers Cosmos as an Airflow provider plugin; includes cluster retry policy |

### Execution Modes

Each mode in `ExecutionMode` enum corresponds to an operator subclass:
- `LOCAL` → `cosmos/operators/local.py` — runs dbt on the Airflow worker
- `VIRTUALENV` → `cosmos/operators/virtualenv.py` — subprocess venv per task
- `DOCKER` → `cosmos/operators/docker.py`
- `KUBERNETES` → `cosmos/operators/kubernetes.py`
- `AWS_EKS`, `AWS_ECS`, `AZURE_CONTAINER_INSTANCE`, `GCP_CLOUD_RUN_JOB` — cloud variants

Operators use lazy loading (`cosmos/operators/lazy_load.py`) so optional dependencies (Docker SDK, Kubernetes client) are only imported when the corresponding mode is used.

### Load Modes

How the dbt project is parsed, controlled by `LoadMode`:
- `DBT_LS` — runs `dbt ls` at DAG parse time
- `DBT_MANIFEST` — reads a pre-compiled `manifest.json`
- `CUSTOM` — user provides a custom loader

### Test Behavior

Controlled by `TestBehavior` enum:
- `AFTER_EACH` — dbt test task after each model task
- `AFTER_ALL` — single test task at the end
- `NONE` — no test tasks

### Code Quality Settings

- Line length: **120** characters
- Python minimum: **3.10**
- mypy: strict mode
- Ruff rules: C901 (complexity), D300, I (imports), F (pyflakes); max complexity 10
- pytest markers: `integration`, `perf`, `dbtfusion`

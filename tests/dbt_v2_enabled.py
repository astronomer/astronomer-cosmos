"""What the dbt v2 (dbt-oss) integration job runs, while it is being rolled out one group at a time.

``.github/workflows/test-dbt-v2.yml`` runs the integration tests against BigQuery. Add a DAG file or a test file here
once it passes in that job. Everything else is left out and each run reports it as skipped or ignored.
"""

from __future__ import annotations

import os
from pathlib import Path

# Example DAG files (under dev/dags) that run in the dbt v2 job. A DAG that does not parse under dbt v2 would make the
# example DAG tests fail at collection time, so the files that are not listed are added to .airflowignore.
ENABLED_DAG_FILES = frozenset(
    {
        "basic_cosmos_dag.py",
        "basic_cosmos_dag_full_module_path_imports.py",
        "basic_cosmos_task_group.py",
        "cosmos_callback_dag.py",
        "cosmos_manifest_example.py",
        "cosmos_profile_mapping.py",
        "cosmos_seed_dag.py",
        "dbt_docs.py",
        "example_cosmos_dbt_build.py",
        "example_duckdb_dag.py",
        "example_seed_rendering.py",
        "example_taskflow_operator_args.py",
        "example_taskflow_project_config.py",
        "example_tasks_map.py",
        "example_watcher.py",
    }
)

# Test files (relative to the repository root) that run in the dbt v2 job. ``None`` runs every integration test file
# that does not need Postgres itself.
ENABLED_TEST_FILES: frozenset[str] | None = None


def is_dbt_v2_job() -> bool:
    return os.getenv("COSMOS_TEST_ADAPTER") == "bigquery"


def dag_files_to_ignore(dags_dir: Path) -> list[str]:
    """Names of the example DAG files to add to ``.airflowignore``; empty outside the dbt v2 job."""
    if not is_dbt_v2_job():
        return []
    return sorted(path.name for path in dags_dir.glob("*.py") if path.name not in ENABLED_DAG_FILES)

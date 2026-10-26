"""What the dbt-oss integration job runs, while it is being rolled out one group at a time.

``.github/workflows/test-dbt-oss.yml`` runs the integration tests against BigQuery. Add an example DAG file to
ENABLED_DAG_FILES once its DAGs pass in that job, and remove a test from SKIPPED_TESTS once it works with dbt-oss. The
example DAG files that are not enabled are ignored, and each skipped test is reported with its reason.
"""

from __future__ import annotations

import os
from pathlib import Path

# Example DAG files (under dev/dags) that run in the dbt-oss job. A DAG that does not parse under dbt-oss would make the
# example DAG tests fail at collection time, so the files that are not listed are added to .airflowignore.
# dbt_docs.py is left out until dbt-oss handles docs generate.
# simple_dag_async.py (async) is left out too. The DAGs that read an example project's own profiles.yml
# (user_defined_profile.py, example_cosmos_sources.py) run here because the workflow rewrites those Postgres profiles to
# BigQuery (scripts/test/bigquery_ci_profiles.py). cosmos_profile_mapping.py runs here because tests/dbt_oss_plugin.py
# answers its lookup of the Airflow connection `example_conn` with a BigQuery profile mapping.
# example_virtualenv_mini.py runs here because the plugin installs dbt-oss in its virtualenv, not dbt-postgres (the
# other virtualenv DAGs are excluded from the integration job). Only test_virtualenv.py runs it: integration.sh skips it
# in the example DAG tests.
ENABLED_DAG_FILES = frozenset(
    {
        "basic_cosmos_dag.py",
        "basic_cosmos_dag_full_module_path_imports.py",
        "basic_cosmos_task_group.py",
        "basic_cosmos_task_group_different_owners.py",
        "cosmos_callback_dag.py",
        "cosmos_manifest_example.py",
        "cosmos_manifest_selectors_example.py",
        "cosmos_profile_mapping.py",
        "cosmos_seed_dag.py",
        "example_cosmos_cleanup_dag.py",
        "example_cosmos_dbt_build.py",
        "example_cosmos_sources.py",
        "example_dbt_deps.py",
        "example_duckdb_dag.py",
        "example_model_version.py",
        "example_operators.py",
        "example_outlets.py",
        "example_seed_rendering.py",
        "example_source_pruning.py",
        "example_source_rendering.py",
        "example_task_mapping.py",
        "example_taskflow_operator_args.py",
        "example_taskflow_project_config.py",
        "example_tasks_map.py",
        "example_tests_multiple_parents.py",
        "example_virtualenv_mini.py",
        "example_watcher.py",
        "user_defined_profile.py",
        "watcher_with_freshness_check.py",
    }
)

# Why a test is skipped in the dbt-oss job.
_NO_BQ_ADAPTER_MODULE = "needs dbt-bigquery's dbt.adapters.bigquery, which cannot be installed next to dbt-oss (both provide the dbt package)"
_ASYNC_DAG_NOT_ENABLED = (
    "its DAG file simple_dag_async.py is not enabled in this job (BOSS-787); once enabled, with the setup task disabled "
    "the async mock imports dbt-bigquery's BigQueryConnectionManager in the worker process (see _NO_BQ_ADAPTER_MODULE)"
)
_NO_CALLBACKS = "dbt-oss dbtRunner does not support callbacks, and dbt does not plan to (https://github.com/dbt-labs/dbt/issues/13102)"
_NO_PARTIAL_PARSE = "dbt-oss has no partial parsing artifact (no target/partial_parse.msgpack, no log line), so Cosmos has nothing to cache"

# Tests that fail in the dbt-oss job for a known reason, keyed by pytest node id. Each one is skipped there with its
# reason until it is fixed or made to work with dbt-oss; remove its entry when it does.
SKIPPED_TESTS: dict[str, str] = {
    # dbt 2 dbtRunner has no callbacks (3)
    "tests/dbt/test_runner.py::test_dbt_runner_caching_and_callbacks": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_freshness_callback_excludes_model": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_process_dbt_log_event_surfaces_dbt_runner_error_from_real_event": _NO_CALLBACKS,
    # dbt-oss has no partial parsing artifact (4)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[False]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[True]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_uses_partial_parse_when_cache_is_disabled": _NO_PARTIAL_PARSE,
    "tests/operators/test_local.py::test_run_operator_caches_partial_parsing": _NO_PARTIAL_PARSE,
    # dbt-bigquery module cannot be installed next to dbt-oss (1)
    "tests/operators/_asynchronous/test_bigquery.py::test_mock_bigquery_adapter": _NO_BQ_ADAPTER_MODULE,
    # The async example DAG is not enabled in this job (1)
    "tests/test_example_dags.py::test_async_example_dag_without_setup_task": _ASYNC_DAG_NOT_ENABLED,
}


def is_bigquery_test_adapter() -> bool:
    return os.getenv("COSMOS_TEST_ADAPTER") == "bigquery"


def dag_files_to_ignore(dags_dir: Path) -> list[str]:
    """Names of the example DAG files to add to ``.airflowignore``; empty outside the dbt-oss job."""
    if not is_bigquery_test_adapter():
        return []
    return sorted(path.name for path in dags_dir.glob("*.py") if path.name not in ENABLED_DAG_FILES)

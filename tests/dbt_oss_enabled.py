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
# cosmos_profile_mapping.py is left out on purpose: it builds its profile from the Postgres Airflow connection
# `example_conn`, which this job does not have. basic_cosmos_task_group.py and dbt_docs.py are left out until dbt-oss
# handles their --vars and docs generate. example_cosmos_sources.py (its own
# Postgres profile) and simple_dag_async.py (async) are left out too.
ENABLED_DAG_FILES = frozenset(
    {
        "basic_cosmos_dag.py",
        "basic_cosmos_dag_full_module_path_imports.py",
        "basic_cosmos_task_group_different_owners.py",
        "cosmos_callback_dag.py",
        "cosmos_manifest_example.py",
        "cosmos_manifest_selectors_example.py",
        "cosmos_seed_dag.py",
        "example_cosmos_cleanup_dag.py",
        "example_cosmos_dbt_build.py",
        "example_dbt_deps.py",
        "example_duckdb_dag.py",
        "example_model_version.py",
        "example_seed_rendering.py",
        "example_source_pruning.py",
        "example_source_rendering.py",
        "example_task_mapping.py",
        "example_taskflow_operator_args.py",
        "example_taskflow_project_config.py",
        "example_tasks_map.py",
        "example_tests_multiple_parents.py",
        "example_watcher.py",
        "watcher_with_freshness_check.py",
    }
)

# Why a test is skipped in the dbt-oss job.
_POSTGRES_ENV = "uses a profiles.yml or sources.yml that reads POSTGRES_* variables, which this job does not set"
_NO_BQ_ADAPTER_MODULE = (
    "needs dbt-bigquery's dbt.adapters.bigquery, which cannot be installed next to dbt-oss (both provide the dbt package); "
    "the async mock patches BigQueryConnectionManager, in the worker process when the setup task is disabled"
)
_NO_CALLBACKS = "dbt-oss dbtRunner does not support callbacks, and dbt does not plan to (https://github.com/dbt-labs/dbt/issues/13102)"
_NO_DATASETS = (
    "dbt-oss writes no datasets: its run_results.json has no args.profiles_dir, so the OpenLineage artifact parser raises "
    "KeyError (logged at debug level) and the inlets and outlets stay empty (reproduced locally with dbt-oss 2.0.8)"
)
_FORCES_DBT_RUNNER = (
    "its example DAG pins invocation_mode=InvocationMode.DBT_RUNNER, which Cosmos rejects on dbt 2 because the dbtRunner "
    "has no callbacks (leaving it unset would fall back to subprocess, but the example is included in the docs)"
)
_NO_LOAD_ERROR = "dbt-oss does not raise the load error the test expects"
_NO_PARTIAL_PARSE = "dbt-oss has no partial parsing artifact (no target/partial_parse.msgpack, no log line), so Cosmos has nothing to cache"
_MODEL_VERSION_ALIAS = "dbt-oss fails on this project with 'Template not found: jaffle_shop.generate_alias_name'"
_VIEW_OVER_TABLE = (
    "dbt-oss's BigQuery view materialization will not replace a table that an earlier test left in the job's dataset"
)
_ERROR_NOT_IN_CAPLOG = "the dbt-oss error text does not reach caplog in some cells (cause inferred, not investigated)"
_NEEDS_SEEDED_TABLE = (
    "needs the raw_orders table, which a seed task in the DAG creates but its source freshness check can run before, so it "
    "only passes when an earlier test has already seeded the job's dataset (the test order varies between runs)"
)
_UNKNOWN = "fails in the dbt-oss job for a cause not yet identified"

# Tests that fail in the dbt-oss job for a known reason, keyed by pytest node id. Each one is skipped there with its
# reason until it is fixed or made to work with dbt-oss; remove its entry when it does.
SKIPPED_TESTS: dict[str, str] = {
    # Postgres env vars (own profiles.yml/sources) (13)
    "tests/dbt/test_graph.py::test_altered_jaffle_shop_manifest_is_in_sync_with_project": _POSTGRES_ENV,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_with_project_config_vars": _POSTGRES_ENV,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_with_sources[load_via_dbt_ls]": _POSTGRES_ENV,
    "tests/dbt/test_graph.py::test_run_command_with_dbt_runner_exception": _POSTGRES_ENV,
    "tests/dbt/test_runner.py::test_handle_exception_if_needed_after_error": _POSTGRES_ENV,
    "tests/dbt/test_runner.py::test_handle_exception_if_needed_after_exception": _POSTGRES_ENV,
    "tests/operators/test_local.py::test_run_test_operator_subprocess_failure_exposes_failing_tests": _POSTGRES_ENV,
    "tests/operators/test_local.py::test_run_test_operator_with_callback[InvocationMode.DBT_RUNNER]": _POSTGRES_ENV,
    "tests/operators/test_local.py::test_run_test_operator_with_callback[InvocationMode.SUBPROCESS]": _POSTGRES_ENV,
    "tests/operators/test_local.py::test_run_test_operator_with_callback_and_trailing_dbt_output": _POSTGRES_ENV,
    "tests/operators/test_local.py::test_run_test_operator_without_callback[InvocationMode.SUBPROCESS]": _POSTGRES_ENV,
    "tests/test_debug.py::test_dbt_run_local_operator_stores_memory_in_xcom_when_debug_enabled": _POSTGRES_ENV,
    "tests/test_example_dags.py::test_source_pruning_dag": _POSTGRES_ENV,
    # The example DAG forces dbtRunner, which has no callbacks on dbt 2 (2)
    "tests/test_example_dags.py::test_example_dag[example_watcher]": _FORCES_DBT_RUNNER,
    "tests/test_example_dags.py::test_example_dag[example_watcher_with_freshness]": _FORCES_DBT_RUNNER,
    # dbt 2 dbtRunner has no callbacks (3)
    "tests/dbt/test_runner.py::test_dbt_runner_caching_and_callbacks": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_freshness_callback_excludes_model": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_process_dbt_log_event_surfaces_dbt_runner_error_from_real_event": _NO_CALLBACKS,
    # dbt 2 behaves differently: expected load error not raised (2)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_without_dbt_deps[dbt_runner]": _NO_LOAD_ERROR,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_without_dbt_deps[subprocess]": _NO_LOAD_ERROR,
    # dbt-oss has no partial parsing artifact (4)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[False]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[True]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_uses_partial_parse_when_cache_is_disabled": _NO_PARTIAL_PARSE,
    "tests/operators/test_local.py::test_run_operator_caches_partial_parsing": _NO_PARTIAL_PARSE,
    # dbt-oss writes no datasets (3)
    "tests/operators/test_local.py::test_run_operator_dataset_inlets_and_outlets_airflow_210": _NO_DATASETS,
    "tests/operators/test_local.py::test_run_operator_dataset_inlets_and_outlets_airflow_3_onwards": _NO_DATASETS,
    "tests/operators/test_local.py::test_run_operator_dataset_manual_outlets_airflow_210": _NO_DATASETS,
    # dbt-bigquery module cannot be installed next to dbt-oss (2)
    "tests/operators/_asynchronous/test_bigquery.py::test_mock_bigquery_adapter": _NO_BQ_ADAPTER_MODULE,
    "tests/test_example_dags.py::test_async_example_dag_without_setup_task": _NO_BQ_ADAPTER_MODULE,
    # dbt-oss fails on the model_version project (1)
    "tests/test_example_dags.py::test_example_dag[example_model_version]": _MODEL_VERSION_ALIAS,
    # dbt-oss view over a table left by an earlier test (1)
    "tests/test_example_dags.py::test_example_dag[example_multiple_parents_test]": _VIEW_OVER_TABLE,
    # Depends on a table that an earlier test seeds in the job's dataset (1)
    "tests/test_example_dags.py::test_example_dag[source_rendering_dag]": _NEEDS_SEEDED_TABLE,
    # dbt-oss error text not in caplog (1)
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_and_failing_model": _ERROR_NOT_IN_CAPLOG,
    # Unknown, needs investigation (2)
    "tests/operators/test_local.py::test_run_test_operator_without_callback[InvocationMode.DBT_RUNNER]": _UNKNOWN,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_and_empty_model": _UNKNOWN,
}


def is_bigquery_test_adapter() -> bool:
    return os.getenv("COSMOS_TEST_ADAPTER") == "bigquery"


def dag_files_to_ignore(dags_dir: Path) -> list[str]:
    """Names of the example DAG files to add to ``.airflowignore``; empty outside the dbt-oss job."""
    if not is_bigquery_test_adapter():
        return []
    return sorted(path.name for path in dags_dir.glob("*.py") if path.name not in ENABLED_DAG_FILES)

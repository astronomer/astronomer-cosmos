"""What the dbt v2 (dbt-oss) integration job runs, while it is being rolled out one group at a time.

``.github/workflows/test-dbt-v2.yml`` runs the integration tests against BigQuery. Add an example DAG file to
ENABLED_DAG_FILES once its DAGs pass in that job, and remove a test from SKIPPED_TESTS once it works with dbt v2. The
example DAG files that are not enabled are ignored, and each skipped test is reported with its reason.
"""

from __future__ import annotations

import os
from pathlib import Path

# Example DAG files (under dev/dags) that run in the dbt v2 job. A DAG that does not parse under dbt v2 would make the
# example DAG tests fail at collection time, so the files that are not listed are added to .airflowignore.
# cosmos_profile_mapping.py is left out on purpose: it builds its profile from the Postgres Airflow connection
# `example_conn`, which this job does not have. basic_cosmos_task_group.py, dbt_docs.py and example_duckdb_dag.py
# are left out until dbt v2 handles their --vars, docs generate and duckdb seed. example_cosmos_sources.py (its own
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
        "example_cosmos_dbt_build.py",
        "example_dbt_deps.py",
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

# Why a test is skipped in the dbt v2 job.
_POSTGRES_ENV = "uses a profiles.yml or sources.yml that reads POSTGRES_* variables, which this job does not set"
_NO_BQ_ADAPTER_MODULE = "needs dbt-bigquery's dbt.adapters.bigquery, which cannot be installed next to dbt v2 (both provide the dbt package)"
_NO_CALLBACKS = "dbt v2 dbtRunner does not support callbacks, and dbt does not plan to (https://github.com/dbt-labs/dbt/issues/13102)"
_NO_LOAD_ERROR = "dbt v2 does not raise the load error the test expects"
_PG_URIS = "asserts on Postgres dataset URIs (cause inferred, not checked against the test)"
_NO_PARTIAL_PARSE = "dbt v2 has no partial parsing artifact (no target/partial_parse.msgpack, no log line), so Cosmos has nothing to cache"
_SCHEMA_ARG = (
    "reads profile_args['schema'], which the BigQuery swap drops (cause inferred, not checked against the test)"
)
_LS_SUBPROCESS = "dbt v2 runs dbt ls through a subprocess, which bypasses the dbtRunner this test mocks"
_MODEL_VERSION_ALIAS = "dbt v2 fails on this project with 'Template not found: jaffle_shop.generate_alias_name'"
_VIEW_OVER_TABLE = (
    "dbt v2's BigQuery view materialization will not replace a table that an earlier test left in the job's dataset"
)
_ERROR_NOT_IN_CAPLOG = "the dbt v2 error text does not reach caplog in some cells (cause inferred, not investigated)"
_UNKNOWN = "fails in the dbt v2 job for a cause not yet identified"

# Tests that fail in the dbt v2 job for a known reason, keyed by pytest node id. Each one is skipped there with its
# reason until it is fixed or made to work with dbt v2; remove its entry when it does.
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
    # dbt 2 dbtRunner has no callbacks (7)
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_and_group_nodes_by_folder": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_freshness_callback_excludes_model": _NO_CALLBACKS,
    "tests/operators/test_watcher.py::test_process_dbt_log_event_surfaces_dbt_runner_error_from_real_event": _NO_CALLBACKS,
    "tests/test_example_dags.py::test_example_dag[example_watcher]": _NO_CALLBACKS,
    "tests/test_example_dags.py::test_example_dag[example_watcher_synchronous]": _NO_CALLBACKS,
    "tests/test_example_dags.py::test_example_dag[example_watcher_with_freshness]": _NO_CALLBACKS,
    # dbt 2 behaves differently: expected load error not raised (2)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_without_dbt_deps[dbt_runner]": _NO_LOAD_ERROR,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_without_dbt_deps[subprocess]": _NO_LOAD_ERROR,
    # Dataset/asset assertions use Postgres URIs (3)
    "tests/operators/test_local.py::test_run_operator_dataset_inlets_and_outlets_airflow_210": _PG_URIS,
    "tests/operators/test_local.py::test_run_operator_dataset_inlets_and_outlets_airflow_3_onwards": _PG_URIS,
    "tests/operators/test_local.py::test_run_operator_dataset_manual_outlets_airflow_210": _PG_URIS,
    # dbt v2 has no partial parsing artifact (4)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[False]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_caching_partial_parsing_subprocess[True]": _NO_PARTIAL_PARSE,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_uses_partial_parse_when_cache_is_disabled": _NO_PARTIAL_PARSE,
    "tests/operators/test_local.py::test_run_operator_caches_partial_parsing": _NO_PARTIAL_PARSE,
    # Test reads profile_args['schema'], which the BigQuery swap drops (1)
    "tests/airflow/test_graph.py::test_build_airflow_graph_with_override_profile_config": _SCHEMA_ARG,
    # dbt 2 runs dbt ls through a subprocess (2)
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_does_not_create_target_logs_in_original_folder[dbt_runner-False]": _LS_SUBPROCESS,
    "tests/dbt/test_graph.py::test_load_via_dbt_ls_does_not_create_target_logs_in_original_folder[dbt_runner-True]": _LS_SUBPROCESS,
    # dbt-bigquery module cannot be installed next to dbt v2 (1)
    "tests/operators/_asynchronous/test_bigquery.py::test_mock_bigquery_adapter": _NO_BQ_ADAPTER_MODULE,
    # dbt v2 fails on the model_version project (1)
    "tests/test_example_dags.py::test_example_dag[example_model_version]": _MODEL_VERSION_ALIAS,
    # dbt v2 view over a table left by an earlier test (1)
    "tests/test_example_dags.py::test_example_dag[example_multiple_parents_test]": _VIEW_OVER_TABLE,
    # dbt v2 error text not in caplog (1)
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_and_failing_model": _ERROR_NOT_IN_CAPLOG,
    # Unknown, needs investigation (4)
    "tests/operators/test_local.py::test_run_test_operator_without_callback[InvocationMode.DBT_RUNNER]": _UNKNOWN,
    "tests/operators/test_watcher.py::test_dbt_dag_with_watcher_and_empty_model": _UNKNOWN,
    "tests/test_example_dags.py::test_async_example_dag_without_setup_task": _UNKNOWN,
    "tests/test_example_dags.py::test_watcher_source_rendering_dag": _UNKNOWN,
}


def is_dbt_v2_job() -> bool:
    return os.getenv("COSMOS_TEST_ADAPTER") == "bigquery"


def dag_files_to_ignore(dags_dir: Path) -> list[str]:
    """Names of the example DAG files to add to ``.airflowignore``; empty outside the dbt v2 job."""
    if not is_dbt_v2_job():
        return []
    return sorted(path.name for path in dags_dir.glob("*.py") if path.name not in ENABLED_DAG_FILES)

"""Integration tests for dbt v2 (dbt-oss), which run against BigQuery because dbt v2 does not support Postgres.

They use their own small dbt project and are skipped unless ``COSMOS_TEST_DBT_V2=1``, which only the dbt v2 CI workflow
sets (together with ``COSMOS_TEST_BIGQUERY_DATASET`` and the ``gcp_gs_conn`` connection), so the other test jobs ignore them.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from airflow.utils.state import DagRunState

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from tests.utils import new_test_dag

pytestmark = pytest.mark.skipif(
    os.getenv("COSMOS_TEST_DBT_V2") != "1", reason="Only runs in the dbt v2 (dbt-oss) CI workflow, against BigQuery"
)

PROJECT_PATH = Path(__file__).parent / "project"
EXPECTED_TASKS = {"raw_customers_seed", "stg_customers_run", "customers_run", "customers_test"}


def _build_dag(dag_id: str, **execution_config_kwargs) -> DbtDag:
    return DbtDag(
        dag_id=dag_id,
        start_date=datetime(2023, 1, 1),
        project_config=ProjectConfig(dbt_project_path=PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
                conn_id="gcp_gs_conn",
                profile_args={
                    "project": os.getenv("COSMOS_TEST_BIGQUERY_PROJECT", "astronomer-dag-authoring"),
                    "dataset": os.environ["COSMOS_TEST_BIGQUERY_DATASET"],
                },
            ),
        ),
        render_config=RenderConfig(load_method=LoadMode.DBT_LS, emit_datasets=False),
        execution_config=ExecutionConfig(**execution_config_kwargs),
        operator_args={"install_deps": False, "execution_timeout": timedelta(seconds=300)},
    )


def _task_ids(dag: DbtDag) -> set[str]:
    # task ids are "<node>.<run|test>" for nested tasks and "<node>_<seed|run|test>" otherwise
    return {task_id.replace(".", "_") for task_id in dag.task_dict}


def test_dbt_ls_renders_the_project():
    """dbt v2 can list the project against BigQuery and Cosmos renders its nodes."""
    dag = _build_dag("dbt_v2_ls")
    assert EXPECTED_TASKS <= _task_ids(dag)


def test_dbt_dag_local_with_dbt_runner():
    dag = _build_dag("dbt_v2_local_runner", invocation_mode=InvocationMode.DBT_RUNNER)
    assert new_test_dag(dag).state == DagRunState.SUCCESS


def test_dbt_dag_local_with_subprocess():
    dag = _build_dag("dbt_v2_local_subprocess", invocation_mode=InvocationMode.SUBPROCESS)
    assert new_test_dag(dag).state == DagRunState.SUCCESS


def test_dbt_dag_with_watcher():
    dag = _build_dag("dbt_v2_watcher", execution_mode=ExecutionMode.WATCHER)
    assert "dbt_producer_watcher" in dag.task_dict
    assert new_test_dag(dag).state == DagRunState.SUCCESS

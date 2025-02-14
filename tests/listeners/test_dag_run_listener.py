import logging
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DAG
from airflow.utils.state import State

from cosmos import DbtRunLocalOperator, ProfileConfig, ProjectConfig
from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.listeners.dag_run_listener import on_dag_run_failed, on_dag_run_success, total_cosmos_tasks
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


@pytest.mark.integration
def test_is_cosmos_dag_is_true():
    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    assert total_cosmos_tasks(dag) == 13


@pytest.mark.integration
def test_total_cosmos_tasks_in_task_group():
    with DAG("test-id-dbt-compile", start_date=datetime(2022, 1, 1)) as dag:
        _ = DbtTaskGroup(
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "jaffle_shop",
            ),
            profile_config=profile_config,
        )

    assert total_cosmos_tasks(dag) == 13


def test_total_cosmos_tasks_is_one():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=profile_config,
            project_dir=DBT_ROOT_PATH / "jaffle_shop",
            task_id="run",
            install_deps=True,
            append_env=True,
        )
        run_operator

    assert total_cosmos_tasks(dag) == 1


def test_not_cosmos_dag():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        pass

    assert total_cosmos_tasks(dag) == 0


@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_success(mock_emit_usage_metrics_if_enabled, caplog):
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    run_id = str(uuid.uuid1())
    dag_run = dag.create_dagrun(
        state=State.NONE,
        run_id=run_id,
    )

    on_dag_run_success(dag_run, msg="test success")
    assert "Running on_dag_run_success" in caplog.text
    assert "Completed on_dag_run_success" in caplog.text
    assert mock_emit_usage_metrics_if_enabled.call_count == 1


@pytest.mark.integration
@patch("cosmos.listeners.dag_run_listener.telemetry.emit_usage_metrics_if_enabled")
def test_on_dag_run_failed(mock_emit_usage_metrics_if_enabled, caplog):
    caplog.set_level(logging.DEBUG)

    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )
    run_id = str(uuid.uuid1())
    dag_run = dag.create_dagrun(
        state=State.FAILED,
        run_id=run_id,
    )

    on_dag_run_failed(dag_run, msg="test failed")
    assert "Running on_dag_run_failed" in caplog.text
    assert "Completed on_dag_run_failed" in caplog.text
    assert mock_emit_usage_metrics_if_enabled.call_count == 1

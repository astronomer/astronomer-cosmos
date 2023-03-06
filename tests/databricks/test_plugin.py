from __future__ import annotations

import uuid
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.dates import days_ago
from airflow.utils.db import create_session
from airflow.utils.state import State
from databricks_cli.sdk.service import JobsService

from cosmos.providers.databricks.plugin import (
    DatabricksJobRepairAllFailedLink,
    DatabricksJobRepairSingleFailedLink,
    DatabricksJobRunLink,
    _clear_task_instances,
    _get_dagrun,
    _get_databricks_task_id,
    _repair_task,
)
from cosmos.providers.databricks.workflow import DatabricksMetaData


@pytest.fixture
def mock_dag():
    dag = MagicMock(spec=DAG)
    dag.dag_id = "my_dag"
    dag.get_task.return_value = MagicMock(
        task_group=MagicMock(
            children={"task_1": "task1", "task_2": "task2", "task_5": "task5"},
            group_id="test_group",
        )
    )
    return dag


@pytest.fixture
def mock_session():
    return MagicMock()


@patch("cosmos.providers.databricks.plugin.get_airflow_app")
@patch("cosmos.providers.databricks.plugin._get_dagrun")
@patch("cosmos.providers.databricks.plugin.clear_task_instances")
def test_clear_task_instances(
    mock_clear_tasks, mock_dagrun, mock_get_airflow_app, session
):
    mock_get_airflow_app.return_value.dag_bag = MagicMock()

    dag_id = "test_dag"
    run_id = "test_run"
    task_ids = ["task_1", "task_2", "task_3"]

    # Create mock objects for testing
    dag = MagicMock()
    dag.get_task_instances.return_value = [
        MagicMock(task_id=task_id, dag_id=dag_id) for task_id in task_ids
    ]

    dag_run = MagicMock(spec=DagRun)
    dag_run.get_task_instances.return_value = dag.get_task_instances()
    mock_dagrun.return_value = dag_run
    mock_session = MagicMock()
    databricks_task_ids = [dag_id + "__" + task_id for task_id in task_ids]
    _clear_task_instances(
        dag_id, run_id, databricks_task_ids, log=MagicMock(), session=mock_session
    )

    # Assert that clear_task_instances was called with the correct list of TaskInstances
    expected_tis = dag_run.get_task_instances.return_value
    mock_clear_tasks.assert_called_once_with(expected_tis, mock_session)


@patch("cosmos.providers.databricks.plugin.get_airflow_app")
@patch("cosmos.providers.databricks.plugin.XCom.get_value")
@patch("cosmos.providers.databricks.plugin.DatabricksHook")
def test_databricks_job_run_link(mock_hook, mock_xcom, mock_get_airflow_app, mock_dag):
    mock_dag_bag = MagicMock()
    mock_dag_bag.get_dag.return_value = mock_dag
    mock_get_airflow_app.return_value.dag_bag = mock_dag_bag

    mock_xcom.return_value = DatabricksMetaData(
        databricks_conn_id="test_conn",
        databricks_job_id="test_job",
        databricks_run_id="test_run",
    )

    mock_hook.return_value.host = "test_host"

    link = DatabricksJobRunLink()
    operator = MagicMock(
        task_id="dummy_task",
        dag=mock_dag,
        task_group=MagicMock(group_id="test_group", default_args={}),
    )
    ti_key = TaskInstanceKey(dag_id="test_dag", task_id="dummy_task", run_id="test_run")
    result = link.get_link(operator=operator, ti_key=ti_key)

    mock_dag_bag.get_dag.assert_called_once_with("test_dag")
    mock_xcom.assert_called_once_with(
        ti_key=TaskInstanceKey(
            task_id="test_group.launch", dag_id="test_dag", run_id="test_run"
        ),
        key="return_value",
    )
    mock_hook.assert_called_once_with("test_conn")

    expected_result = "https://test_host/#job/test_job/run/test_run"
    assert result == expected_result


@pytest.fixture
def mock_dagrun():
    dagrun = MagicMock(spec=DagRun)
    dagrun.get_task_instances.return_value = [
        MagicMock(state="failed", task_id="task_1"),
        MagicMock(state="skipped", task_id="task_2"),
        MagicMock(state="up_for_retry", task_id="task_3"),
        MagicMock(state="upstream_failed", task_id="task_4"),
        MagicMock(state="success", task_id="task_5"),
    ]
    return dagrun


@mock.patch("cosmos.providers.databricks.plugin.XCom")
def test_repair_all_get_link(mock_xcom, mock_dagrun, mock_dag, mock_session):
    # Arrange
    task_instance_key = TaskInstanceKey(
        dag_id="my_dag",
        task_id="my_task",
        run_id="run_id",
    )
    metadata = DatabricksMetaData(
        databricks_conn_id="databricks_conn",
        databricks_run_id="run_id",
        databricks_job_id="job_id",
    )

    mock_xcom.get_value.return_value = metadata
    link = DatabricksJobRepairAllFailedLink()

    link.get_dagrun = MagicMock(return_value=mock_dagrun)
    link.get_dag = MagicMock(return_value=mock_dag)
    link.get_tasks_to_run = MagicMock(return_value="task_1,task_2")
    mock_operator = MagicMock(task_group=MagicMock(group_id="test_group"))
    # Act
    result = link.get_link(mock_operator, None, ti_key=task_instance_key)

    # Assert
    assert (
        result == "/repair_databricks_job?dag_id=my_dag&"
        "databricks_conn_id=databricks_conn&"
        "databricks_run_id=run_id&"
        "run_id=run_id&"
        "tasks_to_repair=task_1,task_2"
    )


@mock.patch("cosmos.providers.databricks.plugin.get_airflow_app")
@mock.patch("cosmos.providers.databricks.plugin._get_dagrun")
def test_get_tasks_to_run(mock_dagrun, mock_airflow_app):
    link = DatabricksJobRepairAllFailedLink()
    ti_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run")
    dag = DAG("test_dag")
    task_group_children = {
        "test_group.test_task": MagicMock(
            task_id="test_group.test_task", dag_id="test_dag"
        ),
        "test_group.test_task_2": MagicMock(
            task_id="test_group.test_task_2", dag_id="test_dag"
        ),
        "test_group.test_task_3": MagicMock(
            task_id="test_group.test_task_3", dag_id="test_dag"
        ),
    }
    task = MagicMock(
        task_id="test_task",
        task_group=MagicMock(
            dag_id="test_dag", group_id="test_group", children=task_group_children
        ),
    )
    mock_airflow_app.return_value.dag_bag.get_dag.return_value = dag
    dag.add_task(task)

    def generate_mock_dagrun(task_map: dict[str, str]):
        dagrun = MagicMock(spec=DagRun)
        dagrun.get_task_instances.return_value = [
            MagicMock(state=state, task_id=task_id)
            for task_id, state in task_map.items()
        ]
        return dagrun

    # Case 1: No failed or skipped tasks
    mock_dagrun.return_value = generate_mock_dagrun({})
    tasks_str = link.get_tasks_to_run(ti_key, operator=task, log=MagicMock())
    assert tasks_str == ""

    # # Case 2: One failed task
    mock_dagrun.return_value = generate_mock_dagrun({"test_group.test_task": "failed"})
    tasks_str = link.get_tasks_to_run(ti_key, task, log=MagicMock())
    assert tasks_str == "test_dag__test_group__test_task"
    #
    # # Case 3: One skipped task
    mock_dagrun.return_value = generate_mock_dagrun({"test_group.test_task": "skipped"})
    tasks_str = link.get_tasks_to_run(ti_key, task, log=MagicMock())
    assert tasks_str == "test_dag__test_group__test_task"
    #
    # # Case 4: Multiple failed and skipped tasks
    mock_dagrun.return_value = generate_mock_dagrun(
        {"test_group.test_task": "failed", "test_group.test_task_2": "skipped"}
    )
    tasks_str = link.get_tasks_to_run(ti_key, task, log=MagicMock())
    assert (
        tasks_str == "test_dag__test_group__test_task,test_dag__test_group__test_task_2"
    )


@pytest.fixture
def session():
    with create_session() as session:
        yield session


def test_get_dagrun(session, dag):
    # Create a DagRun object and add it to the database
    DagBag()
    run_id = "test_run_id" + uuid.uuid4().hex
    dr = dag.create_dagrun(run_id=run_id, state=State.RUNNING)
    session.add(dr)
    session.commit()

    # Call the function and ensure it returns the correct DagRun object
    result = _get_dagrun(dag, run_id, session=session)
    assert result == dr


@mock.patch("cosmos.providers.databricks.plugin.DatabricksHook")
@mock.patch("cosmos.providers.databricks.plugin.JobsService")
@mock.patch("cosmos.providers.databricks.plugin.ApiClient")
def test_repair_task(mock_api_client, mock_jobs_service, mock_hook):
    databricks_conn_id = "my_databricks_conn"
    databricks_run_id = "my_databricks_run"
    tasks_to_repair = ["task1", "task2"]

    # Mock the Databricks hook and API client
    mock_hook.return_value = MagicMock(spec=DatabricksHook)
    mock_api_client = MagicMock()
    mock_hook.get_conn.return_value = mock_api_client

    # Mock the JobsService and its methods
    mock_jobs_service.return_value = MagicMock(spec=JobsService)
    mock_jobs_service.return_value.get_run.return_value = {
        "job_id": 1234,
        "run_id": databricks_run_id,
        "state": "RUNNING",
        "start_time": "2022-02-27T00:00:00Z",
        "end_time": None,
        "tasks": [],
        "state_message": None,
        "creator_user_name": "airflow",
        "run_name": None,
        "run_page_url": None,
        "run_type": None,
        "spark_context_id": None,
        "retry_number": 0,
        "previous_run_id": None,
        "trigger": {},
        "is_completed": False,
        "is_active": True,
        "is_queued": False,
        "cluster_spec": {},
        "overriding_parameters": {},
        "start_time_epoch": 1645958400,
    }
    mock_jobs_service.return_value.repair.return_value = None

    # Patch the DatabricksHook and JobsService constructors
    _repair_task(
        databricks_conn_id, databricks_run_id, tasks_to_repair, log=MagicMock()
    )

    # # Check that the JobsService methods were called correctly
    mock_hook.return_value.get_conn.assert_called_once_with()
    mock_jobs_service.return_value.get_run.assert_called_once_with(
        run_id=databricks_run_id, include_history=True
    )
    mock_jobs_service.return_value.repair.assert_called_once_with(
        run_id=databricks_run_id,
        version="2.1",
        latest_repair_id=None,
        rerun_tasks=tasks_to_repair,
    )


@patch("cosmos.providers.databricks.plugin.get_airflow_app")
def test_databricks_job_repair_single_failed_link(mock_get_airflow_app, dag):
    mock_dag_bag = MagicMock()
    mock_task = MagicMock(
        task_id="test_task",
        task_group=MagicMock(group_id="test_group", default_args={}),
    )
    test_dag = DAG("test_dag", start_date=days_ago(1))
    mock_dag_bag.get_dag.return_value = test_dag
    test_dag.get_task = MagicMock(return_value=mock_task)
    mock_get_airflow_app.return_value.dag_bag = mock_dag_bag
    link = DatabricksJobRepairSingleFailedLink()

    dag_id = "test_dag"
    task_id = "test_task"
    run_id = "test_run"
    databricks_conn_id = "test_conn"
    databricks_run_id = "test_run_id"

    ti_key = TaskInstanceKey(dag_id, task_id, run_id)
    metadata = DatabricksMetaData(
        databricks_conn_id=databricks_conn_id,
        databricks_run_id=databricks_run_id,
        databricks_job_id=1234,
    )

    mock_xcom = MagicMock()
    mock_xcom.get_one.return_value = metadata
    with patch("cosmos.providers.databricks.plugin.XCom", mock_xcom):
        link.get_link(mock_task, dttm=None, ti_key=ti_key)
        f"/repair_databricks_job?dag_id={dag_id}&databricks_conn_id={databricks_conn_id}&databricks_run_id={databricks_run_id}&tasks_to_repair={_get_databricks_task_id(mock_task)}"

import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstanceKey
from airflow.models.xcom import XCom
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from cosmos.providers.databricks.plugin import (
    DatabricksJobRepairAllFailedLink,
    DatabricksJobRunLink,
)
from cosmos.providers.databricks.workflow import DatabricksMetaData


@patch("cosmos.providers.databricks.plugin.get_airflow_app")
@patch("cosmos.providers.databricks.plugin.XCom.get_one")
@patch("cosmos.providers.databricks.plugin.DatabricksHook")
def test_databricks_job_run_link(mock_hook, mock_xcom, mock_get_airflow_app):
    mock_dag_bag = MagicMock()
    mock_task = EmptyOperator(task_id="dummy_task")
    test_dag = DAG("test_dag", start_date=days_ago(1))
    mock_dag_bag.get_dag.return_value = test_dag
    mock_dag_bag.return_value.get_dag.return_value.get_task.return_value = mock_task
    mock_get_airflow_app.return_value.dag_bag = mock_dag_bag

    mock_xcom.return_value = DatabricksMetaData(
        databricks_conn_id="test_conn",
        databricks_job_id="test_job",
        databricks_run_id="test_run",
    )

    mock_hook.return_value.host = "test_host"

    link = DatabricksJobRunLink()
    operator = EmptyOperator(task_id="dummy_task", dag=test_dag)
    ti_key = TaskInstanceKey(dag_id="test_dag", task_id="dummy_task", run_id="test_run")
    result = link.get_link(operator=operator, ti_key=ti_key)

    mock_dag_bag.get_dag.assert_called_once_with("test_dag")
    mock_xcom.assert_called_once_with(
        task_id="dummy_task",
        dag_id="test_dag",
        run_id="test_run",
        key="return_value",
    )
    mock_hook.assert_called_once_with("test_conn")

    expected_result = "https://test_host/#job/test_job/run/test_run"
    assert result == expected_result


def test_get_link(self):
    # Mock XCom.get_one
    with unittest.mock.patch.object(XCom, "get_one", self.mock_xcom_get_one):
        # Test with no tasks to repair
        expected_link = (
            "/repair_databricks_job?dag_id=test_dag&"
            "databricks_conn_id=test_conn&"
            "databricks_run_id=1234&tasks_to_repair="
        )
        self.assertEqual(
            self.link.get_link(None, ti_key=self.mock_ti_key), expected_link
        )

        # Test with tasks to repair
        mock_tasks_to_run = "test_task_1,test_task_2"
        with unittest.mock.patch.object(
            DatabricksJobRepairAllFailedLink,
            "get_tasks_to_run",
            return_value=mock_tasks_to_run,
        ):
            expected_link = (
                "/repair_databricks_job?dag_id=test_dag&"
                "databricks_conn_id=test_conn&"
                "databricks_run_id=1234&"
                "tasks_to_repair=test_task_1,test_task_2"
            )
            self.assertEqual(
                self.link.get_link(None, ti_key=self.mock_ti_key), expected_link
            )


@pytest.fixture
def mock_dagrun():
    dagrun = mock.MagicMock(spec=DagRun)
    dagrun.get_task_instances.return_value = [
        mock.MagicMock(state="failed", task_id="task_1"),
        mock.MagicMock(state="skipped", task_id="task_2"),
        mock.MagicMock(state="up_for_retry", task_id="task_3"),
        mock.MagicMock(state="upstream_failed", task_id="task_4"),
        mock.MagicMock(state="success", task_id="task_5"),
    ]
    return dagrun


@pytest.fixture
def mock_dag():
    dag = mock.MagicMock(spec=DAG)
    dag.dag_id = "my_dag"
    dag.get_task.return_value = mock.MagicMock(
        task_group=mock.MagicMock(
            children={"task_1": "task1", "task_2": "task2", "task_5": "task5"}
        )
    )
    return dag


@pytest.fixture
def mock_session():
    return mock.MagicMock()


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

    mock_xcom.get_one.return_value = metadata
    link = DatabricksJobRepairAllFailedLink()

    link.get_dagrun = mock.MagicMock(return_value=mock_dagrun)
    link.get_dag = mock.MagicMock(return_value=mock_dag)
    link.get_tasks_to_run = mock.MagicMock(return_value="task_1,task_2")

    # Act
    result = link.get_link(None, None, ti_key=task_instance_key)

    # Assert
    assert (
        result == "/repair_databricks_job?dag_id=my_dag&"
        "databricks_conn_id=databricks_conn&"
        "databricks_run_id=run_id&"
        "tasks_to_repair=task_1,task_2"
    )


@mock.patch("cosmos.providers.databricks.plugin.get_airflow_app")
def test_get_tasks_to_run(mock_airflow_app):
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
    link.get_dagrun = MagicMock(return_value=generate_mock_dagrun({}))
    tasks_str = link.get_tasks_to_run(ti_key, operator=task)
    assert tasks_str == ""

    # # Case 2: One failed task
    link.get_dagrun = MagicMock(
        return_value=generate_mock_dagrun({"test_group.test_task": "failed"})
    )
    tasks_str = link.get_tasks_to_run(ti_key, task)
    assert tasks_str == "test_dag__test_group__test_task"
    #
    # # Case 3: One skipped task
    link.get_dagrun = MagicMock(
        return_value=generate_mock_dagrun({"test_group.test_task": "skipped"})
    )
    tasks_str = link.get_tasks_to_run(ti_key, task)
    assert tasks_str == "test_dag__test_group__test_task"
    #
    # # Case 4: Multiple failed and skipped tasks
    link.get_dagrun = MagicMock(
        return_value=generate_mock_dagrun(
            {"test_group.test_task": "failed", "test_group.test_task_2": "skipped"}
        )
    )
    tasks_str = link.get_tasks_to_run(ti_key, task)
    assert (
        tasks_str == "test_dag__test_group__test_task,test_dag__test_group__test_task_2"
    )

from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from cosmos.providers.databricks.notebook import DatabricksNotebookOperator
from cosmos.providers.databricks.workflow import DatabricksWorkflowTaskGroup


@pytest.fixture
def mock_runs_api():
    return MagicMock()


@pytest.fixture
def databricks_notebook_operator():
    return DatabricksNotebookOperator(
        task_id="notebook",
        databricks_conn_id="foo",
        notebook_path="/foo/bar",
        source="WORKSPACE",
        job_cluster_key="foo",
        notebook_params={
            "foo": "bar",
        },
        notebook_packages=[{"nb_index": {"package": "nb_package"}}],
    )


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.launch_notebook_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
def test_databricks_notebook_operator_without_taskgroup(mock_monitor, mock_launch, dag):
    with dag:
        notebook = DatabricksNotebookOperator(
            task_id="notebook",
            databricks_conn_id="foo",
            notebook_path="/foo/bar",
            source="WORKSPACE",
            job_cluster_key="foo",
            notebook_params={
                "foo": "bar",
            },
            notebook_packages=[{"nb_index": {"package": "nb_package"}}],
        )
        assert notebook.task_id == "notebook"
        assert notebook.databricks_conn_id == "foo"
        assert notebook.notebook_path == "/foo/bar"
        assert notebook.source == "WORKSPACE"
        assert notebook.job_cluster_key == "foo"
        assert notebook.notebook_params == {"foo": "bar"}
        assert notebook.notebook_packages == [{"nb_index": {"package": "nb_package"}}]
    dag.test()
    mock_launch.assert_called_once()
    mock_monitor.assert_called_once()


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.launch_notebook_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
@mock.patch(
    "cosmos.providers.databricks.workflow._CreateDatabricksWorkflowOperator.execute"
)
def test_databricks_notebook_operator_with_taskgroup(
    mock_create, mock_monitor, mock_launch, dag
):
    mock_create.return_value = {"job_id": 1}
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
        )
        with task_group:
            notebook = DatabricksNotebookOperator(
                task_id="notebook",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
                notebook_params={
                    "foo": "bar",
                },
                notebook_packages=[{"nb_index": {"package": "nb_package"}}],
            )
            assert notebook.task_id == "test_workflow.notebook"
            assert notebook.databricks_conn_id == "foo"
            assert notebook.notebook_path == "/foo/bar"
            assert notebook.source == "WORKSPACE"
            assert notebook.job_cluster_key == "foo"
            assert notebook.notebook_params == {"foo": "bar"}
            assert notebook.notebook_packages == [{"nb_index": {"package": "nb_package"}}]
    dag.test()
    mock_launch.assert_not_called()
    mock_monitor.assert_called_once()


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
def test_databricks_notebook_operator_without_taskgroup_new_cluster(
    mock_runs_api, mock_api_client, mock_monitor, dag
):
    mock_runs_api.return_value = mock.MagicMock()
    with dag:
        DatabricksNotebookOperator(
            task_id="notebook",
            databricks_conn_id="foo",
            notebook_path="/foo/bar",
            source="WORKSPACE",
            job_cluster_key="foo",
            notebook_params={
                "foo": "bar",
            },
            notebook_packages=[{"nb_index": {"package": "nb_package"}}],
            new_cluster={"foo": "bar"},
        )
    dag.test()
    mock_runs_api.return_value.submit_run.assert_called_once_with(
        {
            "notebook_task": {
                "notebook_path": "/foo/bar",
                "base_parameters": {"source": "WORKSPACE"},
            },
            "new_cluster": {"foo": "bar"},
            "libraries": [{"nb_index": {"package": "nb_package"}}]
        }
    )
    mock_monitor.assert_called_once()


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
def test_databricks_notebook_operator_without_taskgroup_existing_cluster(
    mock_runs_api, mock_api_client, mock_monitor, dag
):
    mock_runs_api.return_value = mock.MagicMock()
    with dag:
        DatabricksNotebookOperator(
            task_id="notebook",
            databricks_conn_id="foo",
            notebook_path="/foo/bar",
            source="WORKSPACE",
            job_cluster_key="foo",
            notebook_params={
                "foo": "bar",
            },
            notebook_packages=[{"nb_index": {"package": "nb_package"}}],
            existing_cluster_id="123",
        )
    dag.test()
    mock_runs_api.return_value.submit_run.assert_called_once_with(
        {
            "notebook_task": {
                "notebook_path": "/foo/bar",
                "base_parameters": {"source": "WORKSPACE"},
            },
            "existing_cluster_id": "123",
            "libraries": [{"nb_index": {"package": "nb_package"}}],
        }
    )
    mock_monitor.assert_called_once()


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
def test_databricks_notebook_operator_without_taskgroup_existing_cluster_and_new_cluster(
    mock_runs_api, mock_api_client, mock_monitor, dag
):
    with pytest.raises(ValueError):
        with dag:
            DatabricksNotebookOperator(
                task_id="notebook",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
                notebook_params={
                    "foo": "bar",
                },
                notebook_packages=[{"nb_index": {"package": "nb_package"}}],
                existing_cluster_id="123",
                new_cluster={"foo": "bar"},
            )
        dag.test()


@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator.monitor_databricks_job"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
def test_databricks_notebook_operator_without_taskgroup_no_cluster(
    mock_runs_api, mock_api_client, mock_monitor, dag
):
    with pytest.raises(ValueError):
        with dag:
            DatabricksNotebookOperator(
                task_id="notebook",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
                notebook_params={
                    "foo": "bar",
                },
                notebook_packages=[{"nb_index": {"package": "nb_package"}}],
            )
        dag.test()


def test_handle_final_state_success(databricks_notebook_operator):
    final_state = {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "state_message": "Job succeeded",
    }
    databricks_notebook_operator._handle_final_state(final_state)


def test_handle_final_state_failure(databricks_notebook_operator):
    final_state = {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "Job failed",
    }
    with pytest.raises(AirflowException):
        databricks_notebook_operator._handle_final_state(final_state)


def test_handle_final_state_exception(databricks_notebook_operator):
    final_state = {
        "life_cycle_state": "SKIPPED",
        "state_message": "Job skipped",
    }
    with pytest.raises(AirflowException):
        databricks_notebook_operator._handle_final_state(final_state)


@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
@mock.patch("time.sleep")
def test_wait_for_pending_task(mock_sleep, mock_runs_api, databricks_notebook_operator):
    # create a mock current task with "PENDING" state
    current_task = {"run_id": "123", "state": {"life_cycle_state": "PENDING"}}
    mock_runs_api.get_run.side_effect = [
        {"state": {"life_cycle_state": "PENDING"}},
        {"state": {"life_cycle_state": "RUNNING"}},
    ]
    databricks_notebook_operator._wait_for_pending_task(current_task, mock_runs_api)
    mock_runs_api.get_run.assert_called_with("123")
    assert mock_runs_api.get_run.call_count == 2
    mock_runs_api.reset_mock()


@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
@mock.patch("time.sleep")
def test_wait_for_terminating_task(
    mock_sleep, mock_runs_api, databricks_notebook_operator
):
    current_task = {"run_id": "123", "state": {"life_cycle_state": "PENDING"}}
    mock_runs_api.get_run.side_effect = [
        {"state": {"life_cycle_state": "TERMINATING"}},
        {"state": {"life_cycle_state": "TERMINATING"}},
        {"state": {"life_cycle_state": "TERMINATED"}},
    ]
    databricks_notebook_operator._wait_for_terminating_task(current_task, mock_runs_api)
    mock_runs_api.get_run.assert_called_with("123")
    assert mock_runs_api.get_run.call_count == 3
    mock_runs_api.reset_mock()


@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
@mock.patch("time.sleep")
def test_wait_for_running_task(mock_sleep, mock_runs_api, databricks_notebook_operator):
    current_task = {"run_id": "123", "state": {"life_cycle_state": "PENDING"}}
    mock_runs_api.get_run.side_effect = [
        {"state": {"life_cycle_state": "RUNNING"}},
        {"state": {"life_cycle_state": "RUNNING"}},
        {"state": {"life_cycle_state": "TERMINATED"}},
    ]
    databricks_notebook_operator._wait_for_running_task(current_task, mock_runs_api)
    mock_runs_api.get_run.assert_called_with("123")
    assert mock_runs_api.get_run.call_count == 3
    mock_runs_api.reset_mock()


def test_get_lifestyle_state(databricks_notebook_operator):
    runs_api_mock = MagicMock()
    runs_api_mock.get_run.return_value = {"state": {"life_cycle_state": "TERMINATING"}}

    task_info = {"run_id": "test_run_id"}

    assert (
        databricks_notebook_operator._get_lifestyle_state(task_info, runs_api_mock)
        == "TERMINATING"
    )


@mock.patch("cosmos.providers.databricks.notebook.DatabricksHook")
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_current_databricks_task"
)
def test_monitor_databricks_job_success(
    mock_get_task_name,
    mock_get_api_client,
    mock_runs_api,
    mock_databricks_hook,
    databricks_notebook_operator,
):
    # Define the expected response
    response = {
        "run_page_url": "https://databricks-instance-xyz.cloud.databricks.com/#job/1234/run/1",
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "Ran successfully",
        },
        "tasks": [
            {
                "run_id": "1",
                "task_key": "1",
            }
        ],
    }
    mock_runs_api.return_value.get_run.return_value = response

    databricks_notebook_operator.databricks_run_id = "1234"
    databricks_notebook_operator.monitor_databricks_job()


@mock.patch("cosmos.providers.databricks.notebook.DatabricksHook")
@mock.patch("cosmos.providers.databricks.notebook.RunsApi")
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_api_client"
)
@mock.patch(
    "cosmos.providers.databricks.notebook.DatabricksNotebookOperator._get_current_databricks_task"
)
def test_monitor_databricks_job_fail(
    mock_get_task_name,
    mock_get_api_client,
    mock_runs_api,
    mock_databricks_hook,
    databricks_notebook_operator,
):
    # Define the expected response
    response = {
        "run_page_url": "https://databricks-instance-xyz.cloud.databricks.com/#job/1234/run/1",
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "job failed",
        },
        "tasks": [
            {
                "run_id": "1",
                "task_key": "1",
            }
        ],
    }
    mock_runs_api.return_value.get_run.return_value = response

    databricks_notebook_operator.databricks_run_id = "1234"
    with pytest.raises(AirflowException):
        databricks_notebook_operator.monitor_databricks_job()

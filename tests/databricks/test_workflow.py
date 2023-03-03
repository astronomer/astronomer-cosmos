from unittest import mock

from cosmos.providers.databricks.notebook import DatabricksNotebookOperator
from cosmos.providers.databricks.workflow import DatabricksWorkflowTaskGroup

expected_workflow_json = {
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "format": "MULTI_TASK",
    "job_clusters": [{"job_cluster_key": "foo"}],
    "max_concurrent_runs": 1,
    "name": "unit_test_dag.test_workflow",
    "tasks": [
        {
            "depends_on": [],
            "email_notifications": {},
            "job_cluster_key": "foo",
            "libraries": [
                {"nb_index": {"package": "nb_package"}},
                {"tg_index": {"package": "tg_package"}},
            ],
            "notebook_task": {
                "base_parameters": {},
                "notebook_path": "/foo/bar",
                "source": "WORKSPACE",
            },
            "task_key": "unit_test_dag__test_workflow__notebook_1",
            "timeout_seconds": 0,
        },
        {
            "depends_on": [{"task_key": "unit_test_dag__test_workflow__notebook_1"}],
            "email_notifications": {},
            "job_cluster_key": "foo",
            "libraries": [{"tg_index": {"package": "tg_package"}}],
            "notebook_task": {
                "base_parameters": {"foo": "bar"},
                "notebook_path": "/foo/bar",
                "source": "WORKSPACE",
            },
            "task_key": "unit_test_dag__test_workflow__notebook_2",
            "timeout_seconds": 0,
        },
    ],
    "webhook_notifications": {},
    "timeout_seconds": 0,
}


@mock.patch("cosmos.providers.databricks.workflow.DatabricksHook")
@mock.patch("cosmos.providers.databricks.workflow.ApiClient")
@mock.patch("cosmos.providers.databricks.workflow.JobsApi")
def test_create_workflow_from_notebooks_with_create(
    mock_jobs_api, mock_api, mock_hook, dag
):
    mock_jobs_api.return_value.create_job.return_value = {"job_id": 1}
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
            notebook_packages=[{"tg_index": {"package": "tg_package"}}],
        )
        with task_group:
            notebook_1 = DatabricksNotebookOperator(
                task_id="notebook_1",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                notebook_packages=[{"nb_index": {"package": "nb_package"}}],
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
                notebook_params={
                    "foo": "bar",
                },
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    task_group.children["test_workflow.launch"].execute(context={})
    mock_jobs_api.return_value.create_job.assert_called_once_with(
        json=expected_workflow_json,
    )
    mock_jobs_api.return_value.run_now.assert_called_once_with(
        job_id=1,
        jar_params=[],
        notebook_params=[{"notebook_path": "/foo/bar"}],
        python_params=[],
        spark_submit_params=[],
    )


@mock.patch("cosmos.providers.databricks.workflow.DatabricksHook")
@mock.patch("cosmos.providers.databricks.workflow.ApiClient")
@mock.patch("cosmos.providers.databricks.workflow.JobsApi")
@mock.patch("cosmos.providers.databricks.workflow._get_job_by_name")
def test_create_workflow_from_notebooks_existing_job(
    mock_get_jobs, mock_jobs_api, mock_api, mock_hook, dag
):
    mock_get_jobs.return_value = {"job_id": 1}
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
            notebook_packages=[{"tg_index": {"package": "tg_package"}}],
        )
        with task_group:
            notebook_1 = DatabricksNotebookOperator(
                task_id="notebook_1",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                notebook_packages=[{"nb_index": {"package": "nb_package"}}],
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
                notebook_params={
                    "foo": "bar",
                },
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    task_group.children["test_workflow.launch"].execute(context={})
    mock_jobs_api.return_value.reset_job.assert_called_once_with(
        json={"job_id": 1, "new_settings": expected_workflow_json},
    )
    mock_jobs_api.return_value.run_now.assert_called_once_with(
        job_id=1,
        jar_params=[],
        notebook_params=[{"notebook_path": "/foo/bar"}],
        python_params=[],
        spark_submit_params=[],
    )


@mock.patch("cosmos.providers.databricks.workflow.DatabricksHook")
@mock.patch("cosmos.providers.databricks.workflow.ApiClient")
@mock.patch("cosmos.providers.databricks.workflow.JobsApi")
@mock.patch("cosmos.providers.databricks.workflow._get_job_by_name")
def test_create_workflow_with_arbitrary_extra_job_params(
    mock_get_jobs, mock_jobs_api, mock_api, mock_hook, dag
):
    mock_get_jobs.return_value = {"job_id": 862519602273592}

    extra_job_params = {
        "timeout_seconds": 10,  # default: 0
        "email_notifications": {"no_alert_for_skipped_runs": True},  # default: False
        "git_source": {  # no default value
            "git_url": "https://github.com/astronomer/astronomer-cosmos",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
    }
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
            extra_job_params=extra_job_params,
        )
        with task_group:
            notebook_with_extra = DatabricksNotebookOperator(
                task_id="notebook_with_extra",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_with_extra

    assert len(task_group.children) == 2

    task_group.children["test_workflow.launch"].create_workflow_json()
    task_group.children["test_workflow.launch"].execute(context={})

    mock_jobs_api.return_value.reset_job.assert_called_once()
    kwargs = mock_jobs_api.return_value.reset_job.call_args_list[0].kwargs["json"]

    assert kwargs["job_id"] == 862519602273592
    assert (
        kwargs["new_settings"]["email_notifications"]
        == extra_job_params["email_notifications"]
    )
    assert (
        kwargs["new_settings"]["timeout_seconds"] == extra_job_params["timeout_seconds"]
    )
    assert kwargs["new_settings"]["git_source"] == extra_job_params["git_source"]


@mock.patch("cosmos.providers.databricks.workflow.DatabricksHook")
@mock.patch("cosmos.providers.databricks.workflow.ApiClient")
@mock.patch("cosmos.providers.databricks.workflow.JobsApi")
@mock.patch("cosmos.providers.databricks.workflow._get_job_by_name")
def test_create_workflow_with_webhook_and_email_notifications(
    mock_get_jobs, mock_jobs_api, mock_api, mock_hook, dag
):
    mock_get_jobs.return_value = {"job_id": 123}

    webhook_notifications = {
        "on_failure": [{"id": "b0aea8ab-ea8c-4a45-a2e9-9a26753fd702"}],
    }
    email_notifications = {
        "no_alert_for_skipped_runs": False,
        "on_start": ["user.name@databricks.com"],
    }
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
            email_notifications=email_notifications,
            webhook_notifications=webhook_notifications,
        )
        with task_group:
            notebook_with_extra = DatabricksNotebookOperator(
                task_id="notebook_with_extra",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_with_extra

    assert len(task_group.children) == 2

    task_group.children["test_workflow.launch"].create_workflow_json()
    task_group.children["test_workflow.launch"].execute(context={})

    mock_jobs_api.return_value.reset_job.assert_called_once()
    kwargs = mock_jobs_api.return_value.reset_job.call_args_list[0].kwargs["json"]

    assert kwargs["job_id"] == 123
    assert kwargs["new_settings"]["webhook_notifications"] == webhook_notifications
    assert kwargs["new_settings"]["email_notifications"] == email_notifications

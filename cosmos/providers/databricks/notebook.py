"""DatabricksNotebookOperator for submitting notebook jobs to databricks."""
from __future__ import annotations

import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.operator import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.context import Context
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk import JobsService
from databricks_cli.sdk.api_client import ApiClient

from cosmos.providers.databricks.workflow import DatabricksMetaData


def _repair_task_retry_callback(context: Context) -> None:
    """
    This function allows the Airflow retry function to create a repair job for Databricks.
    It uses the Databricks API to get the latest repair ID before sending the repair query.

    Note that we use the `JobsService` class instead of the `RunsApi` class. This is because the
    `RunsApi` class does not allow sending the `include_history` parameter which is necessary for
    repair jobs.

    Also for the moment we don't allow custom retry_callbacks. We might implement this in
    the future if users ask for it, but for the moment we want to keep things simple while the API
    stabilizes.
    :return: None
    """
    current_task = context["ti"].task
    api_client = current_task._get_api_client()
    jobs_service = JobsService(api_client)
    current_job = jobs_service.get_run(
        run_id=current_task.databricks_run_id, include_history=True
    )
    repair_history = current_job.get("repair_history")
    repair_history_id = None
    if repair_history and len(repair_history) > 1:
        # We use the last item in the array to get the latest repair ID
        repair_history_id = repair_history[-1]["id"]
    jobs_service.repair(
        run_id=current_task.databricks_run_id,
        version="2.1",
        latest_repair_id=repair_history_id,
        rerun_tasks=[current_task._get_databricks_task_id()],
    )


class DatabricksNotebookOperator(BaseOperator):
    """
    Launches a notebook to databricks using an Airflow operator.

    The DatabricksNotebookOperator allows users to launch and monitor notebook
     deployments on Databricks as Aiflow tasks.
    It can be used as a part of a DatabricksWorkflowTaskGroup to take advantage of job clusters,
    which allows users to run their tasks on cheaper clusters that can be shared between tasks.

    Here is an example of running a notebook as a part of a workflow task group:

    .. code-block: python

        with dag:
            task_group = DatabricksWorkflowTaskGroup(
                group_id="test_workflow",
                databricks_conn_id="databricks_conn",
                job_clusters=job_cluster_spec,
                notebook_params=[],
            )
            with task_group:
                notebook_1 = DatabricksNotebookOperator(
                    task_id="notebook_1",
                    databricks_conn_id="databricks_conn",
                    notebook_path="/Users/daniel@astronomer.io/Test workflow",
                    source="WORKSPACE",
                    job_cluster_key="Shared_job_cluster",
                )
                notebook_2 = DatabricksNotebookOperator(
                    task_id="notebook_2",
                    databricks_conn_id="databricks_conn",
                    notebook_path="/Users/daniel@astronomer.io/Test workflow",
                    source="WORKSPACE",
                    job_cluster_key="Shared_job_cluster",
                    notebook_params={
                        "foo": "bar",
                    },
                )
                notebook_1 >> notebook_2


        :param notebook_path: the path to the notebook in Databricks
        :param source: Optional location type of the notebook. When set to WORKSPACE, the notebook will be retrieved
            from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git repository
            defined in git_source. If the value is empty, the task will use GIT if git_source is defined
            and WORKSPACE otherwise. For more information please visit
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
        :param databricks_conn_id: the connection id to use to connect to Databricks
        :param notebook_params: the parameters to pass to the notebook
    """

    template_fields = ("databricks_metadata",)

    def __init__(
        self,
        notebook_path: str,
        source: str,
        databricks_conn_id: str,
        notebook_params: dict | None = None,
        job_cluster_key: str | None = None,
        new_cluster: dict | None = None,
        existing_cluster_id: str | None = None,
        **kwargs,
    ):
        self.notebook_path = notebook_path
        self.source = source
        self.notebook_params = notebook_params or {}
        self.databricks_conn_id = databricks_conn_id
        self.databricks_metadata: DatabricksMetaData | None = None
        self.job_cluster_key = job_cluster_key or ""
        self.new_cluster = new_cluster or {}
        self.existing_cluster_id = existing_cluster_id or ""
        # kwargs["on_retry_callback"] = _repair_task_retry_callback
        super().__init__(**kwargs)

    def convert_to_databricks_workflow_task(
        self, relevant_upstreams: list[BaseOperator]
    ):
        """
        Convert the operator to a Databricks workflow task that can be task in a workflow
        """
        result = {
            "task_key": self._get_databricks_task_id(),
            "depends_on": [
                {"task_key": self.dag_id + "__" + t.replace(".", "__")}
                for t in self.upstream_task_ids
                if t in relevant_upstreams
            ],
            "job_cluster_key": self.job_cluster_key,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "source": self.source,
                "base_parameters": self.notebook_params,
            },
        }
        return result

    def _get_databricks_task_id(self):
        """Get the databricks task ID using dag_id and task_id. removes illegal characters."""
        return self.dag_id + "__" + self.task_id.replace(".", "__")

    def monitor_databricks_job(self):
        """Monitor the Databricks job until it completes. Raises Airflow exception if the job fails."""
        api_client = self._get_api_client()
        runs_api = RunsApi(api_client)
        current_task = self._get_current_databricks_task(runs_api)
        self._wait_for_pending_task(current_task, runs_api)
        self._wait_for_running_task(current_task, runs_api)
        self._wait_for_terminating_task(current_task, runs_api)
        final_state = runs_api.get_run(current_task["run_id"])["state"]
        self._handle_final_state(final_state)

    def _get_current_databricks_task(self, runs_api):
        return {
            x["task_key"]: x for x in runs_api.get_run(self.databricks_run_id)["tasks"]
        }[self._get_databricks_task_id()]

    def _handle_final_state(self, final_state):
        if final_state.get("life_cycle_state", None) != "TERMINATED":
            raise AirflowException(
                f"Databricks job failed with state {final_state}. Message: {final_state['state_message']}"
            )
        if final_state["result_state"] != "SUCCESS":
            raise AirflowException(
                "Task failed. Final State %s. Reason: %s",
                final_state["result_state"],
                final_state["state_message"],
            )

    def _get_lifestyle_state(self, current_task, runs_api):
        return runs_api.get_run(current_task["run_id"])["state"]["life_cycle_state"]

    def _wait_on_state(self, current_task, runs_api, state):
        while self._get_lifestyle_state(current_task, runs_api) == state:
            print(f"task {self.task_id.replace('.', '__')} {state.lower()}...")
            time.sleep(5)

    def _wait_for_terminating_task(self, current_task, runs_api):
        self._wait_on_state(current_task, runs_api, "TERMINATING")

    def _wait_for_running_task(self, current_task, runs_api):
        self._wait_on_state(current_task, runs_api, "RUNNING")

    def _wait_for_pending_task(self, current_task, runs_api):
        self._wait_on_state(current_task, runs_api, "PENDING")

    def _get_api_client(self):
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        return ApiClient(
            user=databricks_conn.login,
            password=databricks_conn.password,
            host=databricks_conn.host,
        )

    def launch_notebook_job(self):
        """Launch the notebook as a one-time job to Databricks."""
        api_client = self._get_api_client()
        run_json = {
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "base_parameters": {"source": self.source},
            },
        }
        if self.new_cluster and self.existing_cluster_id:
            raise ValueError(
                "Both new_cluster and existing_cluster_id are set. Only one can be set."
            )
        if self.existing_cluster_id:
            run_json["existing_cluster_id"] = self.existing_cluster_id
        elif self.new_cluster:
            run_json["new_cluster"] = self.new_cluster
        else:
            raise ValueError("Must specify either existing_cluster_id or new_cluster")
        runs_api = RunsApi(api_client)
        run = runs_api.submit_run(run_json)
        self.databricks_run_id = run["run_id"]
        return run

    def execute(self, context: Context) -> Any:
        """
        Execute the DataBricksNotebookOperator.

        Executes the DataBricksNotebookOperator. If the task is inside of a
        DatabricksWorkflowTaskGroup, it assumes the notebook is already launched
        and proceeds to monitor the running notebook.

        :param context:
        :return:
        """
        if not (
            hasattr(self.task_group, "is_databricks")
            and getattr(self.task_group, "is_databricks")
        ):
            self.launch_notebook_job()
        else:
            self.databricks_run_id = self.databricks_metadata.databricks_run_id
            self.databricks_conn_id = self.databricks_metadata.databricks_conn_id
        self.monitor_databricks_job()

"""DatabricksWorkflowTaskGroup for submitting jobs to Databricks."""
from __future__ import annotations

from logging import Logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass
import json
import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.context import Context
from airflow.utils.session import provide_session
from airflow.utils.task_group import TaskGroup
from airflow.www.auth import has_access
from airflow.www.views import AirflowBaseView
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk import JobsService
from databricks_cli.sdk.api_client import ApiClient
from flask import Blueprint, flash, redirect, request
from flask_appbuilder.api import expose
from mergedeep import merge


def _repair_task(databricks_conn_id, databricks_run_id) -> None:
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

    def _get_api_client():
        hook = DatabricksHook(databricks_conn_id)
        databricks_conn = hook.get_conn()
        return ApiClient(
            user=databricks_conn.login,
            password=databricks_conn.password,
            host=databricks_conn.host,
        )

    print("getting api client")
    api_client = _get_api_client()
    print("got api client")
    jobs_service = JobsService(api_client)
    current_job = jobs_service.get_run(run_id=databricks_run_id, include_history=True)
    print(f"got current job {current_job}")
    repair_history = current_job.get("repair_history")
    repair_history_id = None
    if repair_history and len(repair_history) > 1:
        # We use the last item in the array to get the latest repair ID
        repair_history_id = repair_history[-1]["id"]
        print(f"got repair history id {repair_history_id}")
    print("sending repair request")
    return jobs_service.repair(
        run_id=databricks_run_id,
        version="2.1",
        latest_repair_id=repair_history_id,
        # rerun_tasks=[current_task._get_databricks_task_id()],
    )


def _get_job_by_name(job_name: str, jobs_api: JobsApi) -> dict | None:
    jobs = jobs_api.list_jobs().get("jobs", [])
    for job in jobs:
        if job.get("settings", {}).get("name") == job_name:
            return job
    return None


class DatabricksJobRunLinkLocal(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    def get_link(
        self,
        operator: BaseOperator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        return f"/foo/bar?dag_id={operator.dag_id}&task_id={operator.task_id}"


class DatabricksJobRepairLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Repair All Failed Tasks"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        dag = get_airflow_app().dag_bag.get_dag(ti_key.dag_id)
        task = dag.get_task(ti_key.task_id)
        metadata = XCom.get_many(
            task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id
        )
        print(f"metadata: {metadata}")
        return f"/foo/bar?dag_id={ti_key.dag_id}&databricks_conn_id={task.databricks_conn_id}&databricks_run_id={task.databricks_run_id}"

    @provide_session
    def get_ti(self, ti_key: TaskInstanceKey, session=None) -> TaskInstance:
        return (
            session.query(TaskInstance)
            .where(TaskInstance.filter_for_tis([ti_key]))
            .one()
        )


bp = Blueprint(
    "test_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_url_path="/dags/<string:dag_id>/graph",
)


class DagGraphView(AirflowBaseView):
    default_view = "test"

    @expose("/foo/bar", methods=["GET"])
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    @provide_session
    def test(self, session=None):
        databricks_conn_id = request.values.get("databricks_conn_id")
        databricks_run_id = request.values.get("databricks_run_id")
        dag_id = request.values.get("dag_id")
        res = _repair_task(
            databricks_conn_id=databricks_conn_id, databricks_run_id=databricks_run_id
        )
        flash("Databricks repair job is starting!: " + res)
        return redirect(f"/dags/{dag_id}/grid")


v_appbuilder_view = DagGraphView()

v_appbuilder_package = {
    "name": "Test View",
    "category": "Test Plugin",
    "view": v_appbuilder_view,
}


class MyAirflowPlugin(AirflowPlugin):
    name = "my_namespace"
    operator_extra_links = [DatabricksJobRepairLink(), DatabricksJobRunLinkLocal()]
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]


from attrs import define


@define
class DatabricksMetaData:
    databricks_conn_id: str
    databricks_run_id: str


class _CreateDatabricksWorkflowOperator(BaseOperator):
    """Creates a databricks workflow from a DatabricksWorkflowTaskGroup.

    :param task_id: The task id of the operator
    :param databricks_conn_id: The databricks connection id
    :param job_clusters: A list of job clusters to use in the workflow
    :param existing_clusters: A list of existing clusters to use in the workflow
    :param max_concurrent_runs: The maximum number of concurrent runs
    :param tasks_to_convert: A list of tasks to convert to a workflow. This list can also
    be populated after initialization by calling add_task.
    :param extra_job_params: A dictionary containing properties which will override the
    default Databricks Workflow Job definitions.
    """

    operator_extra_links = (DatabricksJobRunLinkLocal(), DatabricksJobRepairLink())
    databricks_conn_id: str
    databricks_run_id: str

    def __init__(
        self,
        task_id,
        databricks_conn_id,
        job_clusters: list[dict[str, object]] = None,
        existing_clusters: list[str] = None,
        max_concurrent_runs: int = 1,
        tasks_to_convert: list[BaseOperator] = None,
        extra_job_params: dict[str, Any] = None,
        **kwargs,
    ):
        self.existing_clusters = existing_clusters or []
        self.job_clusters = job_clusters or []
        self.job_cluster_dict = {j["job_cluster_key"]: j for j in self.job_clusters}
        self.tasks_to_convert = tasks_to_convert or []
        self.relevant_upstreams = [task_id]
        self.databricks_conn_id = databricks_conn_id
        self.databricks_run_id = None
        self.max_concurrent_runs = max_concurrent_runs
        self.extra_job_params = extra_job_params
        super().__init__(task_id=task_id, **kwargs)

    def add_task(self, task: BaseOperator):
        """
        Add a task to the list of tasks to convert to a workflow.

        :param task:
        :return:
        """
        self.tasks_to_convert.append(task)

    def create_workflow_json(self) -> dict[str, object]:
        """Create a workflow json that can be submitted to databricks.

        :return: A workflow json
        """
        task_json = [
            task.convert_to_databricks_workflow_task(
                relevant_upstreams=self.relevant_upstreams
            )
            for task in self.tasks_to_convert
        ]
        full_json = {
            "name": self.databricks_job_name,
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "tasks": task_json,
            "format": "MULTI_TASK",
            "job_clusters": self.job_clusters,
            "max_concurrent_runs": self.max_concurrent_runs,
        }
        full_json = merge(full_json, self.extra_job_params)
        return full_json

    @property
    def databricks_job_name(self):
        return self.dag_id + "." + self.task_group.group_id

    def execute(self, context: Context) -> Any:
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            token=databricks_conn.password, host=databricks_conn.host
        )
        jobs_api = JobsApi(api_client)
        job = _get_job_by_name(self.databricks_job_name, jobs_api)

        job_id = job["job_id"] if job else None
        current_job_spec = self.create_workflow_json()
        if not isinstance(self.task_group, DatabricksWorkflowTaskGroup):
            raise AirflowException("Task group must be a DatabricksWorkflowTaskGroup")
        if job_id:
            self.log.info(
                "Updating existing job with spec %s",
                json.dumps(current_job_spec, indent=4),
            )

            jobs_api.reset_job(
                json={"job_id": job_id, "new_settings": current_job_spec}
            )
        else:
            self.log.info(
                "Creating new job with spec %s", json.dumps(current_job_spec, indent=4)
            )
            job_id = jobs_api.create_job(json=current_job_spec)["job_id"]

        run_id = jobs_api.run_now(
            job_id=job_id,
            jar_params=self.task_group.jar_params,
            notebook_params=self.task_group.notebook_params,
            python_params=self.task_group.python_params,
            spark_submit_params=self.task_group.spark_submit_params,
        )["run_id"]
        runs_api = RunsApi(api_client)

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
            print("job pending")
            time.sleep(5)
        self.databricks_run_id = run_id
        return DatabricksMetaData(
            databricks_conn_id=self.databricks_conn_id, databricks_run_id=run_id
        )


class DatabricksWorkflowTaskGroup(TaskGroup):
    """
    A task group that takes a list of tasks and creates a databricks workflow.

    The DatabricksWorkflowTaskGroup takes a list of tasks and creates a databricks workflow
    based on the metadata produced by those tasks. For a task to be eligible for this
    TaskGroup, it must contain the ``convert_to_databricks_workflow_task`` method. If any tasks
    do not contain this method then the Taskgroup will raise an error at parse time.

    Here is an example of what a DAG looks like with a DatabricksWorkflowTaskGroup:

    .. code-block:: python

        job_clusters = [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "11.3.x-scala2.12",
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-east-2b",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0,
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                    "enable_elastic_disk": False,
                    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                    "runtime_engine": "STANDARD",
                    "num_workers": 8,
                },
            }
        ]

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
                notebook_path="/Users/<user>/Test workflow",
                source="WORKSPACE",
                job_cluster_key="Shared_job_cluster",
            )
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2",
                databricks_conn_id="databricks_conn",
                notebook_path="/Users/<user>/Test workflow",
                source="WORKSPACE",
                job_cluster_key="Shared_job_cluster",
                notebook_params={
                    "foo": "bar",
                },
            )
            notebook_1 >> notebook_2

    With this example, Airflow will produce a job named <dag_name>.test_workflow that will
    run notebook_1 and then notebook_2. The job will be created in the databricks workspace
    if it does not already exist. If the job already exists, it will be updated to match
    the workflow defined in the DAG.

    To minimize update conflicts, we recommend that you keep parameters in the ``notebook_params`` of the
    ``DatabricksWorkflowTaskGroup`` and not in the ``DatabricksNotebookOperator`` whenever possible.
        This is because tasks in the
    ``DatabricksWorkflowTaskGroup`` are passed in at the job trigger time and do not modify the job definition

    :param group_id: The name of the task group
    :param databricks_conn_id: The name of the databricks connection to use
    :param job_clusters: A list of job clusters to use for this workflow.
    :param notebook_params: A list of notebook parameters to pass to the workflow.These parameters will be passed to
    all notebook tasks in the workflow.
    :param jar_params: A list of jar parameters to pass to the workflow. These parameters will be passed to all jar
        tasks
    in the workflow.
    :param python_params: A list of python parameters to pass to the workflow. These parameters will be passed to
        all python tasks
    in the workflow.
    :param spark_submit_params: A list of spark submit parameters to pass to the workflow. These parameters
        will be passed to all spark submit tasks
    :param extra_job_params: A dictionary containing properties which will override the default Databricks Workflow
    Job definitions.
    :param max_concurrent_runs: The maximum number of concurrent runs for this workflow.

    """

    @property
    def log(self) -> Logger:
        """Returns logger."""
        pass

    is_databricks = True

    def __init__(
        self,
        databricks_conn_id,
        existing_clusters=None,
        job_clusters=None,
        jar_params: dict = None,
        notebook_params: list = None,
        python_params: list = None,
        spark_submit_params: list = None,
        max_concurrent_runs: int = 1,
        extra_job_params: dict[str, Any] = None,
        **kwargs,
    ):
        """
        Create a new DatabricksWorkflowTaskGroup.

        :param group_id: The name of the task group
        :param databricks_conn_id: The name of the databricks connection to use
        :param job_clusters: A list of job clusters to use for this workflow.
        :param notebook_params: A list of notebook parameters to pass to the workflow.These parameters will be passed to
        all notebook tasks in the workflow.
        :param jar_params: A list of jar parameters to pass to the workflow.
         These parameters will be passed to all jar tasks
        in the workflow.
        :param python_params: A list of python parameters to pass to the workflow.
         These parameters will be passed to all python tasks
        in the workflow.
        :param spark_submit_params: A list of spark submit parameters to pass to the workflow.
         These parameters will be passed to all spark submit tasks
        :param max_concurrent_runs: The maximum number of concurrent runs for this workflow.
        :param extra_job_params: A dictionary containing properties which will override the default Databricks
        Workflow Job definitions.
        """
        self.databricks_conn_id = databricks_conn_id
        self.existing_clusters = existing_clusters or []
        self.job_clusters = job_clusters or []
        self.notebook_params = notebook_params or []
        self.python_params = python_params or []
        self.spark_submit_params = spark_submit_params or []
        self.jar_params = jar_params or []
        self.max_concurrent_runs = max_concurrent_runs
        self.extra_job_params = extra_job_params or {}
        super().__init__(**kwargs)

    def __exit__(self, _type, _value, _tb):
        """Exit the context manager and add tasks to a single _CreateDatabricksWorkflowOperator."""
        roots = self.roots
        create_databricks_workflow_task: _CreateDatabricksWorkflowOperator = (
            _CreateDatabricksWorkflowOperator(
                dag=self.dag,
                task_id="launch",
                databricks_conn_id=self.databricks_conn_id,
                job_clusters=self.job_clusters,
                existing_clusters=self.existing_clusters,
                extra_job_params=self.extra_job_params,
            )
        )

        for task in roots:
            if not (
                hasattr(task, "convert_to_databricks_workflow_task")
                and callable(task.convert_to_databricks_workflow_task)
            ):
                raise AirflowException(
                    f"Task {task.task_id} does not support conversion to databricks workflow task."
                )
            create_databricks_workflow_task.set_upstream(
                task_or_task_list=list(task.upstream_list)
            )

        for task_id, task in self.children.items():
            if task_id != f"{self.group_id}.launch":
                create_databricks_workflow_task.relevant_upstreams.append(task_id)
                create_databricks_workflow_task.add_task(task)
                task.databricks_metadata = create_databricks_workflow_task.output

        create_databricks_workflow_task.set_downstream(roots)
        super().__exit__(_type, _value, _tb)

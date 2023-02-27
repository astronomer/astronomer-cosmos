"""DatabricksWorkflowTaskGroup for submitting jobs to Databricks."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.session import provide_session
from airflow.www.auth import has_access
from airflow.www.views import AirflowBaseView
from databricks_cli.sdk import JobsService
from databricks_cli.sdk.api_client import ApiClient
from flask import flash, redirect, request
from flask_appbuilder.api import expose


def _get_databricks_task_id(task: BaseOperator) -> str:
    """Get the databricks task ID using dag_id and task_id. removes illegal characters.

    :param task: The task to get the databricks task ID for.
    :return: The databricks task ID.
    """
    return task.dag_id + "__" + task.task_id.replace(".", "__")


def get_databricks_task_ids(
    group_id: str, task_map: dict[str, BaseOperator]
) -> list[str]:
    """
    Returns a list of all Databricks task IDs for a dictionary of Airflow tasks.

    :param group_id: The task group ID.
    :param task_map: A dictionary mapping task IDs to BaseOperator instances.
    :return: A list of Databricks task IDs for the given task group.
    """
    task_ids = []
    print("Getting task ids for db")
    for task_id, task in task_map.items():
        if task_id != f"{group_id}.launch":
            print(f"databricks task id {_get_databricks_task_id(task)}")
            task_ids.append(_get_databricks_task_id(task))
    return task_ids


def _repair_task(
    databricks_conn_id: str, databricks_run_id: str, tasks_to_repair: list[str]
) -> None:
    """
    This function allows the Airflow retry function to create a repair job for Databricks.
    It uses the Databricks API to get the latest repair ID before sending the repair query.

    Note that we use the `JobsService` class instead of the `RunsApi` class. This is because the
    `RunsApi` class does not allow sending the `include_history` parameter which is necessary for
    repair jobs.

    Also for the moment we don't allow custom retry_callbacks. We might implement this in
    the future if users ask for it, but for the moment we want to keep things simple while the API
    stabilizes.

    :param databricks_conn_id: The Databricks connection ID.
    :param databricks_run_id: The Databricks run ID.
    :param tasks_to_repair: A list of Databricks task IDs to repair.
    :return: None
    """

    def _get_api_client():
        hook = DatabricksHook(databricks_conn_id)
        databricks_conn = hook.get_conn()
        return ApiClient(
            user=databricks_conn.login,
            token=databricks_conn.password,
            host=databricks_conn.host,
        )

    api_client = _get_api_client()
    jobs_service = JobsService(api_client)
    current_job = jobs_service.get_run(run_id=databricks_run_id, include_history=True)
    repair_history = current_job.get("repair_history")
    repair_history_id = None
    if repair_history and len(repair_history) > 1:
        # We use the last item in the array to get the latest repair ID
        repair_history_id = repair_history[-1]["id"]
    return jobs_service.repair(
        run_id=databricks_run_id,
        version="2.1",
        latest_repair_id=repair_history_id,
        rerun_tasks=tasks_to_repair,
    )


class DatabricksJobRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    def get_link(
        self,
        operator: BaseOperator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        dag = get_airflow_app().dag_bag.get_dag(ti_key.dag_id)
        dag.get_task(ti_key.task_id)
        # Should we catch the exception here if there is no return value?
        metadata = XCom.get_one(
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            key="return_value",
        )
        hook = DatabricksHook(metadata.databricks_conn_id)
        return f"https://{hook.host}/#job/{metadata.databricks_job_id}/run/{metadata.databricks_run_id}"
        return f"/foo/bar?dag_id={operator.dag_id}&task_id={operator.task_id}"


class DatabricksJobRepairAllFailedLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Repair All Failed Tasks"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        # Should we catch the exception here if there is no return value?
        metadata = XCom.get_one(
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            key="return_value",
        )

        tasks_str = self.get_tasks_to_run(ti_key, operator)
        print(f"tasks to rerun {tasks_str}")
        return (
            f"/repair_databricks_job?dag_id={ti_key.dag_id}&"
            f"databricks_conn_id={metadata.databricks_conn_id}&"
            f"databricks_run_id={metadata.databricks_run_id}&"
            f"tasks_to_repair={tasks_str}"
        )

    def get_tasks_to_run(self, ti_key: TaskInstanceKey, operator: BaseOperator) -> str:
        dag = get_airflow_app().dag_bag.get_dag(ti_key.dag_id)
        dr = self.get_dagrun(dag, ti_key.run_id)
        failed_and_skipped_tasks = self._get_failed_and_skipped_tasks(dr)
        tasks_to_run = {
            ti: t
            for ti, t in operator.task_group.children.items()
            if ti in failed_and_skipped_tasks
        }
        tasks_str = ",".join(
            get_databricks_task_ids(operator.task_group.group_id, tasks_to_run)
        )
        return tasks_str

    def _get_failed_and_skipped_tasks(self, dr: DagRun):
        """
        Returns a list of task IDs for tasks that have failed or have been skipped in the given DagRun.

        :param dr: The DagRun object for which to retrieve failed and skipped tasks.

        :return: A list of task IDs for tasks that have failed or have been skipped.
        """
        return [
            t.task_id
            for t in dr.get_task_instances(
                state=["failed", "skipped", "up_for_retry", "upstream_failed"],
            )
        ]

    @provide_session
    def get_dagrun(self, dag: DAG, run_id, session=None) -> DagRun:
        """
        Retrieves the DagRun object associated with the specified DAG and run_id.

        :param dag: The DAG object associated with the DagRun to retrieve.
        :param run_id: The run_id associated with the DagRun to retrieve.
        :param session: The SQLAlchemy session to use for the query. If None, uses the default session.
        :return: The DagRun object associated with the specified DAG and run_id.
        """
        return (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id, DagRun.run_id == run_id)
            .first()
        )

    @provide_session
    def get_ti(self, ti_key: TaskInstanceKey, session=None) -> TaskInstance:
        return (
            session.query(TaskInstance)
            .where(TaskInstance.filter_for_tis([ti_key]))
            .one()
        )


class DatabricksJobRepairSingleFailedLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Repair a single failed task"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        dag = get_airflow_app().dag_bag.get_dag(ti_key.dag_id)
        task = dag.get_task(ti_key.task_id)
        # Should we catch the exception here if there is no return value?
        metadata = XCom.get_one(
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            key="return_value",
        )

        return (
            f"/repair_databricks_job?dag_id={ti_key.dag_id}&"
            f"databricks_conn_id={metadata.databricks_conn_id}&"
            f"databricks_run_id={metadata.databricks_run_id}&"
            f"tasks_to_repair={_get_databricks_task_id(task)}"
        )


class RepairDatabricksTasks(AirflowBaseView):
    default_view = "repair"

    @expose("/repair_databricks_job", methods=["GET"])
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def repair(self):
        databricks_conn_id = request.values.get("databricks_conn_id")
        databricks_run_id = request.values.get("databricks_run_id")
        dag_id = request.values.get("dag_id")
        tasks_to_repair = request.values.get("tasks_to_repair").split(",")
        print(f"tasks_to_repair from params: {tasks_to_repair}")
        res = _repair_task(
            databricks_conn_id=databricks_conn_id,
            databricks_run_id=databricks_run_id,
            tasks_to_repair=tasks_to_repair,
        )
        flash(f"Databricks repair job is starting!: {res}")
        return redirect(f"/dags/{dag_id}/grid")


repair_databricks_view = RepairDatabricksTasks()

repair_databricks_package = {
    "name": "Test View",
    "category": "Test Plugin",
    "view": repair_databricks_view,
}


class CosmosDatabricksPlugin(AirflowPlugin):
    name = "my_namespace"
    operator_extra_links = [DatabricksJobRepairAllFailedLink(), DatabricksJobRunLink()]
    appbuilder_views = [repair_databricks_package]

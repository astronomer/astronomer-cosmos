"""DatabricksWorkflowTaskGroup for submitting jobs to Databricks."""
from __future__ import annotations

import logging
from operator import itemgetter

from airflow.configuration import conf
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.dag import DAG, clear_task_instances
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.security import permissions
from airflow.version import version as airflow_version
from packaging import version

try:
    # The following utility was included in Airflow version 2.3.3; we handle the needed import in the exception block.
    from airflow.utils.airflow_flask_app import get_airflow_app
except ModuleNotFoundError:
    # For older versions of airflow < 2.3.3 that don't have the utility.
    from flask import current_app

from airflow.exceptions import TaskInstanceNotFound
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.task_group import TaskGroup
from airflow.www.auth import has_access
from airflow.www.views import AirflowBaseView
from databricks_cli.sdk import JobsService
from databricks_cli.sdk.api_client import ApiClient
from flask import flash, redirect, request
from flask_appbuilder.api import expose
from sqlalchemy.orm.session import Session


def _get_flask_app():
    """Get the Airflow flask app instance"""
    try:
        flask_app = get_airflow_app()
    except NameError:
        flask_app = current_app
    return flask_app


def _get_databricks_task_id(task: BaseOperator) -> str:
    """Get the databricks task ID using dag_id and task_id. removes illegal characters.

    :param task: The task to get the databricks task ID for.
    :return: The databricks task ID.
    """
    return task.dag_id + "__" + task.task_id.replace(".", "__")


def get_databricks_task_ids(
    group_id: str, task_map: dict[str, BaseOperator], log: logging.Logger
) -> list[str]:
    """
    Returns a list of all Databricks task IDs for a dictionary of Airflow tasks.

    :param group_id: The task group ID.
    :param task_map: A dictionary mapping task IDs to BaseOperator instances.
    :return: A list of Databricks task IDs for the given task group.
    """
    task_ids = []
    log.debug("Getting databricks task ids for group %s", group_id)
    for task_id, task in task_map.items():
        if task_id == f"{group_id}.launch":
            continue
        databricks_task_id = _get_databricks_task_id(task)
        log.debug("databricks task id for task %s is %s", task_id, databricks_task_id)
        task_ids.append(databricks_task_id)
    return task_ids


@provide_session
def _get_dagrun(dag: DAG, run_id, session=None) -> DagRun:
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
def _clear_task_instances(
    dag_id: str, run_id: str, task_ids: list[str], log: logging.Logger, session=None
):
    dag = _get_flask_app().dag_bag.get_dag(dag_id)
    log.debug("task_ids to clear", str(task_ids))
    dr: DagRun = _get_dagrun(dag, run_id)
    tis_to_clear = [
        ti for ti in dr.get_task_instances() if _get_databricks_task_id(ti) in task_ids
    ]
    clear_task_instances(tis_to_clear, session)


def _repair_task(
    databricks_conn_id: str,
    databricks_run_id: str,
    tasks_to_repair: list[str],
    log: logging.Logger,
) -> dict:
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
    log.debug("Getting latest repair ID")
    jobs_service = JobsService(api_client)
    current_job = jobs_service.get_run(run_id=databricks_run_id, include_history=True)
    repair_history = current_job.get("repair_history")
    repair_history_id = None
    if (
        repair_history and len(repair_history) > 1
    ):  # We use >1 because the first entry is the original run.
        # We use the last item in the array to get the latest repair ID
        repair_history_id = repair_history[-1]["id"]
        log.debug("Latest repair ID is %s", repair_history_id)
    log.debug(
        "Sending repair query for tasks %s on run %s",
        tasks_to_repair,
        databricks_run_id,
    )
    return jobs_service.repair(
        run_id=databricks_run_id,
        version="2.1",
        latest_repair_id=repair_history_id,
        rerun_tasks=tasks_to_repair,
    )


def get_task_group_legacy(operator: BaseOperator) -> TaskGroup:
    """
    Returns the task group for a given operator. This is a workaround for Airflow 2.2.4.
    Unfortunately in Airflow 2.2.4 the task_group property is not set on the operator, so we
    have to get the taskgroup tree from the DAG and search for the operator. This allows us to
    return the operators group, so we can find the xcom result of the launch task.

    :param operator: The operator to get the task group for.
    :return: The task group for the given operator.
    """

    def find_my_group(group: TaskGroup, task_id: str):
        groups_to_recurse = set()
        for elem in group.children.values():
            if isinstance(elem, TaskGroup):
                groups_to_recurse.add(elem)
            else:
                if operator.task_id == task_id:
                    return group
        for group in groups_to_recurse:
            val = find_my_group(group, task_id)
            if val:
                return val

    return find_my_group(operator.dag.task_group, operator.task_id)


def _get_launch_task_key(
    current_task_key: TaskInstanceKey, task_id: str
) -> TaskInstanceKey:
    """
    Returns the task key for the launch task. This allows us to gather databricks Metadata
    even if the current task has failed (since tasks only create xcom values if they succeed).

    :param current_task_key: The task key for the current task.
    :param task_id: The task ID for the current task.
    :return: The task key for the launch task.
    """
    if task_id:
        return TaskInstanceKey(
            dag_id=current_task_key.dag_id,
            task_id=task_id,
            run_id=current_task_key.run_id,
            try_number=current_task_key.try_number,
        )
    else:
        return current_task_key


@provide_session
def get_task_instance(operator, dttm, session: Session = NEW_SESSION):
    dag_id = operator.dag.dag_id
    dag_run = DagRun.find(dag_id, execution_date=dttm)[0]
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.task_id == operator.task_id,
        )
        .one_or_none()
    )
    if not ti:
        raise TaskInstanceNotFound("Task instance not found")
    return ti


def get_task_group(operator):
    if not hasattr(operator, "task_group"):
        task_group = get_task_group_legacy(operator)
    else:
        task_group = operator.task_group
    return task_group


class DatabricksJobRunLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "See Databricks Job Run"

    def get_link(
        self,
        operator: BaseOperator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        ti = None
        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key
        task_group = get_task_group(operator)

        dag = _get_flask_app().dag_bag.get_dag(ti_key.dag_id)
        dag.get_task(ti_key.task_id)
        self.log.info("Getting link for task %s", ti_key.task_id)
        if ".launch" not in ti_key.task_id:
            self.log.debug(
                "Finding the launch task for job run metadata %s", ti_key.task_id
            )
            launch_task_id = task_group.get_child_by_label("launch").task_id
            ti_key = _get_launch_task_key(ti_key, task_id=launch_task_id)
        # Should we catch the exception here if there is no return value?
        try:
            metadata = XCom.get_value(
                ti_key=ti_key,
                key="return_value",
            )
        except AttributeError:
            if not ti:
                raise TaskInstanceNotFound()
            metadata = XCom.get_one(
                task_id=ti_key.task_id,
                dag_id=ti_key.dag_id,
                execution_date=ti.execution_date,
                key="return_value",
            )

        hook = DatabricksHook(metadata.databricks_conn_id)
        return f"https://{hook.host}/#job/{metadata.databricks_job_id}/run/{metadata.databricks_run_id}"


class DatabricksJobRepairAllFailedLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to send a request to repair all failed databricks tasks."""

    name = "Repair All Failed Tasks"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        ti = None
        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key
        task_group = get_task_group(operator)
        self.log.debug(
            "Creating link to repair all tasks for databricks job run %s",
            task_group.group_id,
        )
        # Should we catch the exception here if there is no return value?
        try:
            metadata = XCom.get_value(
                ti_key=ti_key,
                key="return_value",
            )
        except AttributeError:
            if not ti:
                raise TaskInstanceNotFound()
            metadata = XCom.get_one(
                task_id=ti_key.task_id,
                dag_id=ti_key.dag_id,
                execution_date=ti.execution_date,
                key="return_value",
            )

        tasks_str = self.get_tasks_to_run(ti_key, operator, self.log)
        self.log.debug("tasks to rerun: %s", tasks_str)
        return (
            f"/repair_databricks_job?dag_id={ti_key.dag_id}&"
            f"databricks_conn_id={metadata.databricks_conn_id}&"
            f"databricks_run_id={metadata.databricks_run_id}&"
            f"run_id={ti_key.run_id}&"
            f"tasks_to_repair={tasks_str}"
        )

    def get_tasks_to_run(
        self, ti_key: TaskInstanceKey, operator: BaseOperator, log: logging.Logger
    ) -> str:
        task_group = get_task_group(operator)
        dag = _get_flask_app().dag_bag.get_dag(ti_key.dag_id)
        dr = _get_dagrun(dag, ti_key.run_id)
        log.debug("Getting failed and skipped tasks for dag run %s", dr.run_id)
        failed_and_skipped_tasks = self._get_failed_and_skipped_tasks(dr)
        log.debug("Failed and skipped tasks: %s", failed_and_skipped_tasks)
        tasks_to_run = {
            ti: t
            for ti, t in task_group.children.items()
            if ti in failed_and_skipped_tasks
        }
        log.debug(
            "Tasks to repair in databricks job %s : %s",
            task_group.group_id,
            tasks_to_run,
        )
        tasks_str = ",".join(
            get_databricks_task_ids(task_group.group_id, tasks_to_run, log)
        )
        return tasks_str

    def _get_failed_and_skipped_tasks(self, dr: DagRun) -> list[str]:
        """
        Returns a list of task IDs for tasks that have failed or have been skipped in the given DagRun.

        :param dr: The DagRun object for which to retrieve failed and skipped tasks.

        :return: A list of task IDs for tasks that have failed or have been skipped.
        """
        return [
            t.task_id
            for t in dr.get_task_instances(
                state=["failed", "skipped", "up_for_retry", "upstream_failed", None],
            )
        ]


class DatabricksJobRepairSingleFailedLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to send a repair request for a single databricks task."""

    name = "Repair a single failed task"

    def get_link(
        self,
        operator,
        dttm=None,
        *,
        ti_key: TaskInstanceKey | None = None,
    ) -> str:
        ti = None
        if not ti_key:
            ti = get_task_instance(operator, dttm)
            ti_key = ti.key

        task_group = get_task_group(operator)

        self.log.info(
            "Creating link to repair a single task for databricks job run %s task %s",
            task_group.group_id,
            ti_key.task_id,
        )
        dag = _get_flask_app().dag_bag.get_dag(ti_key.dag_id)
        task = dag.get_task(ti_key.task_id)
        # Should we catch the exception here if there is no return value?
        if ".launch" not in ti_key.task_id:
            launch_task_id = task_group.get_child_by_label("launch").task_id
            ti_key = _get_launch_task_key(ti_key, task_id=launch_task_id)
        try:
            metadata = XCom.get_value(
                ti_key=ti_key,
                key="return_value",
            )

        except AttributeError:
            if not ti:
                raise TaskInstanceNotFound()
            metadata = XCom.get_one(
                task_id=ti_key.task_id,
                dag_id=ti_key.dag_id,
                execution_date=ti.execution_date,
                key="return_value",
            )


        return (
            f"/repair_databricks_job?dag_id={ti_key.dag_id}&"
            f"databricks_conn_id={metadata.databricks_conn_id}&"
            f"databricks_run_id={metadata.databricks_run_id}&"
            f"tasks_to_repair={_get_databricks_task_id(task)}&"
            f"run_id={ti_key.run_id}"
        )


class RepairDatabricksTasks(AirflowBaseView, LoggingMixin):
    default_view = "repair"

    @expose("/repair_databricks_job", methods=["GET"])
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def repair(self):
        databricks_conn_id, databricks_run_id, dag_id, tasks_to_repair = itemgetter(
            "databricks_conn_id", "databricks_run_id", "dag_id", "tasks_to_repair"
        )(request.values)
        view = conf.get("webserver", "dag_default_view")
        return_url = self._get_return_url(dag_id, view)
        run_id = request.values.get("run_id").replace(
            " ", "+"
        )  # get run id separately since we need to modify it
        if tasks_to_repair == "":
            # If there are no tasks to repair, we return.
            flash("No tasks to repair. Not sending repair request.")
            return redirect(return_url)
        self.log.info("Repairing databricks job %s", databricks_run_id)
        res = _repair_task(
            databricks_conn_id=databricks_conn_id,
            databricks_run_id=databricks_run_id,
            tasks_to_repair=tasks_to_repair.split(","),
            log=self.log,
        )
        self.log.info(
            "Repairing databricks job query for run %s sent", databricks_run_id
        )
        self.log.info("Clearing tasks to rerun in airflow")
        _clear_task_instances(dag_id, run_id, tasks_to_repair.split(","), self.log)
        flash(f"Databricks repair job is starting!: {res}")
        return redirect(return_url)

    @staticmethod
    def _get_return_url(dag_id, view):
        if version.parse(airflow_version) < version.parse("2.3.0"):
            return_url = f"/{view}?dag_id={dag_id}"
        else:
            return_url = f"/dags/{dag_id}/{view}"
        return return_url


repair_databricks_view = RepairDatabricksTasks()

repair_databricks_package = {
    "name": "Repair Databricks View",
    "category": "Repair Databricks Plugin",
    "view": repair_databricks_view,
}


class CosmosDatabricksPlugin(AirflowPlugin):
    name = "databricks_plugin"
    operator_extra_links = [
        DatabricksJobRepairAllFailedLink(),
        DatabricksJobRepairSingleFailedLink(),
        DatabricksJobRunLink(),
    ]
    appbuilder_views = [repair_databricks_package]

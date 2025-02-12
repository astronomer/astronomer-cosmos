from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import airflow
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from packaging.version import Version

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.dataset import get_dataset_alias_name
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import AbstractDbtLocalBase
from cosmos.settings import enable_setup_async_task, remote_target_path, remote_target_path_conn_id

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

AIRFLOW_VERSION = Version(airflow.__version__)


def _mock_bigquery_adapter() -> None:
    from typing import Optional, Tuple

    import agate
    from dbt.adapters.bigquery.connections import BigQueryAdapterResponse, BigQueryConnectionManager
    from dbt_common.clients.agate_helper import empty_table

    def execute(  # type: ignore[no-untyped-def]
        self, sql, auto_begin=False, fetch=None, limit: Optional[int] = None
    ) -> Tuple[BigQueryAdapterResponse, agate.Table]:
        return BigQueryAdapterResponse("mock_bigquery_adapter_response"), empty_table()

    BigQueryConnectionManager.execute = execute


def _configure_bigquery_async_op_args(async_op_obj: Any, **kwargs: Any) -> Any:
    sql = kwargs.get("sql")
    if not sql:
        raise CosmosValueError("Keyword argument 'sql' is required for BigQuery Async operator")
    async_op_obj.configuration = {
        "query": {
            "query": sql,
            "useLegacySql": False,
        }
    }
    return async_op_obj


class DbtRunAirflowAsyncBigqueryOperator(BigQueryInsertJobOperator, AbstractDbtLocalBase):  # type: ignore[misc]

    template_fields: Sequence[str] = ("gcp_project", "dataset", "location", "compiled_sql")
    template_fields_renderers = {
        "compiled_sql": "sql",
    }

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, Any] | None = None,
        dbt_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.project_dir = project_dir
        self.profile_config = profile_config
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        profile = self.profile_config.profile_mapping.profile  # type: ignore
        self.gcp_project = profile["project"]
        self.dataset = profile["dataset"]
        self.extra_context = extra_context or {}
        self.configuration: dict[str, Any] = {}
        self.dbt_kwargs = dbt_kwargs or {}
        task_id = self.dbt_kwargs.pop("task_id")
        AbstractDbtLocalBase.__init__(
            self, task_id=task_id, project_dir=project_dir, profile_config=profile_config, **self.dbt_kwargs
        )
        if kwargs.get("emit_datasets", True) and settings.enable_dataset_alias and AIRFLOW_VERSION >= Version("2.10"):
            from airflow.datasets import DatasetAlias

            # ignoring the type because older versions of Airflow raise the follow error in mypy
            # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")
            dag_id = kwargs.get("dag")
            task_group_id = kwargs.get("task_group")
            kwargs["outlets"] = [
                DatasetAlias(name=get_dataset_alias_name(dag_id, task_group_id, self.task_id))
            ]  # type: ignore
        super().__init__(
            gcp_conn_id=self.gcp_conn_id,
            configuration=self.configuration,
            deferrable=True,
            **kwargs,
        )
        self.async_context = extra_context or {}
        self.async_context["profile_type"] = self.profile_config.get_profile_type()
        self.async_context["async_operator"] = BigQueryInsertJobOperator
        self.compiled_sql = ""

    @property
    def base_cmd(self) -> list[str]:
        return ["run"]

    def get_remote_sql(self) -> str:
        start_time = time.time()

        if not settings.AIRFLOW_IO_AVAILABLE:  # pragma: no cover
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")
        from airflow.io.path import ObjectStoragePath

        file_path = self.async_context["dbt_node_config"]["file_path"]  # type: ignore
        dbt_dag_task_group_identifier = self.async_context["dbt_dag_task_group_identifier"]

        remote_target_path_str = str(remote_target_path).rstrip("/")

        if TYPE_CHECKING:  # pragma: no cover
            assert self.project_dir is not None

        project_dir_parent = str(Path(self.project_dir).parent)
        relative_file_path = str(file_path).replace(project_dir_parent, "").lstrip("/")
        remote_model_path = f"{remote_target_path_str}/{dbt_dag_task_group_identifier}/run/{relative_file_path}"

        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)
        with object_storage_path.open() as fp:  # type: ignore
            sql = fp.read()
            elapsed_time = time.time() - start_time
            self.log.info("SQL file download completed in %.2f seconds.", elapsed_time)
            return sql  # type: ignore

    def execute(self, context: Context, **kwargs: Any) -> None:
        if enable_setup_async_task:
            self.configuration = {
                "query": {
                    "query": self.get_remote_sql(),
                    "useLegacySql": False,
                }
            }
            super().execute(context=context)
        else:
            self.build_and_run_cmd(context=context, run_as_async=True, async_context=self.async_context)
        self._store_compiled_sql(context=context)

    @provide_session
    def _store_compiled_sql(self, context: Context, session: Session = NEW_SESSION) -> None:
        from airflow.models.renderedtifields import RenderedTaskInstanceFields
        from airflow.models.taskinstance import TaskInstance

        if not enable_setup_async_task:
            self.log.info("SQL cannot be made available, skipping registration of compiled_sql template field")
        sql = self.get_remote_sql().strip()
        self.log.info("Executed SQL is: %s", sql)
        self.compiled_sql = sql

        # need to refresh the rendered task field record in the db because Airflow only does this
        # before executing the task, not after
        ti = context["ti"]

        if isinstance(ti, TaskInstance):  # verifies ti is a TaskInstance in order to access and use the "task" field
            if TYPE_CHECKING:
                assert ti.task is not None
            ti.task.template_fields = self.template_fields
            rtif = RenderedTaskInstanceFields(ti, render_templates=False)

            # delete the old records
            session.query(RenderedTaskInstanceFields).filter(
                RenderedTaskInstanceFields.dag_id == self.dag_id,  # type: ignore[attr-defined]
                RenderedTaskInstanceFields.task_id == self.task_id,
                RenderedTaskInstanceFields.run_id == ti.run_id,
            ).delete()
            session.add(rtif)
        else:
            self.log.info("Warning: ti is of type TaskInstancePydantic. Cannot update template_fields.")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        job_id = super().execute_complete(context=context, event=event)
        self.log.info("Configuration is %s", str(self.configuration))
        self._store_compiled_sql(context=context)
        return job_id

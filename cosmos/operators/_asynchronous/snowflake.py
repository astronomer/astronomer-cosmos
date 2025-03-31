from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import airflow
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from packaging.version import Version

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.dataset import get_dataset_alias_name
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import AbstractDbtLocalBase
from cosmos.settings import remote_target_path, remote_target_path_conn_id

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

AIRFLOW_VERSION = Version(airflow.__version__)


def _mock_snowflake_adapter() -> None:
    from typing import Optional, Tuple

    import agate
    from dbt.adapters.snowflake.connections import AdapterResponse, SnowflakeConnectionManager

    try:
        from dbt_common.clients.agate_helper import empty_table
    except (ModuleNotFoundError, ImportError):
        from dbt.clients.agate_helper import empty_table

    def execute(  # type: ignore[no-untyped-def]
        self, sql, auto_begin=False, fetch=None, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        # Note that in the dbt-snowflake repository, there is no SnowflakeAdapterResponse (the way that there is
        # something similar for BigQuery.
        return AdapterResponse("mock_snowflake_adapter_response"), empty_table()

    SnowflakeConnectionManager.execute = execute


def _configure_snowflake_async_op_args(async_op_obj: Any, **kwargs: Any) -> Any:
    """
    _configure_snowflake_async_op_args

    The list of required fields is provided here: https://registry.astronomer.io/providers/ \
        apache-airflow-providers-common-sql/versions/1.18.0/modules/SQLExecuteQueryOperator

    :param async_op_obj: (Any)
    :param kwargs: (Any)
    """
    # This is only doing validation, unlike what is needed with BigQuery (actually setting configuration)
    sql = kwargs.get("sql")

    if not sql:
        raise CosmosValueError("Keyword argument 'sql' is required for Snowflake Async operator.")

    async_op_obj.sql = sql

    return async_op_obj


class DbtRunAirflowAsyncOperatorMixin:
    """Ideally, this logic should be put somewhere else. But it's here for now."""

    @staticmethod
    def configure_datasets(operator_kwargs: dict[str, Any], task_id: str) -> dict[str, Any]:
        """
        _configure_datasets

        :param operator_kwargs: (dict[str, Any])
        :param task_id: (str)
        :return: (dict)
        """
        from airflow.datasets import DatasetAlias

        # ignoring the type because older versions of Airflow raise the follow error in mypy
        # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")
        dag_id = operator_kwargs.get("dag")
        task_group_id = operator_kwargs.get("task_group")
        operator_kwargs["outlets"] = [DatasetAlias(name=get_dataset_alias_name(dag_id, task_group_id, task_id))]  # type: ignore

        return operator_kwargs

    @staticmethod
    def get_remote_sql(async_context: dict[str, Any], project_dir: str) -> str:
        # For logging
        log = logging.getLogger(__name__)
        start_time = time.time()

        if not settings.AIRFLOW_IO_AVAILABLE:  # pragma: no cover
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")

        from airflow.io.path import ObjectStoragePath

        # file_path: None
        # dbt_dag_task_group_identifier: for DAG/TaskGroup Cosmos builds, pull the ID
        # remote: target_path_str: storage location for compiled SQL
        file_path = async_context["dbt_node_config"]["file_path"]  # type: ignore
        dbt_dag_task_group_identifier = async_context["dbt_dag_task_group_identifier"]
        remote_target_path_str = str(remote_target_path).rstrip("/")

        if TYPE_CHECKING:  # pragma: no cover
            assert project_dir is not None

        # project_dir_parent: None
        # relative_file_path: None
        # remote_model_path: str path to be used when configuring the ObjectStoragePath
        # object_storage_path: ObjectStoragePath where compiled SQL/models are stored
        project_dir_parent = str(Path(project_dir).parent)
        relative_file_path = str(file_path).replace(project_dir_parent, "").lstrip("/")
        remote_model_path = f"{remote_target_path_str}/{dbt_dag_task_group_identifier}/run/{relative_file_path}"
        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)

        with object_storage_path.open() as fp:  # type: ignore
            sql = fp.read()
            elapsed_time = time.time() - start_time
            log.info("SQL file download completed in %.2f seconds.", elapsed_time)
            return sql  # type: ignore


# This is no longer a SnowflakeOperator, meaning the SQLExecuteQueryOperator needs to be used. Does this make this
# approach more widely applicable?

# The SnowflakeOperator does not have deferrable support. We'll need to figure out another way to do this.


class DbtRunAirflowAsyncSnowflakeOperator(SnowflakeSqlApiOperator, AbstractDbtLocalBase):  # type: ignore[misc]
    template_fields: Sequence[str] = ("dataset", "compiled_sql")  # BigQuery included: gcp_project and location
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
        self.extra_context = extra_context or {}
        self.dbt_kwargs = dbt_kwargs or {}

        self.snowflake_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        self.configuration: dict[str, Any] = {}
        task_id = self.dbt_kwargs.pop("task_id")

        AbstractDbtLocalBase.__init__(
            self,
            task_id=task_id,
            project_dir=project_dir,
            profile_config=profile_config,
            **self.dbt_kwargs,
        )

        # Configure datasets. This uses the task_id set by AbstractDbtLocalBase
        if kwargs.get("emit_datasets", True) and settings.enable_dataset_alias and AIRFLOW_VERSION >= Version("2.10"):
            kwargs = DbtRunAirflowAsyncOperatorMixin.configure_datasets(operator_kwargs=kwargs, task_id=self.task_id)

        super().__init__(
            snowflake_conn_id=self.snowflake_conn_id,
            sql=None,  # Setting to None, this will be updated later
            deferrable=True,
            **kwargs,
        )

        self.async_context = extra_context or {}
        self.async_context["profile_type"] = self.profile_config.get_profile_type()
        self.async_context["async_operator"] = SnowflakeSqlApiOperator

        self.dataset: str = ""  # TODO: Why are we doing this

    @property
    def base_cmd(self) -> list[str]:
        return ["run"]

    def execute(self, context: Context, **kwargs: Any) -> None:
        if settings.enable_setup_async_task:
            # sql is a required parameter for the SnowflakeSqlApiOperator, so this needs to be set. However, there are
            # no other required fields. The assumption is that this will be passed in via the connection
            self.sql = DbtRunAirflowAsyncOperatorMixin.get_remote_sql(
                async_context=self.async_context, project_dir=self.project_dir
            )
            super().execute(context, **kwargs)
        else:
            self.build_and_run_cmd(context=context, run_as_async=True, async_context=self.async_context)
        self._store_template_fields(context=context)

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        # No need to capture the output, it will always be None
        super().execute_complete(context, event)

        self.log.info("Configuration is %s", str(self.configuration))
        self._store_template_fields(context=context)

    @provide_session
    def _store_template_fields(self, context: Context, session: Session = NEW_SESSION) -> None:
        if not settings.enable_setup_async_task:
            self.log.info("SQL cannot be made available, skipping registration of compiled_sql template field")
            return

        # TODO: We'd need to figure this out
        sql = DbtRunAirflowAsyncOperatorMixin.get_remote_sql(
            async_context=self.async_context, project_dir=self.project_dir  # These would need to be passed in
        ).strip()

        self.log.debug("Executed SQL is: %s", sql)
        self.compiled_sql = sql

        if self.profile_config.profile_mapping is not None:
            profile = self.profile_config.profile_mapping.profile
        else:
            raise CosmosValueError(
                "The `profile_config.profile`_mapping attribute must be defined to use `ExecutionMode.AIRFLOW_ASYNC`"
            )

        # self.gcp_project = profile["project"]  # This should only needed for BigQuery
        self.dataset = profile["dataset"]  # TODO: Figure this out
        self._refresh_template_fields(context=context, session=session)


# Last line of file

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

import airflow
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from packaging.version import Version

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.dataset import configure_datasets
from cosmos.exceptions import CosmosValueError
from cosmos.io import get_remote_sql
from cosmos.operators.local import AbstractDbtLocalBase

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
        # something similar for BigQuery).
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


class DbtRunAirflowAsyncSnowflakeOperator(SnowflakeSqlApiOperator, AbstractDbtLocalBase):  # type: ignore[misc]
    template_fields: Sequence[str] = ("compiled_sql", "freshness")
    template_fields_renderers = {"compiled_sql": "sql", "freshness": "json"}

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
            configure_datasets(operator_kwargs=kwargs, task_id=self.task_id)

        super().__init__(
            snowflake_conn_id=self.snowflake_conn_id,
            sql=None,  # Setting to None, this will be updated later
            deferrable=True,
            **kwargs,
        )

        self.async_context = extra_context or {}
        self.async_context["profile_type"] = self.profile_config.get_profile_type()
        self.async_context["async_operator"] = SnowflakeSqlApiOperator

    @property
    def base_cmd(self) -> list[str]:
        return ["run"]

    def execute(self, context: Context, **kwargs: Any) -> None:
        if settings.enable_setup_async_task:
            # sql is a required parameter for the SnowflakeSqlApiOperator, so this needs to be set. However, there are
            # no other required fields. The assumption is that this will be passed in via the connection
            self.sql = get_remote_sql(async_context=self.async_context, project_dir=self.project_dir)
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

        sql = get_remote_sql(async_context=self.async_context, project_dir=self.project_dir).strip()

        self.log.debug("Executed SQL is: %s", sql)
        self.compiled_sql = sql

        if not self.profile_config.profile_mapping:
            raise CosmosValueError(
                "The `profile_config.profile`_mapping attribute must be defined to use `ExecutionMode.AIRFLOW_ASYNC`"
            )

        self._refresh_template_fields(context=context, session=session)


# Last line of file

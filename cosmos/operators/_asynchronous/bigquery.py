from __future__ import annotations

from typing import Any, Sequence

import airflow
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context
from packaging.version import Version

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.dataset import get_dataset_alias_name
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import AbstractDbtLocalBase

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

    template_fields: Sequence[str] = (
        "gcp_project",
        "dataset",
        "location",
    )

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

    @property
    def base_cmd(self) -> list[str]:
        return ["run"]

    def execute(self, context: Context, **kwargs: Any) -> None:
        self.build_and_run_cmd(context=context, run_as_async=True, async_context=self.async_context)

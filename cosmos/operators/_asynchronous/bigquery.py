from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos import settings
from cosmos.config import ProfileConfig
from cosmos.exceptions import CosmosValueError
from cosmos.settings import remote_target_path, remote_target_path_conn_id


class DbtRunAirflowAsyncBigqueryOperator(BigQueryInsertJobOperator):  # type: ignore[misc]

    template_fields: Sequence[str] = (
        "full_refresh",
        "gcp_project",
        "dataset",
        "location",
    )

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.project_dir = project_dir
        self.profile_config = profile_config
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        profile = self.profile_config.profile_mapping.profile  # type: ignore
        self.gcp_project = profile["project"]
        self.dataset = profile["dataset"]
        self.extra_context = extra_context or {}
        self.full_refresh = None
        if "full_refresh" in kwargs:
            self.full_refresh = kwargs.pop("full_refresh")
        self.configuration: dict[str, Any] = {}
        super().__init__(
            gcp_conn_id=self.gcp_conn_id,
            configuration=self.configuration,
            deferrable=True,
            **kwargs,
        )

    def get_remote_sql(self) -> str:
        if not settings.AIRFLOW_IO_AVAILABLE:
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")
        from airflow.io.path import ObjectStoragePath

        file_path = self.extra_context["dbt_node_config"]["file_path"]  # type: ignore
        dbt_dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]

        remote_target_path_str = str(remote_target_path).rstrip("/")

        if TYPE_CHECKING:  # pragma: no cover
            assert self.project_dir is not None

        project_dir_parent = str(Path(self.project_dir).parent)
        relative_file_path = str(file_path).replace(project_dir_parent, "").lstrip("/")
        remote_model_path = f"{remote_target_path_str}/{dbt_dag_task_group_identifier}/compiled/{relative_file_path}"

        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)
        with object_storage_path.open() as fp:  # type: ignore
            return fp.read()  # type: ignore

    def drop_table_sql(self) -> None:
        model_name = self.extra_context["dbt_node_config"]["resource_name"]  # type: ignore
        sql = f"DROP TABLE IF EXISTS {self.gcp_project}.{self.dataset}.{model_name};"

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.configuration = {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        }
        hook.insert_job(configuration=self.configuration, location=self.location, project_id=self.gcp_project)

    def execute(self, context: Context) -> Any | None:

        if not self.full_refresh:
            raise CosmosValueError("The async execution only supported for full_refresh")
        else:
            # It may be surprising to some, but the dbt-core --full-refresh argument fully drops the table before populating it
            # https://github.com/dbt-labs/dbt-core/blob/5e9f1b515f37dfe6cdae1ab1aa7d190b92490e24/core/dbt/context/base.py#L662-L666
            # https://docs.getdbt.com/reference/resource-configs/full_refresh#recommendation
            # We're emulating this behaviour here
            self.drop_table_sql()
            sql = self.get_remote_sql()
            model_name = self.extra_context["dbt_node_config"]["resource_name"]  # type: ignore
            # prefix explicit create command to create table
            sql = f"CREATE TABLE {self.gcp_project}.{self.dataset}.{model_name} AS  {sql}"
            self.configuration = {
                "query": {
                    "query": sql,
                    "useLegacySql": False,
                }
            }
            return super().execute(context)

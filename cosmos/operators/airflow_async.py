from __future__ import annotations

from typing import Any, Sequence

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos import settings
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCompileLocalOperator,
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)
from cosmos.settings import remote_target_path, remote_target_path_conn_id

_SUPPORTED_DATABASES = ["bigquery"]


class DbtBuildAirflowAsyncOperator(DbtBuildLocalOperator):
    pass


class DbtLSAirflowAsyncOperator(DbtLSLocalOperator):
    pass


class DbtSeedAirflowAsyncOperator(DbtSeedLocalOperator):
    pass


class DbtSnapshotAirflowAsyncOperator(DbtSnapshotLocalOperator):
    pass


class DbtSourceAirflowAsyncOperator(DbtSourceLocalOperator):
    pass


class DbtRunAirflowAsyncOperator(BigQueryInsertJobOperator):  # type: ignore

    template_fields: Sequence[str] = (
        "full_refresh",
        "project_dir",
        "gcp_project",
        "dataset",
        "location",
    )

    def __init__(self, *args, full_refresh: bool = False, **kwargs):  # type: ignore
        # dbt task param
        self.profile_config = kwargs.get("profile_config")
        self.project_dir = kwargs.get("project_dir")
        self.extra_context = kwargs.get("extra_context", {})
        self.profile_type: str = self.profile_config.get_profile_type()  # type: ignore
        self.full_refresh = full_refresh

        # airflow task param
        self.configuration: dict[str, object] = {}
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        if not self.profile_config or not self.profile_config.profile_mapping:
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")
        profile = self.profile_config.profile_mapping.profile
        self.gcp_project = profile["project"]
        self.dataset = profile["dataset"]
        self.location = kwargs.get("location", "northamerica-northeast1")  # TODO: must be provided by users
        super().__init__(
            *args, configuration=self.configuration, location=self.location, task_id=kwargs["task_id"], deferrable=True
        )

        if self.profile_type not in _SUPPORTED_DATABASES:
            raise CosmosValueError(f"Async run are only supported: {_SUPPORTED_DATABASES}")

    def get_remote_sql(self) -> str:
        if not settings.AIRFLOW_IO_AVAILABLE:
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")
        from airflow.io.path import ObjectStoragePath

        file_path = self.extra_context["dbt_node_config"]["file_path"]
        dbt_dag_task_group_identifier = self.extra_context["dbt_dag_task_group_identifier"]

        remote_target_path_str = str(remote_target_path).rstrip("/")
        project_dir_parent = str(self.project_dir.parent)
        relative_file_path = str(file_path).replace(project_dir_parent, "").lstrip("/")
        remote_model_path = f"{remote_target_path_str}/{dbt_dag_task_group_identifier}/compiled/{relative_file_path}"

        print("remote_model_path: ", remote_model_path)
        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)
        with object_storage_path.open() as fp:  # type: ignore
            return fp.read()  # type: ignore

    def drop_table_sql(self) -> None:
        model_name = self.extra_context["dbt_node_config"]["resource_name"]
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
            self.drop_table_sql()

            sql = self.get_remote_sql()
            model_name = self.extra_context["dbt_node_config"]["resource_name"]
            # prefix explicit create command to create table
            sql = f"CREATE TABLE {self.gcp_project}.{self.dataset}.{model_name} AS  {sql}"

            self.configuration = {
                "query": {
                    "query": sql,
                    "useLegacySql": False,
                }
            }
            return super().execute(context)


class DbtTestAirflowAsyncOperator(DbtTestLocalOperator):
    pass


class DbtRunOperationAirflowAsyncOperator(DbtRunOperationLocalOperator):
    pass


class DbtCompileAirflowAsyncOperator(DbtCompileLocalOperator):
    pass

from __future__ import annotations

from typing import Any

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
    def __init__(self, *args, full_refresh: bool = False, **kwargs):  # type: ignore
        # dbt task param
        self.profile_config = kwargs.get("profile_config")
        self.project_dir = kwargs.get("project_dir")
        self.file_path = kwargs.get("extra_context", {}).get("dbt_node_config", {}).get("file_path")
        self.profile_type: str = self.profile_config.get_profile_type()  # type: ignore
        self.full_refresh = full_refresh

        # airflow task param
        self.async_op_args = kwargs.pop("async_op_args", {})
        self.configuration: dict[str, object] = {}
        self.job_id = self.async_op_args.get("job_id", "")
        self.impersonation_chain = self.async_op_args.get("impersonation_chain", "")
        self.gcp_project = self.async_op_args.get("project_id", "astronomer-dag-authoring")
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore
        self.dataset = self.async_op_args.get("dataset", "my_dataset")
        self.location = self.async_op_args.get("location", "US")
        self.async_op_args["deferrable"] = True
        self.reattach_states: set[str] = self.async_op_args.get("reattach_states") or set()

        super().__init__(*args, configuration=self.configuration, task_id=kwargs.get("task_id"), **self.async_op_args)

        if self.profile_type not in _SUPPORTED_DATABASES:
            raise CosmosValueError(f"Async run are only supported: {_SUPPORTED_DATABASES}")

    def get_remote_sql(self) -> str:
        if not settings.AIRFLOW_IO_AVAILABLE:
            raise CosmosValueError(f"Cosmos async support is only available starting in Airflow 2.8 or later.")
        from airflow.io.path import ObjectStoragePath

        if not self.file_path or not self.project_dir:
            raise CosmosValueError("file_path and project_dir are required to be set on the task for async execution")
        project_dir_parent = str(self.project_dir.parent)
        relative_file_path = str(self.file_path).replace(project_dir_parent, "").lstrip("/")
        remote_model_path = f"{str(remote_target_path).rstrip('/')}/{self.dag_id}/{relative_file_path}"

        print("remote_model_path: ", remote_model_path)
        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)
        with object_storage_path.open() as fp:  # type: ignore
            return fp.read()  # type: ignore

    def drop_table_sql(self) -> None:
        model_name = self.task_id.split(".")[0]
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
            model_name = self.task_id.split(".")[0]
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

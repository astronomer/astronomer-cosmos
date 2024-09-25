from __future__ import annotations

from typing import Any

from airflow.io.path import ObjectStoragePath
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos.operators.base import DbtCompileMixin
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtDepsLocalOperator,
    DbtDocsAzureStorageLocalOperator,
    DbtDocsCloudLocalOperator,
    DbtDocsGCSLocalOperator,
    DbtDocsLocalOperator,
    DbtDocsS3LocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
    DbtRunOperationLocalOperator,
    DbtCompileLocalOperator,
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


class DbtRunAirflowAsyncOperator(BigQueryInsertJobOperator):
    def __init__(self, *args, **kwargs):
        self.configuration = {}
        self.job_id = kwargs.get("job_id", {}) or ""
        self.impersonation_chain = kwargs.get("impersonation_chain", {}) or ""
        self.project_id = kwargs.get("project_id", {}) or ""

        self.profile_config = kwargs.get("profile_config")
        self.project_dir = kwargs.get("project_dir")

        self.async_op_args = kwargs.get("async_op_args", {})
        self.async_op_args["deferrable"] = True
        super().__init__(*args, configuration=self.configuration, task_id=kwargs.get("task_id"), **self.async_op_args)
        self.profile_type = self.profile_config.get_profile_type()
        if self.profile_type not in _SUPPORTED_DATABASES:
            raise f"Async run are only supported: {_SUPPORTED_DATABASES}"

        self.reattach_states: set[str] = self.async_op_args.get("reattach_states") or set()

    def get_remote_sql(self):
        project_name = str(self.project_dir).split("/")[-1]
        model_name: str = self.task_id.split(".")[0]
        if model_name.startswith("stg_"):
            remote_model_path = f"{remote_target_path}/{project_name}/models/staging/{model_name}.sql"
        else:
            remote_model_path = f"{remote_target_path}/{project_name}/models/{model_name}.sql"

        print("remote_model_path: ", remote_model_path)
        object_storage_path = ObjectStoragePath(remote_model_path, conn_id=remote_target_path_conn_id)
        with object_storage_path.open() as fp:
            return fp.read()

    def execute(self, context: Context) -> Any | None:
        sql = self.get_remote_sql()
        print("sql: ", sql)
        self.configuration = {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        }
        print("async_op_args: ", self.async_op_args)
        super().execute(context)



class DbtTestAirflowAsyncOperator(DbtTestLocalOperator):
    pass


class DbtRunOperationAirflowAsyncOperator(DbtRunOperationLocalOperator):
    pass


class DbtDocsAirflowAsyncOperator(DbtDocsLocalOperator):
    pass


class DbtDocsCloudAirflowAsyncOperator(DbtDocsCloudLocalOperator):
    pass


class DbtDocsS3AirflowAsyncOperator(DbtDocsS3LocalOperator):
    pass


class DbtDocsAzureStorageAirflowAsyncOperator(DbtDocsAzureStorageLocalOperator):
    pass


class DbtDocsGCSAirflowAsyncOperator(DbtDocsGCSLocalOperator):
    pass


class DbtCompileAirflowAsyncOperator(DbtCompileLocalOperator):
    pass

class DbtDepsAirflowAsyncOperator(DbtDepsLocalOperator):
    pass



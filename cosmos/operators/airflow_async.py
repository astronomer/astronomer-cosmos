from __future__ import annotations

from typing import Any, Sequence

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.constants import BIGQUERY_PROFILE_TYPE
from cosmos.exceptions import CosmosValueError
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtCompileLocalOperator,
    DbtLSLocalOperator,
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)

_SUPPORTED_DATABASES = [BIGQUERY_PROFILE_TYPE]

from abc import ABCMeta

from airflow.models.baseoperator import BaseOperator


class DbtBaseAirflowAsyncOperator(BaseOperator, metaclass=ABCMeta):
    def __init__(self, **kwargs) -> None:  # type: ignore
        self.location = kwargs.pop("location")
        self.configuration = kwargs.pop("configuration", {})
        super().__init__(**kwargs)


class DbtBuildAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtBuildLocalOperator):  # type: ignore
    pass


class DbtLSAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtLSLocalOperator):  # type: ignore
    pass


class DbtSeedAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtSeedLocalOperator):  # type: ignore
    pass


class DbtSnapshotAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtSnapshotLocalOperator):  # type: ignore
    pass


class DbtSourceAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtSourceLocalOperator):  # type: ignore
    pass


class DbtRunAirflowAsyncOperator(BigQueryInsertJobOperator, DbtRunLocalOperator):  # type: ignore

    template_fields: Sequence[str] = (
        "full_refresh",
        "project_dir",
        "location",
    )

    def __init__(  # type: ignore
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        location: str,  # This is a mandatory parameter when using BigQueryInsertJobOperator with deferrable=True
        full_refresh: bool = False,
        extra_context: dict[str, object] | None = None,
        configuration: dict[str, object] | None = None,
        **kwargs,
    ) -> None:
        # dbt task param
        self.project_dir = project_dir
        self.full_refresh = full_refresh
        self.profile_config = profile_config
        if not self.profile_config or not self.profile_config.profile_mapping:
            raise CosmosValueError(f"Cosmos async support is only available when using ProfileMapping")

        self.profile_type: str = profile_config.get_profile_type()  # type: ignore
        if self.profile_type not in _SUPPORTED_DATABASES:
            raise CosmosValueError(f"Async run are only supported: {_SUPPORTED_DATABASES}")

        # airflow task param
        self.location = location
        self.configuration = configuration or {}
        self.gcp_conn_id = self.profile_config.profile_mapping.conn_id  # type: ignore

        super().__init__(
            project_dir=self.project_dir,
            profile_config=self.profile_config,
            gcp_conn_id=self.gcp_conn_id,
            configuration=self.configuration,
            location=self.location,
            deferrable=True,
            **kwargs,
        )
        self.extra_context = extra_context or {}
        self.extra_context["profile_type"] = self.profile_type

    def execute(self, context: Context) -> Any | None:
        sql = self.build_and_run_cmd(context, return_sql=True, sql_context=self.extra_context)
        self.configuration = {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        }
        return super().execute(context)


class DbtTestAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtTestLocalOperator):  # type: ignore
    pass


class DbtRunOperationAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtRunOperationLocalOperator):  # type: ignore
    pass


class DbtCompileAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCompileLocalOperator):  # type: ignore
    pass


class DbtCloneAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCloneLocalOperator):
    pass

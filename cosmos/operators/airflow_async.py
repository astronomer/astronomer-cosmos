from __future__ import annotations

import inspect
from typing import Any, Sequence

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.constants import BIGQUERY_PROFILE_TYPE
from cosmos.exceptions import CosmosValueError
from cosmos.operators.base import AbstractDbtBaseOperator
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtCompileLocalOperator,
    DbtLocalBaseOperator,
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

    template_fields: Sequence[str] = DbtRunLocalOperator.template_fields + (  # type: ignore[operator]
        "full_refresh",
        "project_dir",
        "gcp_project",
        "dataset",
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
        profile = self.profile_config.profile_mapping.profile
        self.gcp_project = profile["project"]
        self.dataset = profile["dataset"]

        # Cosmos attempts to pass many kwargs that BigQueryInsertJobOperator simply does not accept.
        # We need to pop them.
        async_op_kwargs = {}
        cosmos_op_kwargs = {}
        non_async_args = set(inspect.signature(AbstractDbtBaseOperator.__init__).parameters.keys())
        non_async_args |= set(inspect.signature(DbtLocalBaseOperator.__init__).parameters.keys())

        for arg_key, arg_value in kwargs.items():
            if arg_key == "task_id":
                async_op_kwargs[arg_key] = arg_value
                cosmos_op_kwargs[arg_key] = arg_value
            elif arg_key not in non_async_args:
                async_op_kwargs[arg_key] = arg_value
            else:
                cosmos_op_kwargs[arg_key] = arg_value

        # The following are the minimum required parameters to run BigQueryInsertJobOperator using the deferrable mode
        BigQueryInsertJobOperator.__init__(
            self,
            gcp_conn_id=self.gcp_conn_id,
            configuration=self.configuration,
            location=self.location,
            deferrable=True,
            **async_op_kwargs,
        )

        DbtRunLocalOperator.__init__(
            self,
            project_dir=self.project_dir,
            profile_config=self.profile_config,
            **cosmos_op_kwargs,
        )
        self.async_context = extra_context or {}
        self.async_context["profile_type"] = self.profile_type
        self.async_context["async_operator"] = BigQueryInsertJobOperator

    def execute(self, context: Context) -> Any | None:
        return self.build_and_run_cmd(context, run_as_async=True, async_context=self.async_context)


class DbtTestAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtTestLocalOperator):  # type: ignore
    pass


class DbtRunOperationAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtRunOperationLocalOperator):  # type: ignore
    pass


class DbtCompileAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCompileLocalOperator):  # type: ignore
    pass


class DbtCloneAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCloneLocalOperator):
    pass

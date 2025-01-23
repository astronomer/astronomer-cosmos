from __future__ import annotations

import inspect

from cosmos.config import ProfileConfig
from cosmos.operators._async.base import DbtRunBaseAirflowAsyncOperator
from cosmos.operators.base import AbstractDbtBaseOperator
from cosmos.operators.local import (
    DbtBuildLocalOperator,
    DbtCloneLocalOperator,
    DbtCompileLocalOperator,
    DbtLocalBaseOperator,
    DbtLSLocalOperator,
    DbtRunOperationLocalOperator,
    DbtSeedLocalOperator,
    DbtSnapshotLocalOperator,
    DbtSourceLocalOperator,
    DbtTestLocalOperator,
)

_SUPPORTED_DATABASES = ["bigquery"]

from abc import ABCMeta

from airflow.models.baseoperator import BaseOperator


class DbtBaseAirflowAsyncOperator(BaseOperator, metaclass=ABCMeta):
    def __init__(self, **kwargs) -> None:  # type: ignore
        kwargs.pop("async_args")
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


class DbtRunAirflowAsyncOperator(DbtRunBaseAirflowAsyncOperator):  # type: ignore

    # template_fields: Sequence[str] = (
    #     "full_refresh",
    #     "project_dir",
    #     "gcp_project",
    #     "dataset",
    #     "location",
    # )

    def __init__(  # type: ignore
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        # location: str = "us",  # This is a mandatory parameter when using BigQueryInsertJobOperator with deferrable=True
        # full_refresh: bool = False,
        extra_context: dict[str, object] | None = None,
        # configuration: dict[str, object] | None = None,
        **kwargs,
    ) -> None:
        # dbt task param
        # self.project_dir = project_dir
        # self.extra_context = extra_context or {}
        # self.full_refresh = full_refresh
        # self.profile_config = profile_config
        # if not self.profile_config or not self.profile_config.profile_mapping:
        #     raise CosmosValueError(f"Cosmos async support is only available when using ProfileMapping")
        #
        # self.profile_type: str = profile_config.get_profile_type()  # type: ignore
        # if self.profile_type not in _SUPPORTED_DATABASES:
        #     raise CosmosValueError(f"Async run are only supported: {_SUPPORTED_DATABASES}")
        #
        # # airflow task param

        # Cosmos attempts to pass many kwargs that BigQueryInsertJobOperator simply does not accept.
        # We need to pop them.
        clean_kwargs = {}
        non_async_args = set(inspect.signature(AbstractDbtBaseOperator.__init__).parameters.keys())
        non_async_args |= set(inspect.signature(DbtLocalBaseOperator.__init__).parameters.keys())
        non_async_args -= {"task_id"}

        for arg_key, arg_value in kwargs.items():
            if arg_key not in non_async_args:
                clean_kwargs[arg_key] = arg_value

        # The following are the minimum required parameters to run BigQueryInsertJobOperator using the deferrable mode
        super().__init__(
            # gcp_conn_id=self.gcp_conn_id,
            # configuration=self.configuration,
            # location=self.location,
            # deferrable=True,
            project_dir=project_dir,
            profile_config=profile_config,
            extra_context=extra_context,
            **clean_kwargs,
            # **kwargs
        )


class DbtTestAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtTestLocalOperator):  # type: ignore
    pass


class DbtRunOperationAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtRunOperationLocalOperator):  # type: ignore
    pass


class DbtCompileAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCompileLocalOperator):  # type: ignore
    pass


class DbtCloneAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCloneLocalOperator):
    pass

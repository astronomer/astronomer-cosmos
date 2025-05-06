from __future__ import annotations

import inspect
from typing import Any

from cosmos.config import ProfileConfig
from cosmos.constants import BIGQUERY_PROFILE_TYPE
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator
from cosmos.operators.base import AbstractDbtBase
from cosmos.operators.local import (
    AbstractDbtLocalBase,
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

_SUPPORTED_DATABASES = [BIGQUERY_PROFILE_TYPE]


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


class DbtRunAirflowAsyncOperator(DbtRunAirflowAsyncFactoryOperator):

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> None:

        # Cosmos attempts to pass many kwargs that async operator simply does not accept.
        # We need to pop them.
        clean_kwargs = {}
        non_async_args = set(inspect.signature(AbstractDbtBase.__init__).parameters.keys())
        non_async_args |= set(inspect.signature(DbtLocalBaseOperator.__init__).parameters.keys())
        non_async_args |= set(inspect.signature(AbstractDbtLocalBase.__init__).parameters.keys())

        dbt_kwargs = {}

        # Extract full_refresh from kwargs if present
        dbt_kwargs["full_refresh"] = kwargs.pop("full_refresh", False)

        for arg_key, arg_value in kwargs.items():
            if arg_key == "task_id":
                clean_kwargs[arg_key] = arg_value
                dbt_kwargs[arg_key] = arg_value
            elif arg_key not in non_async_args:
                clean_kwargs[arg_key] = arg_value
            else:
                dbt_kwargs[arg_key] = arg_value

        super().__init__(
            project_dir=project_dir,
            profile_config=profile_config,
            extra_context=extra_context,
            dbt_kwargs=dbt_kwargs,
            **clean_kwargs,
        )


class DbtTestAirflowAsyncOperator(DbtTestLocalOperator):
    pass


class DbtRunOperationAirflowAsyncOperator(DbtRunOperationLocalOperator):
    pass


class DbtCompileAirflowAsyncOperator(DbtCompileLocalOperator):
    pass


class DbtCloneAirflowAsyncOperator(DbtCloneLocalOperator):
    pass

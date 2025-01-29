from __future__ import annotations

import inspect

from cosmos.config import ProfileConfig
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator
from cosmos.operators.base import AbstractDbtBase
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
        if "location" in kwargs:
            kwargs.pop("location")
        super().__init__(**kwargs)


class DbtBuildAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtBuildLocalOperator):  # type: ignore
    pass


class DbtLSAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtLSLocalOperator):  # type: ignore
    pass


class DbtSeedAirflowAsyncOperator(DbtSeedLocalOperator):  # type: ignore
    def __init__(self, *args, **kwargs) -> None:
        clean_kwargs = {}
        base_operator_args = set(inspect.signature(BaseOperator.__init__).parameters.keys())
        for arg_key, arg_value in kwargs.items():
            if arg_key in base_operator_args:
                clean_kwargs[arg_key] = arg_value
        BaseOperator.__init__(self, **clean_kwargs)
        super().__init__(*args, **kwargs)


class DbtSnapshotAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtSnapshotLocalOperator):  # type: ignore
    pass


class DbtSourceAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtSourceLocalOperator):  # type: ignore
    pass


class DbtRunAirflowAsyncOperator(DbtRunAirflowAsyncFactoryOperator):  # type: ignore

    def __init__(  # type: ignore
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, object] | None = None,
        **kwargs,
    ) -> None:

        # Cosmos attempts to pass many kwargs that async operator simply does not accept.
        # We need to pop them.
        clean_kwargs = {}
        non_async_args = set(inspect.signature(AbstractDbtBase.__init__).parameters.keys())
        non_async_args |= set(inspect.signature(DbtLocalBaseOperator.__init__).parameters.keys())
        # non_async_args -= {"task_id"}

        dbt_kwargs = {}

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


class DbtTestAirflowAsyncOperator(DbtTestLocalOperator):  # type: ignore
    def __init__(self, *args, **kwargs) -> None:
        clean_kwargs = {}
        base_operator_args = set(inspect.signature(BaseOperator.__init__).parameters.keys())
        for arg_key, arg_value in kwargs.items():
            if arg_key in base_operator_args:
                clean_kwargs[arg_key] = arg_value
        super().__init__(*args, **kwargs)
        BaseOperator.__init__(self, **clean_kwargs)


class DbtRunOperationAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtRunOperationLocalOperator):  # type: ignore
    pass


class DbtCompileAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCompileLocalOperator):  # type: ignore
    pass


class DbtCloneAirflowAsyncOperator(DbtBaseAirflowAsyncOperator, DbtCloneLocalOperator):
    pass

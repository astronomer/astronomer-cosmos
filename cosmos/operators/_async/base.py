from abc import ABCMeta
from typing import Any

from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.operators._async.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators.local import DbtRunLocalOperator


class DbtRunAirflowAsyncFactoryOperator(DbtRunLocalOperator, metaclass=ABCMeta):  # type: ignore[misc]
    def __init__(self, project_dir: str, profile_config: ProfileConfig, **kwargs: Any):
        self.project_dir = project_dir
        self.profile_config = profile_config
        self.async_args = None
        if "async_args" in kwargs:
            self.async_args = kwargs.pop("async_args")

        async_operator_class = self.create_async_operator()

        if async_operator_class == DbtRunAirflowAsyncBigqueryOperator:
            DbtRunAirflowAsyncFactoryOperator.__bases__ = (DbtRunAirflowAsyncBigqueryOperator,)
            super().__init__(
                project_dir=project_dir, profile_config=profile_config, async_args=self.async_args, **kwargs
            )

    def create_async_operator(self) -> Any:
        async_class_map = {"bigquery": DbtRunAirflowAsyncBigqueryOperator}

        profile_type = self.profile_config.get_profile_type()

        if profile_type not in async_class_map:
            raise Exception(f"Async operator not supported for profile {profile_type}")

        return async_class_map.get(profile_type)

    def execute(self, context: Context) -> None:
        super().execute(context)

from abc import ABCMeta
from typing import Any, Sequence

from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.operators._async.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators.local import DbtRunLocalOperator

# Register async operator here
ASYNC_CLASS_MAP = {"bigquery": DbtRunAirflowAsyncBigqueryOperator}


class DbtRunAirflowAsyncFactoryOperator(DbtRunLocalOperator, metaclass=ABCMeta):  # type: ignore[misc]

    template_fields: Sequence[str] = ("project_dir",)

    def __init__(self, project_dir: str, profile_config: ProfileConfig, **kwargs: Any):
        self.project_dir = project_dir
        self.profile_config = profile_config
        self.async_args = None
        if "async_args" in kwargs:
            self.async_args = kwargs.pop("async_args")

        async_operator_class = self.create_async_operator()

        DbtRunAirflowAsyncFactoryOperator.__bases__ = (async_operator_class,)
        super().__init__(project_dir=project_dir, profile_config=profile_config, async_args=self.async_args, **kwargs)

    def create_async_operator(self) -> Any:

        profile_type = self.profile_config.get_profile_type()

        if profile_type not in ASYNC_CLASS_MAP:
            raise Exception(f"Async operator not supported for profile {profile_type}")

        return ASYNC_CLASS_MAP[profile_type]

    def execute(self, context: Context) -> None:
        super().execute(context)

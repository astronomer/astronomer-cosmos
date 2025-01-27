import importlib
import logging
from abc import ABCMeta
from typing import Any, Sequence

from airflow.utils.context import Context

from cosmos.airflow.graph import _snake_case_to_camelcase
from cosmos.config import ProfileConfig
from cosmos.constants import ExecutionMode
from cosmos.operators.local import DbtRunLocalOperator

log = logging.getLogger(__name__)


def _create_async_operator_class(profile_type: str, dbt_class: str) -> Any:
    """
    Dynamically constructs and returns an asynchronous operator class for the given profile type and dbt class name.

    The function constructs a class path string for an asynchronous operator, based on the provided `profile_type` and
    `dbt_class`. It attempts to import the corresponding class dynamically and return it. If the class cannot be found,
    it falls back to returning the `DbtRunLocalOperator` class.

    :param profile_type: The dbt profile type
    :param dbt_class: The dbt class name. Example DbtRun, DbtTest.
    """
    execution_mode = ExecutionMode.AIRFLOW_ASYNC.value
    class_path = f"cosmos.operators._asynchronous.{profile_type}.{dbt_class}{_snake_case_to_camelcase(execution_mode)}{profile_type.capitalize()}Operator"
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        operator_class = getattr(module, class_name)
        return operator_class
    except (ModuleNotFoundError, AttributeError):
        log.info("Error in loading class: %s. falling back to DbtRunLocalOperator", class_path)
        return DbtRunLocalOperator


class DbtRunAirflowAsyncFactoryOperator(DbtRunLocalOperator, metaclass=ABCMeta):  # type: ignore[misc]

    template_fields: Sequence[str] = DbtRunLocalOperator.template_fields + ("project_dir",)  # type: ignore[operator]

    def __init__(self, project_dir: str, profile_config: ProfileConfig, **kwargs: Any):
        self.project_dir = project_dir
        self.profile_config = profile_config

        async_operator_class = self.create_async_operator()

        # Dynamically modify the base classes.
        # This is necessary because the async operator class is only known at runtime.
        # When using composition instead of inheritance to initialize the async class and run its execute method,
        # Airflow throws a `DuplicateTaskIdFound` error.
        DbtRunAirflowAsyncFactoryOperator.__bases__ = (async_operator_class,)
        super().__init__(project_dir=project_dir, profile_config=profile_config, **kwargs)

    def create_async_operator(self) -> Any:

        profile_type = self.profile_config.get_profile_type()

        async_class_operator = _create_async_operator_class(profile_type, "DbtRun")

        return async_class_operator

    def execute(self, context: Context) -> None:
        super().execute(context)

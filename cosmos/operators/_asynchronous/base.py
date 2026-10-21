from __future__ import annotations

import types
from functools import lru_cache
from typing import Any

from cosmos._utils.importer import load_method_from_module
from cosmos.airflow.graph import _snake_case_to_camelcase
from cosmos.config import ProfileConfig
from cosmos.constants import ExecutionMode

try:  # Airflow 3
    from airflow.sdk.bases.operator import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator


def _create_async_operator_class(profile_type: str, dbt_class: str) -> Any:
    """
    Dynamically constructs and returns an asynchronous operator class for the given profile type and dbt class name.

    The function constructs a class path string for an asynchronous operator, based on the provided `profile_type` and
    `dbt_class`. It attempts to import the corresponding class dynamically and return it. If the class cannot be found,
    it raises an error.

    :param profile_type: The dbt profile type
    :param dbt_class: The dbt class name. Example DbtRun, DbtTest.
    """
    execution_mode = ExecutionMode.AIRFLOW_ASYNC.value
    class_path = f"cosmos.operators._asynchronous.{profile_type}.{dbt_class}{_snake_case_to_camelcase(execution_mode)}{profile_type.capitalize()}Operator"
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        return load_method_from_module(module_path, class_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"Error in loading class: {class_path}. Unable to find the specified operator class.") from e


@lru_cache(maxsize=None)
def _get_async_subclass(factory_cls: type, async_operator_class: type) -> type:
    """
    Mix ``factory_cls`` with the runtime-resolved async operator class without re-basing the shared class.

    Assigning ``__bases__`` raises ``TypeError`` when the instance layouts differ, as with Airflow 3.4's ABC-based
    ``ResumableJobMixin``. Composition instead raises ``DuplicateTaskIdFound``.
    """
    return types.new_class(
        factory_cls.__name__,
        (factory_cls, async_operator_class),
        exec_body=lambda ns: ns.update(__module__=factory_cls.__module__),
    )


class DbtRunAirflowAsyncFactoryOperator(BaseOperator):
    """Resolves the profile-specific async operator at construction and mixes it in via a dynamic subclass."""

    def __init__(
        self,
        project_dir: str,
        profile_config: ProfileConfig,
        extra_context: dict[str, object] | None = None,
        dbt_kwargs: dict[str, object] | None = None,
        **kwargs: Any,
    ) -> None:
        self.project_dir = project_dir
        self.profile_config = profile_config

        async_operator_class = _create_async_operator_class(profile_config.get_profile_type(), "DbtRun")
        # Safe because the dynamic class is a subclass of ``type(self)``, so the instance layout is unchanged.
        self.__class__ = _get_async_subclass(type(self), async_operator_class)
        super().__init__(
            project_dir=project_dir,
            profile_config=profile_config,
            extra_context=extra_context,
            dbt_kwargs=dbt_kwargs,
            **kwargs,
        )

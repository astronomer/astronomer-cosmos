"""
This module contains a function to render a dbt project as an Airflow Task Group.
"""

from __future__ import annotations

from typing import Any

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos import settings
from cosmos.config import ExecutionConfig
from cosmos.constants import ExecutionMode
from cosmos.converter import DbtToAirflowConverter, airflow_kwargs, specific_kwargs


class DbtTaskGroup(TaskGroup, DbtToAirflowConverter):
    """
    Render a dbt project as an Airflow Task Group.
    """

    def __init__(
        self,
        group_id: str = "dbt_task_group",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._execution_config = kwargs.get("execution_config")
        kwargs["group_id"] = group_id
        TaskGroup.__init__(self, *args, **airflow_kwargs(**kwargs))
        kwargs["task_group"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))

    @property
    def is_watcher_mode(self) -> bool:
        """Whether this task group uses a watcher execution mode."""
        return isinstance(self._execution_config, ExecutionConfig) and self._execution_config.execution_mode in (
            ExecutionMode.WATCHER,
            ExecutionMode.WATCHER_KUBERNETES,
        )

    def _set_watcher_downstream_tasks_trigger_rule(self, downstream_tasks: Any) -> None:
        """In watcher mode the producer task may be skipped on retry.

        Set ``trigger_rule="none_failed_min_one_success"`` on downstream tasks so the skip
        does not propagate outside the task group.
        """
        if not self.is_watcher_mode or not settings.propagate_watcher_trigger_rule:
            return
        items = downstream_tasks if isinstance(downstream_tasks, (list, tuple)) else [downstream_tasks]
        for item in items:
            if isinstance(item, TaskGroup):
                # For downstream TaskGroups, only set trigger_rule on root tasks
                # (tasks with no upstream within the group) to avoid altering
                # the group's internal dependency semantics.
                for root_task in item.get_roots():
                    if hasattr(root_task, "trigger_rule"):
                        root_task.trigger_rule = "none_failed_min_one_success"  # type: ignore[assignment]
            elif hasattr(item, "trigger_rule"):
                item.trigger_rule = "none_failed_min_one_success"  # type: ignore[assignment]

    def __rshift__(self, other: Any) -> Any:
        # dbt_group >> post_dbt — post_dbt is downstream of dbt_group
        result = super().__rshift__(other)
        self._set_watcher_downstream_tasks_trigger_rule(other)
        return result

    def __rlshift__(self, other: Any) -> Any:
        # other << dbt_group — other is downstream of dbt_group
        result = super().__rlshift__(other)
        self._set_watcher_downstream_tasks_trigger_rule(other)
        return result

    def set_downstream(self, task_or_task_list: Any, edge_modifier: Any = None) -> None:  # type: ignore[override]
        super().set_downstream(task_or_task_list, edge_modifier=edge_modifier)
        self._set_watcher_downstream_tasks_trigger_rule(task_or_task_list)

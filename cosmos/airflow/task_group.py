"""
This module contains a function to render a dbt project as an Airflow Task Group.
"""

from __future__ import annotations

from typing import Any

from airflow.utils.task_group import TaskGroup

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
        kwargs["group_id"] = group_id
        TaskGroup.__init__(self, *args, **airflow_kwargs(**kwargs))
        kwargs["task_group"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))

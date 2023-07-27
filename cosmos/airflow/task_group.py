"""
This module contains a function to render a dbt project as an Airflow Task Group.
"""
from __future__ import annotations
from typing import Any

from airflow.utils.task_group import TaskGroup

from cosmos.converter import airflow_kwargs, specific_kwargs, DbtToAirflowConverter


class DbtTaskGroup(TaskGroup, DbtToAirflowConverter):  # type: ignore[misc] # ignores subclass MyPy error
    """
    Render a dbt project as an Airflow Task Group.
    """

    def __init__(
        self,
        group_id: str = "dbt_task_group",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        group_id = group_id
        TaskGroup.__init__(self, group_id=group_id, *args, **airflow_kwargs(**kwargs))
        kwargs["task_group"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))

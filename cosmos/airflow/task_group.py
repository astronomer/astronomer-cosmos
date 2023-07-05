"""
This module contains a function to render a dbt project as an Airflow Task Group.
"""
from __future__ import annotations
from typing import Any

from airflow.utils.task_group import TaskGroup

from cosmos.airflow.group import airflow_kwargs, specific_kwargs, AirflowGroup


class DbtTaskGroup(TaskGroup, AirflowGroup):
    """
    Render a dbt project as an Airflow Task Group.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        group_id = kwargs.get("group_id", kwargs.get("dbt_project_name", "dbt_task_group"))
        TaskGroup.__init__(self, group_id, *args, **airflow_kwargs(**kwargs))
        AirflowGroup.__init__(self, *args, task_group=self, **specific_kwargs(**kwargs))

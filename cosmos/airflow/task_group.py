"""
This module contains a function to render a dbt project as an Airflow Task Group.
"""
from __future__ import annotations
from typing import Any

from airflow.utils.task_group import TaskGroup

from cosmos.config import ProjectConfig
from cosmos.converter import airflow_kwargs, specific_kwargs, DbtToAirflowConverter


class DbtTaskGroup(TaskGroup, DbtToAirflowConverter):  # type: ignore[misc] # ignores subclass MyPy error
    """
    Render a dbt project as an Airflow Task Group.
    """

    def __init__(
        self,
        project_config: ProjectConfig,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        group_id = kwargs.pop("group_id", project_config.project_name)

        kwargs = {
            **kwargs,
            "group_id": group_id,
            "project_config": project_config,
        }

        TaskGroup.__init__(self, *args, **airflow_kwargs(**kwargs))
        DbtToAirflowConverter.__init__(self, *args, task_group=self, **specific_kwargs(**kwargs))

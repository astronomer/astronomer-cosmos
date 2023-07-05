"""
This module contains a function to render a dbt project as an Airflow DAG.
"""
from __future__ import annotations

from typing import Any

from airflow.models.dag import DAG

from cosmos.airflow.group import airflow_kwargs, specific_kwargs, AirflowGroup


class DbtDag(DAG, AirflowGroup):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        DAG.__init__(self, *args, **airflow_kwargs(**kwargs))
        AirflowGroup.__init__(self, *args, dag=self, **specific_kwargs(**kwargs))

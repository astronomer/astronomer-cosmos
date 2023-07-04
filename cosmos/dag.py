"""
This module contains a function to render a dbt project as an Airflow DAG.
"""
from __future__ import annotations

from typing import Any

from airflow.models.dag import DAG

from cosmos.airflow import airflow_kwargs, specific_kwargs, Group


class DbtDag(DAG, Group):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        dag = DAG.__init__(self, *args, **airflow_kwargs(**kwargs))
        Group.__init__(self, *args, dag=dag, **specific_kwargs(**kwargs))

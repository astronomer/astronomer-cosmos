"""
This module contains a function to render a dbt project as an Airflow DAG.
"""
from __future__ import annotations

from typing import Any

from airflow.models.dag import DAG

from cosmos.converter import airflow_kwargs, specific_kwargs, DbtToAirflowConverter


class DbtDag(DAG, DbtToAirflowConverter):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        DAG.__init__(self, *args, **airflow_kwargs(**kwargs))
        DbtToAirflowConverter.__init__(self, *args, dag=self, **specific_kwargs(**kwargs))

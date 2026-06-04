"""
This module contains a function to render a dbt project as an Airflow DAG.
"""

from __future__ import annotations

from typing import Any

from airflow.models.dag import DAG

from cosmos.converter import DbtToAirflowConverter, airflow_kwargs, specific_kwargs


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
        kwargs["dag"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))

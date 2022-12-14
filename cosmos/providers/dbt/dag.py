"""
This module contains a function to render a dbt project as an Airflow DAG.
"""
from typing import Any, Literal
from cosmos.core.airflow import CosmosDag
from cosmos.providers.dbt.parser.project import DbtProject
from .render import render_project

from airflow.models import DAG
from airflow.utils.decorators import apply_defaults


class DbtDag(CosmosDag):
    """
    Render a dbt project as an Airflow DAG. Overrides the Airflow DAG model to allow
    for additional configs to be passed.

    :param dbt_project_name: The name of the dbt project
    :param dbt_root_path: The path to the dbt root directory
    :param conn_id: The Airflow connection ID to use for the dbt profile
    :param dbt_args: Parameters to pass to the underlying dbt operators
    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
    :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
        Defaults to "after_each"
    :param dbt_tags: A list of dbt tags to filter the dbt models by
    """

    def __init__(
        self,
        dbt_project_name: str,
        conn_id: str,
        dbt_args: dict[str, Any] = {},
        emit_datasets: bool = True,
        dbt_root_path: str = "/usr/local/airflow/dbt",
        test_behavior: Literal["none", "after_each", "after_all"] = "after_each",
        dbt_tags: list[str] = [],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # add additional args to the dbt_args
        dbt_args = {
            **dbt_args,
            "conn_id": conn_id,
        }

        # get the group of the dbt project
        group = render_project(
            dbt_project_name=dbt_project_name,
            dbt_root_path=dbt_root_path,
            task_args=dbt_args,
            test_behavior=test_behavior,
            emit_datasets=emit_datasets,
            conn_id=conn_id,
            dbt_tags=dbt_tags,
        )

        # call the airflow DAG constructor
        super().__init__(group, *args, **kwargs)

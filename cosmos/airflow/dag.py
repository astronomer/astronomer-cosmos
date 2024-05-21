"""
This module contains a function to render a dbt project as an Airflow DAG.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from airflow.models.dag import DAG

from cosmos import cache, settings
from cosmos.converter import DbtToAirflowConverter, airflow_kwargs, specific_kwargs
from cosmos.log import get_logger

logger = get_logger()


class DbtDag(DAG, DbtToAirflowConverter):
    """
    Render a dbt project as an Airflow DAG.
    """

    @staticmethod
    def get_cache_filepath(cache_identifier: str) -> Path:
        cache_dir_path = cache._obtain_cache_dir_path(cache_identifier)
        return cache_dir_path / f"{cache_identifier}.pkl"

    @staticmethod
    def should_use_cache() -> bool:
        return True

    def __new__(cls, *args, **kwargs):  # type: ignore
        dag_id = kwargs.get("dag_id")
        if dag_id is not None:
            cache_filepath = DbtDag.get_cache_filepath(dag_id)
            if settings.enable_cache and settings.experimental_cache and cache_filepath.exists():
                logger.info(f"Restoring DbtDag {dag_id} from cache {cache_filepath}")
                with open(cache_filepath, "rb") as fp:
                    return pickle.load(fp)

        instance = DAG.__new__(DAG)
        DbtDag.__init__(instance, *args, **kwargs)  # type: ignore
        return instance

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        logger.info(f"Creating DbtDag {kwargs.get('dag_id')}")
        DAG.__init__(self, *args, **airflow_kwargs(**kwargs))
        kwargs["dag"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))
        cache_filepath = DbtDag.get_cache_filepath(kwargs["dag_id"])
        if settings.enable_cache and settings.experimental_cache:
            with open(cache_filepath, "wb") as fp:
                pickle.dump(self, fp)

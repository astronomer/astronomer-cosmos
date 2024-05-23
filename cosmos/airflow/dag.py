"""
This module contains a function to render a dbt project as an Airflow DAG.
"""

from __future__ import annotations

# import inspect
import pickle
import time
from typing import Any

from airflow.models.dag import DAG

from cosmos import cache
from cosmos.converter import DbtToAirflowConverter, airflow_kwargs, specific_kwargs
from cosmos.log import get_logger

logger = get_logger()


class DbtDag(DAG, DbtToAirflowConverter):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __new__(cls, *args, **kwargs):  # type: ignore
        dag_id = kwargs.get("dag_id")
        project_config = kwargs.get("project_config")

        # When we load a Pickle dump of an instance, __new__ is invoked without kwargs
        # In those cases, we should not call __new__ again, otherwise we'll have an infinite recursion
        if dag_id is not None and project_config and project_config.dbt_project_path:
            cache_id = cache.create_cache_identifier_v2(dag_id, None)
            current_version = cache.calculate_current_version(cache_id, project_config.dbt_project_path)
            cache_filepath = cache.should_use_cache() and cache.is_project_unmodified(cache_id, current_version)
            if cache_filepath:
                logger.info(f"Restoring {cls.__name__} {dag_id} from cache {cache_filepath}")
                with open(cache_filepath, "rb") as fp:
                    start_time = time.process_time()
                    dbt_dag = pickle.load(fp)
                    elapsed_time = time.process_time() - start_time
                    logger.info(
                        f"It took {elapsed_time:.3}s to restore the cached version of the {cls.__name__} {dag_id}"
                    )
                    return dbt_dag

        instance = DAG.__new__(DAG)
        cls.__init__(instance, *args, **kwargs)  # type: ignore
        return instance

    # The __init__ is not called when restoring the cached in __new__
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        start_time = time.process_time()
        dag_id = kwargs["dag_id"]
        project_config = kwargs.get("project_config")

        DAG.__init__(self, *args, **airflow_kwargs(**kwargs))
        kwargs["dag"] = self
        DbtToAirflowConverter.__init__(self, *args, **specific_kwargs(**kwargs))

        elapsed_time = time.process_time() - start_time
        logger.info(f"It took {elapsed_time} to create the {self.__class__.__name__} {dag_id} from scratch")

        if cache.should_use_cache() and project_config:
            cache_id = cache.create_cache_identifier_v2(dag_id, None)
            cache_filepath = cache.get_cache_filepath(cache_id)
            with open(cache_filepath, "wb") as fp:
                pickle.dump(self, fp)
            cache_version_filepath = cache.get_cache_version_filepath(cache_id)
            current_version = cache.calculate_current_version(cache_id, project_config.dbt_project_path)
            cache_version_filepath.write_text(current_version)
            logger.info(f"Stored {self.__class__.__name__} {dag_id} cache {cache_filepath}")

"""
This module contains a function to render a dbt project as an Airflow DAG.
"""

from __future__ import annotations

import functools

# import inspect
import pickle
import time
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
    @functools.lru_cache
    def get_cache_filepath(cache_identifier: str) -> Path:
        cache_dir_path = cache._obtain_cache_dir_path(cache_identifier)
        return cache_dir_path / f"{cache_identifier}.pkl"

    @staticmethod
    @functools.lru_cache
    def get_cache_version_filepath(cache_identifier: str) -> Path:
        return Path(str(DbtDag.get_cache_filepath(cache_identifier)) + ".version")

    @staticmethod
    @functools.lru_cache
    def should_use_cache() -> bool:
        return settings.enable_cache and settings.experimental_cache

    @staticmethod
    @functools.lru_cache
    def is_project_unmodified(dag_id: str, current_version: str) -> Path | None:
        cache_filepath = DbtDag.get_cache_filepath(dag_id)
        cache_version_filepath = DbtDag.get_cache_version_filepath(dag_id)
        if cache_version_filepath.exists() and cache_filepath.exists():
            previous_cache_version = cache_version_filepath.read_text()
            if previous_cache_version == current_version:
                return cache_filepath
        return None

    @staticmethod
    @functools.lru_cache
    def calculate_current_version(dag_id: str, project_dir: Path) -> str:
        start_time = time.process_time()

        # When DAG file was last changed - this is very slow (e.g. 0.6s)
        # caller_dag_frame = inspect.stack()[1]
        # caller_dag_filepath = Path(caller_dag_frame.filename)
        # logger.info("The %s DAG is located in: %s" % (dag_id, caller_dag_filepath))
        # dag_last_modified = caller_dag_filepath.stat().st_mtime
        # mid_time = time.process_time() - start_time
        # logger.info(f"It took {mid_time:.3}s to calculate the first part of the version")
        dag_last_modified = None

        # Combined value for when the dbt project directory files were last modified
        # This is fast (e.g. 0.01s for jaffle shop, 0.135s for a 5k models dbt folder)
        dbt_combined_last_modified = sum([path.stat().st_mtime for path in project_dir.glob("**/*")])

        elapsed_time = time.process_time() - start_time
        logger.info(f"It took {elapsed_time:.3}s to calculate the cache version for the DbtDag {dag_id}")
        return f"{dag_last_modified} {dbt_combined_last_modified}"

    def __new__(cls, *args, **kwargs):  # type: ignore
        dag_id = kwargs.get("dag_id")
        project_config = kwargs.get("project_config")

        # When we load a Pickle dump of a DbtDag, __new__ is invoked without kwargs
        # In those cases, we should not call DbtDag.__new__ again, otherwise we'll have an infinite recursion
        if dag_id is not None and project_config and project_config.dbt_project_path:
            current_version = DbtDag.calculate_current_version(dag_id, project_config.dbt_project_path)
            cache_filepath = DbtDag.should_use_cache() and DbtDag.is_project_unmodified(dag_id, current_version)
            if cache_filepath:
                logger.info(f"Restoring DbtDag {dag_id} from cache {cache_filepath}")
                with open(cache_filepath, "rb") as fp:
                    start_time = time.process_time()
                    dbt_dag = pickle.load(fp)
                    elapsed_time = time.process_time() - start_time
                    logger.info(f"It took {elapsed_time:.3}s to restore the cached version of the DbtDag {dag_id}")
                    return dbt_dag

        instance = DAG.__new__(DAG)
        DbtDag.__init__(instance, *args, **kwargs)  # type: ignore
        return instance

    # The __init__ is not called when restoring the cached DbtDag in __new__
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
        logger.info(f"It took {elapsed_time} to create the DbtDag {dag_id} from scratch")

        if DbtDag.should_use_cache() and project_config:
            cache_filepath = DbtDag.get_cache_filepath(dag_id)
            with open(cache_filepath, "wb") as fp:
                pickle.dump(self, fp)
            cache_version_filepath = DbtDag.get_cache_version_filepath(dag_id)
            current_version = DbtDag.calculate_current_version(dag_id, project_config.dbt_project_path)
            cache_version_filepath.write_text(current_version)
            logger.info(f"Stored DbtDag {dag_id} cache {cache_filepath}")

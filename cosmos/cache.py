from __future__ import annotations

import shutil
from pathlib import Path

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos import settings
from cosmos.constants import DBT_MANIFEST_FILE_NAME, DBT_TARGET_DIR_NAME
from cosmos.dbt.project import get_partial_parse_path


# It was considered to create a cache identifier based on the dbt project path, as opposed
# to where it is used in Airflow. However, we could have concurrency issues if the same
# dbt cached directory was being used by different dbt task groups or DAGs within the same
# node. For this reason, as a starting point, the cache is identified by where it is used.
# This can be reviewed in the future.
def _create_cache_identifier(dag: DAG, task_group: TaskGroup | None) -> str:
    """
    Given a DAG name and a (optional) task_group_name, create the identifier for caching.

    :param dag_name: Name of the Cosmos DbtDag being cached
    :param task_group_name: (optional) Name of the Cosmos DbtTaskGroup being cached
    :return: Unique identifier representing the cache
    """
    if task_group:
        if task_group.dag_id is not None:
            cache_identifiers_list = [task_group.dag_id]
        if task_group.group_id is not None:
            cache_identifiers_list.extend([task_group.group_id.replace(".", "__")])
        cache_identifier = "__".join(cache_identifiers_list)
    else:
        cache_identifier = dag.dag_id

    return cache_identifier


def _obtain_cache_dir_path(cache_identifier: str, base_dir: Path = settings.cache_dir) -> Path:
    """
    Return a directory used to cache a specific Cosmos DbtDag or DbtTaskGroup. If the directory
    does not exist, create it.

    :param cache_identifier: Unique key used as a cache identifier
    :param base_dir: Root directory where cache will be stored
    :return: Path to directory used to cache this specific Cosmos DbtDag or DbtTaskGroup
    """
    cache_dir_path = base_dir / cache_identifier
    tmp_target_dir = cache_dir_path / DBT_TARGET_DIR_NAME
    tmp_target_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir_path


def _get_timestamp(path: Path) -> float:
    """
    Return the timestamp of a path or 0, if it does not exist.

    :param path: Path to the file or directory of interest
    :return: File or directory timestamp
    """
    try:
        timestamp = path.stat().st_mtime
    except FileNotFoundError:
        timestamp = 0
    return timestamp


def _get_latest_partial_parse(dbt_project_path: Path, cache_dir: Path) -> Path | None:
    """
    Return the path to the latest partial parse file, if defined.

    :param dbt_project_path: Original dbt project path
    :param cache_dir: Path to the Cosmos project cache directory
    :return: Either return the Path to the latest partial parse file, or None.
    """
    project_partial_parse_path = get_partial_parse_path(dbt_project_path)
    cosmos_cached_partial_parse_filepath = get_partial_parse_path(cache_dir)

    age_project_partial_parse = _get_timestamp(project_partial_parse_path)
    age_cosmos_cached_partial_parse_filepath = _get_timestamp(cosmos_cached_partial_parse_filepath)

    if age_project_partial_parse and age_cosmos_cached_partial_parse_filepath:
        if age_project_partial_parse > age_cosmos_cached_partial_parse_filepath:
            return project_partial_parse_path
        else:
            return cosmos_cached_partial_parse_filepath
    elif age_project_partial_parse:
        return project_partial_parse_path
    elif age_cosmos_cached_partial_parse_filepath:
        return cosmos_cached_partial_parse_filepath

    return None


def _update_partial_parse_cache(latest_partial_parse_filepath: Path, cache_dir: Path) -> None:
    """
    Update the cache to have the latest partial parse file contents.

    :param latest_partial_parse_filepath: Path to the most up-to-date partial parse file
    :param cache_dir: Path to the Cosmos project cache directory
    """
    cache_path = get_partial_parse_path(cache_dir)
    manifest_path = get_partial_parse_path(cache_dir).parent / DBT_MANIFEST_FILE_NAME
    latest_manifest_filepath = latest_partial_parse_filepath.parent / DBT_MANIFEST_FILE_NAME

    shutil.copy(str(latest_partial_parse_filepath), str(cache_path))
    shutil.copy(str(latest_manifest_filepath), str(manifest_path))


def _copy_partial_parse_to_project(partial_parse_filepath: Path, project_path: Path) -> None:
    """
    Update target dbt project directory to have the latest partial parse file contents.

    :param partial_parse_filepath: Path to the most up-to-date partial parse file
    :param project_path: Path to the target dbt project directory
    """
    target_partial_parse_file = get_partial_parse_path(project_path)
    tmp_target_dir = project_path / DBT_TARGET_DIR_NAME
    tmp_target_dir.mkdir(exist_ok=True)

    source_manifest_filepath = partial_parse_filepath.parent / DBT_MANIFEST_FILE_NAME
    target_manifest_filepath = target_partial_parse_file.parent / DBT_MANIFEST_FILE_NAME
    shutil.copy(str(partial_parse_filepath), str(target_partial_parse_file))
    shutil.copy(str(source_manifest_filepath), str(target_manifest_filepath))

from __future__ import annotations

import functools
import hashlib
import json
import os
import shutil
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import msgpack
import yaml
from airflow.models import DagRun, Variable
from airflow.models.dag import DAG
from airflow.utils.session import provide_session
from airflow.utils.task_group import TaskGroup
from airflow.version import version as airflow_version
from sqlalchemy import select
from sqlalchemy.orm import Session

from cosmos import settings
from cosmos.constants import (
    DBT_MANIFEST_FILE_NAME,
    DBT_TARGET_DIR_NAME,
    DEFAULT_PROFILES_FILE_NAME,
    FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP,
    PACKAGE_LOCKFILE_YML,
)
from cosmos.dbt.project import get_partial_parse_path
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger
from cosmos.settings import (
    AIRFLOW_IO_AVAILABLE,
    cache_dir,
    dbt_profile_cache_dir_name,
    enable_cache,
    enable_cache_package_lockfile,
    enable_cache_profile,
    remote_cache_dir_conn_id,
)
from cosmos.settings import remote_cache_dir as settings_remote_cache_dir

logger = get_logger(__name__)
VAR_KEY_CACHE_PREFIX = "cosmos_cache__"


def _configure_remote_cache_dir() -> Path | None:
    """Configure the remote cache dir if it is provided."""
    if not settings_remote_cache_dir:
        return None

    _configured_cache_dir = None

    cache_dir_str = str(settings_remote_cache_dir)

    remote_cache_conn_id = remote_cache_dir_conn_id
    if not remote_cache_conn_id:
        cache_dir_schema = cache_dir_str.split("://")[0]
        remote_cache_conn_id = FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP.get(cache_dir_schema, None)  # type: ignore[assignment]
    if remote_cache_conn_id is None:
        return _configured_cache_dir

    if not AIRFLOW_IO_AVAILABLE:
        raise CosmosValueError(
            f"You're trying to specify remote cache_dir {cache_dir_str}, but the required "
            f"Object Storage feature is unavailable in Airflow version {airflow_version}. Please upgrade to "
            "Airflow 2.8 or later."
        )

    from airflow.io.path import ObjectStoragePath

    _configured_cache_dir = ObjectStoragePath(cache_dir_str, conn_id=remote_cache_conn_id)

    if not _configured_cache_dir.exists():  # type: ignore[no-untyped-call]
        # TODO: Check if we should raise an error instead in case the provided path does not exist.
        _configured_cache_dir.mkdir(parents=True, exist_ok=True)

        # raise CosmosValueError(
        #     f"remote_cache_path `{cache_dir_str}` does not exist or is not accessible using "
        #     f"remote_cache_conn_id `{remote_cache_conn_id}`"
        # )

    return _configured_cache_dir


def _get_airflow_metadata(dag: DAG, task_group: TaskGroup | None) -> dict[str, str | None]:
    dag_id = None
    task_group_id = None
    cosmos_type = "DbtDag"

    if task_group:
        if task_group.dag_id is not None:
            dag_id = task_group.dag_id
        if task_group.group_id is not None:
            task_group_id = task_group.group_id
            cosmos_type = "DbtTaskGroup"
    else:
        dag_id = dag.dag_id

    return {"cosmos_type": cosmos_type, "dag_id": dag_id, "task_group_id": task_group_id}


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
    metadata = _get_airflow_metadata(dag, task_group)
    cache_identifiers_list = []
    dag_id = metadata.get("dag_id")
    task_group_id = metadata.get("task_group_id")

    if dag_id:
        cache_identifiers_list.append(dag_id)
    if task_group_id:
        cache_identifiers_list.append(task_group_id.replace(".", "__"))

    return "__".join(cache_identifiers_list)


def create_cache_key(cache_identifier: str) -> str:
    return f"{VAR_KEY_CACHE_PREFIX}{cache_identifier}"


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
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = get_partial_parse_path(cache_dir).parent / DBT_MANIFEST_FILE_NAME
    latest_manifest_filepath = latest_partial_parse_filepath.parent / DBT_MANIFEST_FILE_NAME

    shutil.copyfile(str(latest_partial_parse_filepath), str(cache_path))
    shutil.copyfile(str(latest_manifest_filepath), str(manifest_path))


def patch_partial_parse_content(partial_parse_filepath: Path, project_path: Path) -> bool:
    """
    Update, if needed, the root_path references in partial_parse.msgpack to an existing project directory.
    This is necessary because an issue is observed where on specific earlier versions of dbt-core like 1.5.4 and 1.6.5,
    the commands fail to locate project files as they are pointed to a stale directory by the root_path in the partial
    parse file.

    This issue was not observed on recent versions of dbt-core 1.5.8, 1.6.6, 1.7.0 and 1.8.0 as tested on.
    It is suspected that PR dbt-labs/dbt-core#8762 is likely the fix and the fix appears to be backported to later
    version releases of 1.5.x and 1.6.x. However, the below modification is applied to ensure that the root_path is
    correctly set to the needed project directory and the feature is compatible across all dbt-core versions.

    :param partial_parse_filepath: Path to the most up-to-date partial parse file
    :param project_path: Path to the target dbt project directory
    """
    should_patch_partial_parse_content = False

    try:
        with partial_parse_filepath.open("rb") as f:
            # Issue reported: https://github.com/astronomer/astronomer-cosmos/issues/971
            # it may be due a race condition of multiple processes trying to read/write this file
            data = msgpack.unpack(f)
    except ValueError as e:
        logger.info("Unable to patch the partial_parse.msgpack file due to %s" % repr(e))
    else:
        for node in data["nodes"].values():
            expected_filepath = node.get("root_path")
            if expected_filepath is None:
                continue
            elif expected_filepath and not Path(expected_filepath).exists():
                node["root_path"] = str(project_path)
                should_patch_partial_parse_content = True
            else:
                break
        if should_patch_partial_parse_content:
            with partial_parse_filepath.open("wb") as f:
                packed = msgpack.packb(data)
                f.write(packed)
    return should_patch_partial_parse_content


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

    patch_partial_parse_content(target_partial_parse_file, project_path)

    if source_manifest_filepath.exists():
        shutil.copy(str(source_manifest_filepath), str(target_manifest_filepath))


def _create_folder_version_hash(dir_path: Path) -> str:
    """
    Given a directory, iterate through its content and create a hash that will change in case the
    contents of the directory change. The value should not change if the values of the directory do not change, even if
    the command is run from different Airflow instances.

    This method output must be concise and it currently changes based on operating system.
    """
    # This approach is less efficient than using modified time
    # sum([path.stat().st_mtime for path in dir_path.glob("**/*")])
    # unfortunately, the modified time approach does not work well for dag-only deployments
    # where DAGs are constantly synced to the deployed Airflow
    # for 5k files, this seems to take 0.14
    hasher = hashlib.md5()
    filepaths = []

    for root_dir, dirs, files in os.walk(dir_path):
        paths = [os.path.join(root_dir, filepath) for filepath in files]
        filepaths.extend(paths)

    for filepath in sorted(filepaths):
        try:
            with open(str(filepath), "rb") as fp:
                buf = fp.read()
                hasher.update(buf)
        except FileNotFoundError:
            logger.warning(f"The dbt project folder contains a symbolic link to a non-existent file: {filepath}")

    return hasher.hexdigest()


def _calculate_dbt_ls_cache_current_version(cache_identifier: str, project_dir: Path, cmd_args: list[str]) -> str:
    """
    Taking into account the project directory contents and the command arguments, calculate the
    hash that represents the "dbt ls" command version - to be used to decide if the cache should be refreshed or not.

    :param cache_identifier: Unique identifier of the cache (may include DbtDag or DbtTaskGroup information)
    :param project_path: Path to the target dbt project directory
    :param cmd_args: List containing the arguments passed to the dbt ls command that would affect its output
    """
    start_time = time.perf_counter()

    # Combined value for when the dbt project directory files were last modified
    # This is fast (e.g. 0.01s for jaffle shop, 0.135s for a 5k models dbt folder)
    dbt_project_hash = _create_folder_version_hash(project_dir)

    # The performance for the following will depend on the user's configuration
    hash_args = hashlib.md5("".join(cmd_args).encode()).hexdigest()

    elapsed_time = time.perf_counter() - start_time
    logger.info(
        f"Cosmos performance: time to calculate cache identifier {cache_identifier} for current version: {elapsed_time}"
    )
    return f"{dbt_project_hash},{hash_args}"


@functools.lru_cache
def was_project_modified(previous_version: str, current_version: str) -> bool:
    """
    Given the cache version of a project and the latest version of the project,
    decides if the project was modified or not.
    """
    return previous_version != current_version


@provide_session
def delete_unused_dbt_ls_cache(
    max_age_last_usage: timedelta = timedelta(days=30), session: Session | None = None
) -> int:
    """
    Delete Cosmos cache stored in Airflow Variables based on the last execution of their associated DAGs.

    Example usage:

    There are three Cosmos cache Airflow Variables:
    1. ``cache cosmos_cache__basic_cosmos_dag``
    2. ``cosmos_cache__basic_cosmos_task_group__orders``
    3. ``cosmos_cache__basic_cosmos_task_group__customers``

    The first relates to the ``DbtDag`` ``basic_cosmos_dag`` and the two last ones relate to the DAG
    ``basic_cosmos_task_group`` that has two ``DbtTaskGroups``: ``orders`` and ``customers``.

    Let's assume the last DAG run of ``basic_cosmos_dag`` was a week ago and the last DAG run of
    ``basic_cosmos_task_group`` was an hour ago.

    To delete the cache related to ``DbtDags`` and ``DbtTaskGroup`` that were run more than 5 days ago:

    ..code: python
        >>> delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(days=5))
        INFO - Removing the dbt ls cache cosmos_cache__basic_cosmos_dag

    To delete the cache related to ``DbtDags`` and ``DbtTaskGroup`` that were run more than 10 minutes ago:

    ..code: python
        >>> delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(minutes=10))
        INFO - Removing the dbt ls cache cosmos_cache__basic_cosmos_dag
        INFO - Removing the dbt ls cache cosmos_cache__basic_cosmos_task_group__orders
        INFO - Removing the dbt ls cache cosmos_cache__basic_cosmos_task_group__orders

    To delete the cache related to ``DbtDags`` and ``DbtTaskGroup`` that were run more than 10 days ago

    ..code: python
        >>> delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(days=10))

    In this last example, nothing is deleted.
    """
    if session is None:
        return 0

    logger.info(f"Delete the Cosmos cache stored in Airflow Variables that hasn't been used for  {max_age_last_usage}")
    cosmos_dags_ids = defaultdict(list)
    all_variables = session.scalars(select(Variable)).all()
    total_cosmos_variables = 0
    deleted_cosmos_variables = 0

    # Identify Cosmos-related cache in Airflow variables
    for var in all_variables:
        if var.key.startswith(VAR_KEY_CACHE_PREFIX):
            var_value = json.loads(var.val)
            cosmos_dags_ids[var_value["dag_id"]].append(var.key)
            total_cosmos_variables += 1

    # Delete DAGs that have not been run in the last X time
    for dag_id, vars_keys in cosmos_dags_ids.items():
        last_dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        if last_dag_run and last_dag_run.execution_date < (datetime.now(timezone.utc) - max_age_last_usage):
            for var_key in vars_keys:
                logger.info(f"Removing the dbt ls cache {var_key}")
                Variable.delete(var_key)
                deleted_cosmos_variables += 1

    logger.info(
        f"Deleted {deleted_cosmos_variables}/{total_cosmos_variables} Airflow Variables used to store  Cosmos cache. "
    )
    return deleted_cosmos_variables


# TODO: Add integration tests once remote cache is supported in the CI pipeline
@provide_session
def delete_unused_dbt_ls_remote_cache_files(  # pragma: no cover
    max_age_last_usage: timedelta = timedelta(days=30), session: Session | None = None
) -> int:
    """
    Delete Cosmos cache stored in remote storage based on the last execution of their associated DAGs.
    """
    if session is None:
        return 0

    logger.info(f"Delete the Cosmos cache stored remotely that hasn't been used for  {max_age_last_usage}")
    cosmos_dags_ids_remote_cache_files = defaultdict(list)

    configured_remote_cache_dir = _configure_remote_cache_dir()
    if not configured_remote_cache_dir:
        logger.info(
            "No remote cache directory configured. Skipping the deletion of the dbt ls cache files in remote storage."
        )
        return 0

    dirs = [obj for obj in configured_remote_cache_dir.iterdir() if obj.is_dir()]
    files = [f for label in dirs for f in label.iterdir() if f.is_file()]

    total_cosmos_remote_cache_files = 0
    for file in files:
        prefix_path = (configured_remote_cache_dir / VAR_KEY_CACHE_PREFIX).as_uri()
        if file.as_uri().startswith(prefix_path):
            with file.open("r") as fp:
                cache_dict = json.load(fp)
            cosmos_dags_ids_remote_cache_files[cache_dict["dag_id"]].append(file)
            total_cosmos_remote_cache_files += 1

    deleted_cosmos_remote_cache_files = 0

    for dag_id, files in cosmos_dags_ids_remote_cache_files.items():
        last_dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        if last_dag_run and last_dag_run.execution_date < (datetime.now(timezone.utc) - max_age_last_usage):
            for file in files:
                logger.info(f"Removing the dbt ls cache remote file {file}")
                file.unlink()
                deleted_cosmos_remote_cache_files += 1
    logger.info(
        "Deleted %s/%s dbt ls cache files in remote storage.",
        deleted_cosmos_remote_cache_files,
        total_cosmos_remote_cache_files,
    )

    return deleted_cosmos_remote_cache_files


def is_profile_cache_enabled() -> bool:
    """Return True if global and profile cache is enable"""
    return enable_cache and enable_cache_profile


def _get_or_create_profile_cache_dir() -> Path:
    """
    Get or create the directory path for caching DBT profiles.

    - Constructs the profile cache directory path based on cache_dir and dbt_profile_cache_dir.
    - Checks if the directory exists; if not, creates it
    - Return profile cache directory
    """
    profile_cache_dir = cache_dir / dbt_profile_cache_dir_name
    if not profile_cache_dir.exists():
        profile_cache_dir.mkdir(parents=True, exist_ok=True)
    return profile_cache_dir


def get_cached_profile(version: str) -> Path | None:
    """
    Retrieve the path to a cached DBT profile YML file if it exists for the given version.

    - Constructs the DBT profile YML Path based on version and profile cache directory
    - Checks if the profile YML exists
    - Return the profile YML Path
    """
    profile_yml_path = _get_or_create_profile_cache_dir() / version / DEFAULT_PROFILES_FILE_NAME
    if profile_yml_path.exists() and profile_yml_path.is_file():
        return profile_yml_path
    return None


def create_cache_profile(version: str, profile_content: str) -> Path:
    """
    Create a cached DBT profile YAML file with the provided content for the given version.

    - Constructs the path for profile YML  based on the version in the profile cache directory
    - Creates the profile directory if it does not exist
    - Writes the profile content to the profile YML file
    - Return the profile YML Path
    """
    profile_yml_dir = _get_or_create_profile_cache_dir() / version
    profile_yml_dir.mkdir(parents=True, exist_ok=True)
    profile_yml_path = profile_yml_dir / DEFAULT_PROFILES_FILE_NAME
    profile_yml_path.write_text(profile_content)
    return profile_yml_path


def is_cache_package_lockfile_enabled(project_dir: Path) -> bool:
    if not enable_cache_package_lockfile:
        return False
    package_lockfile = project_dir / PACKAGE_LOCKFILE_YML
    return package_lockfile.is_file()


def _get_sha1_hash(yaml_file: Path) -> str:
    """Read package-lock.yml file and return sha1_hash"""
    with open(yaml_file) as file:
        yaml_content = file.read()
    data = yaml.safe_load(yaml_content)
    sha1_hash: str = data.get("sha1_hash", "")
    return sha1_hash


def _get_latest_cached_package_lockfile(project_dir: Path) -> Path | None:
    """
    Retrieves the latest cached package-lock.yml for the specified project directory,
    or creates and caches it if not already cached and hashes match.
    """
    cache_identifier = project_dir.name
    package_lockfile = project_dir / PACKAGE_LOCKFILE_YML
    cached_package_lockfile = cache_dir / cache_identifier / PACKAGE_LOCKFILE_YML

    if cached_package_lockfile.exists() and cached_package_lockfile.is_file():
        project_sha1_hash = _get_sha1_hash(package_lockfile)
        cached_sha1_hash = _get_sha1_hash(cached_package_lockfile)
        if project_sha1_hash == cached_sha1_hash:
            return cached_package_lockfile
    cached_lockfile_dir = cache_dir / cache_identifier
    cached_lockfile_dir.mkdir(parents=True, exist_ok=True)
    _safe_copy(package_lockfile, cached_package_lockfile)
    return cached_package_lockfile


def _copy_cached_package_lockfile_to_project(cached_package_lockfile: Path, project_dir: Path) -> None:
    """Copy the cached package-lock.yml to tmp project dir"""
    package_lockfile = project_dir / PACKAGE_LOCKFILE_YML
    _safe_copy(cached_package_lockfile, package_lockfile)


# TODO: Move this function to a different location
def _safe_copy(src: Path, dst: Path) -> None:
    """
    Safely copies a file from a source path to a destination path.

    This function ensures that the copy operation is atomic by first
    copying the file to a temporary file in the same directory as the
    destination and then renaming the temporary file to the destination
    file. This approach minimizes the risk of file corruption or partial
    writes in case of a failure or interruption during the copy process.

    See the blog for atomic file operations:
    https://alexwlchan.net/2019/atomic-cross-filesystem-moves-in-python/
    """
    # Create a temporary file in the same directory as the destination
    dir_name, base_name = os.path.split(dst)
    temp_fd, temp_path = tempfile.mkstemp(dir=dir_name)

    shutil.copyfile(src, temp_path)

    # Rename the temporary file to the destination file
    os.rename(temp_path, dst)

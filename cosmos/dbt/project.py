from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from cosmos.constants import (
    DBT_DEPENDENCIES_FILE_NAMES,
    DBT_LOG_DIR_NAME,
    DBT_PACKAGES_FOLDER,
    DBT_PARTIAL_PARSE_FILE_NAME,
    DBT_TARGET_DIR_NAME,
    PACKAGE_LOCKFILE_YML,
)
from cosmos.log import get_logger

logger = get_logger(__name__)


def has_non_empty_dependencies_file(project_path: Path) -> bool:
    """
    Check if the dbt project has dependencies.yml or packages.yml.

    :param project_path: Path to the project
    :returns: True or False
    """
    project_dir = Path(project_path)
    for filename in DBT_DEPENDENCIES_FILE_NAMES:
        filepath = project_dir / filename
        if filepath.exists() and filepath.stat().st_size > 0:
            return True

    logger.info(f"Project {project_path} does not have {DBT_DEPENDENCIES_FILE_NAMES}")
    return False


def copy_dbt_packages(source_folder: Path, target_folder: Path) -> None:
    """
    Copies the dbt packages related files and directories from source_folder to target_folder.

    :param: source_folder: The base directory where paths are sourced from.
    :param: target_folder: The directory where paths will be copied to.
    """
    logger.info("Copying dbt packages to temporary folder...")
    dbt_packages_paths = [DBT_PACKAGES_FOLDER, PACKAGE_LOCKFILE_YML]

    for relative_path in dbt_packages_paths:
        src_path = source_folder / relative_path
        dst_path = target_folder / relative_path

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)

        if src_path.is_dir():
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
        else:
            shutil.copy2(src_path, dst_path)

    logger.info("Completed copying dbt packages to temporary folder.")


def create_symlinks(project_path: Path, tmp_dir: Path, ignore_dbt_packages: bool) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = [DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, PACKAGE_LOCKFILE_YML, "profiles.yml"]
    if ignore_dbt_packages:
        # this is linked to dbt deps so if dbt deps is true then ignore existing dbt_packages folder
        ignore_paths.append(DBT_PACKAGES_FOLDER)
    for child_name in os.listdir(project_path):
        if child_name not in ignore_paths:
            os.symlink(project_path / child_name, tmp_dir / child_name)


def get_partial_parse_path(project_dir_path: Path) -> Path:
    """
    Return the partial parse (partial_parse.msgpack) path for a given dbt project directory.
    """
    return project_dir_path / DBT_TARGET_DIR_NAME / DBT_PARTIAL_PARSE_FILE_NAME


@contextmanager
def environ(env_vars: dict[str, str]) -> Generator[None, None, None]:
    """Temporarily set environment variables inside the context manager and restore
    when exiting.
    """
    original_env = {key: os.getenv(key) for key in env_vars}
    os.environ.update(env_vars)
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value


@contextmanager
def change_working_directory(path: str) -> Generator[None, None, None]:
    """Temporarily changes the working directory to the given path, and then restores
    back to the previous value on exit.
    """
    previous_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous_cwd)

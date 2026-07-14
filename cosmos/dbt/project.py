from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import yaml
from jinja2 import Template

from cosmos.constants import (
    DBT_DEFAULT_PACKAGES_FOLDER,
    DBT_DEPENDENCIES_FILE_NAMES,
    DBT_LOG_DIR_NAME,
    DBT_MANIFEST_FILE_NAME,
    DBT_PARTIAL_PARSE_FILE_NAME,
    DBT_PROJECT_FILENAME,
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

    logger.info("Project %s does not have %s", project_path, DBT_DEPENDENCIES_FILE_NAMES)
    return False


def _resolve_env_var(template_str: str) -> str:
    """
    Given a Jinja template string, resolve the environment variables, declared using the dbt syntax,
    and return the rendered string.

    Example:
    - template_str = '/usr/local/airflow/dags/dbt/dbt_packages{{ "_" + env_var("env","") if env_var("env","")!="" }}'
    - environment variable `env` is set to "test"

    Then, the rendered string will be:
    '/usr/local/airflow/dags/dbt/dbt_packages_test'
    """

    def env_var(name: str, default: str = "") -> str:
        return os.getenv(name, default)

    template = Template(template_str)
    rendered = template.render(env_var=env_var)
    return rendered


def get_dbt_packages_subpath(source_folder: Path) -> str:
    """
    Return the dbt project's package installation sub path.

    By default, ``dbt deps`` installs packages in the ``dbt_packages`` directory, inside the dbt project folder.
    Users can specify a custom directory via the `packages-install-path` in the ``dbt_project.yml`` file.
    Example: ``packages-install-path: custom_dbt_packages``.

    More information:
    https://docs.getdbt.com/reference/project-configs/packages-install-path

    :param source_folder: The dbt project root directory
    :returns: A string containing the dbt_packages subpath within the source folder.
    """
    subpath = DBT_DEFAULT_PACKAGES_FOLDER
    dbt_project_yml_path = source_folder / DBT_PROJECT_FILENAME
    if dbt_project_yml_path.exists():
        with open(dbt_project_yml_path) as fp:
            try:
                dbt_project_file_content = yaml.safe_load(fp)
            except yaml.YAMLError:
                logger.info("Unable to read the %s file", DBT_PROJECT_FILENAME)
            else:
                if isinstance(dbt_project_file_content, dict):
                    subpath = dbt_project_file_content.get("packages-install-path", DBT_DEFAULT_PACKAGES_FOLDER)
    return _resolve_env_var(subpath)


def copy_dbt_packages(source_folder: Path, target_folder: Path) -> None:
    """
    Copies the dbt packages related files and directories from source_folder to target_folder.

    :param source_folder: The base directory where paths are sourced from.
    :param target_folder: The directory where paths will be copied to.
    """
    logger.info("Copying dbt packages to temporary folder...")

    dbt_packages_folder = get_dbt_packages_subpath(source_folder)
    dbt_packages_paths = [dbt_packages_folder, PACKAGE_LOCKFILE_YML]

    for relative_path in dbt_packages_paths:
        src_path = source_folder / relative_path
        dst_path = target_folder / relative_path

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)

        if src_path.is_dir():
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
        else:
            shutil.copy2(src_path, dst_path)

    logger.info("Completed copying dbt packages to temporary folder.")


def copy_manifest_file_if_exists(source_manifest: str | Path, dbt_project_folder: str | Path) -> None:
    """
    Copies the source manifest.json file, if available, to the given desired dbt project folder.

    :param source_manifest: manifest.json filepath
    :param dbt_project_folder: destination dbt project folder (it will be copied to the target folder)
    """
    dbt_project_folder = Path(dbt_project_folder)
    source_manifest = str(source_manifest)
    if source_manifest and Path(source_manifest).exists():
        logger.info("Copying the manifest from %s...", source_manifest)
        target_folder_path = dbt_project_folder / DBT_TARGET_DIR_NAME
        tmp_manifest_filepath = target_folder_path / DBT_MANIFEST_FILE_NAME
        Path(target_folder_path).mkdir(parents=True, exist_ok=True)
        shutil.copy(source_manifest, tmp_manifest_filepath)


def create_symlinks(project_path: Path, tmp_dir: Path, ignore_dbt_packages: bool) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = [DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, PACKAGE_LOCKFILE_YML, "profiles.yml"]
    if ignore_dbt_packages:
        dbt_packages_subpath = get_dbt_packages_subpath(project_path)
        # this is linked to dbt deps so if dbt deps is true then ignore existing dbt_packages folder
        ignore_paths.append(dbt_packages_subpath)
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


def _resolve_dags_folder() -> str | None:
    """Return the realpath of the Airflow DAGs folder, or ``None`` if it cannot be determined.

    ``airflow.settings.DAGS_FOLDER`` is the resolved path Airflow itself appends to ``sys.path`` on
    both Airflow 2 and 3 (and on Astro). Note that Airflow 3 custom DAG bundles may live outside
    ``DAGS_FOLDER``; those are not covered here.
    """
    try:
        from airflow.settings import DAGS_FOLDER
    except Exception:
        return None
    return os.path.realpath(DAGS_FOLDER) if DAGS_FOLDER else None


# dbt-core discovers plugins by importing *every* importable top-level module whose name starts with
# ``dbt_`` (see ``dbt.plugins.manager._get_dbt_modules``, which scans the whole ``sys.path`` via
# ``pkgutil.iter_modules``). Airflow puts its DAGs folder on the path, so when Cosmos runs ``dbt``
# (the default for ``LoadMode.DBT_LS`` uses the in-process ``dbtRunner`` since Cosmos 1.9), dbt's
# plugin discovery imports any DAG file named ``dbt_*.py``. That executes the DAG file -- leaking and
# duplicating DAGs across unrelated Cosmos DAGs (in-process) or crashing the dbt command when the DAG
# file re-imports Airflow (subprocess). The two helpers below keep the DAGs folder out of dbt's
# reach: ``exclude_dags_folder_from_sys_path`` for in-process ``dbtRunner`` (live ``sys.path``) and
# ``remove_dags_folder_from_pythonpath`` for subprocess dbt (inherited ``PYTHONPATH``).
# See https://github.com/astronomer/astronomer-cosmos/issues/1673
@contextmanager
def exclude_dags_folder_from_sys_path() -> Generator[None, None, None]:
    """Temporarily remove the Airflow DAGs folder from ``sys.path`` while dbt runs in-process.

    Used around in-process ``dbtRunner`` invocations so dbt's ``dbt_*`` plugin discovery does not
    import DAG files, while leaving genuinely installed dbt plugins (which live in site-packages, not
    the DAGs folder) untouched.
    """
    target = _resolve_dags_folder()

    # (original index, path) for each matching entry, so removal can't disturb the recorded
    # positions of entries found later in the scan.
    removed: list[tuple[int, str]] = []
    if target:
        removed = [(index, path) for index, path in enumerate(sys.path) if os.path.realpath(path) == target]
        for _, path in removed:
            sys.path.remove(path)
    try:
        yield
    finally:
        # Restore in ascending original-index order so each insert lands in its original slot:
        # every entry restored so far already occupies its correct position, and no not-yet-restored
        # entry sits before this index either.
        for index, path in removed:
            sys.path.insert(index, path)


def remove_dags_folder_from_pythonpath(env: dict[str, str]) -> dict[str, str]:
    """Return a copy of ``env`` with the Airflow DAGs folder removed from ``PYTHONPATH``.

    Companion to :func:`exclude_dags_folder_from_sys_path` for the subprocess code path: a dbt
    subprocess derives its ``sys.path`` from the inherited ``PYTHONPATH``, so dbt's ``dbt_*`` plugin
    discovery would otherwise import DAG files from the DAGs folder -- crashing the dbt command when
    the DAG file re-imports Airflow, or running DAG-file side effects. Only the DAGs-folder entries
    are dropped; every other entry (including genuinely installed dbt plugins) is preserved.
    """
    pythonpath = env.get("PYTHONPATH")
    target = _resolve_dags_folder()
    if not pythonpath or not target:
        return dict(env)

    kept = [entry for entry in pythonpath.split(os.pathsep) if not (entry and os.path.realpath(entry) == target)]
    sanitized = dict(env)
    sanitized["PYTHONPATH"] = os.pathsep.join(kept)
    return sanitized

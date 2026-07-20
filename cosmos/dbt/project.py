from __future__ import annotations

import os
import shutil
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
    rendered: str = template.render(env_var=env_var)
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


def _resolve_extra_source(project_path: Path, extra_path: str | Path) -> Path:
    """Resolve an entry of ``extra_paths`` to an absolute path (relative entries are relative to the project root)."""
    source = Path(extra_path)
    if not source.is_absolute():
        source = Path(project_path) / source
    return source.resolve()


def _relpath_to_project(source: Path, project_path: Path) -> str | None:
    """
    Return ``source`` expressed relative to ``project_path``, or ``None`` when no relative path exists.

    On Windows ``os.path.relpath`` raises ``ValueError`` when the two paths are on different drives (e.g. the
    project is on ``C:`` and an absolute extra path is on ``D:``). Such a path cannot be referenced relatively from
    the project root, so it has no bearing on clone nesting and is materialised by dbt via its absolute path
    directly; callers treat ``None`` as "nothing to reproduce / no nesting impact".
    """
    try:
        return os.path.relpath(source, project_path)
    except ValueError:
        return None


def compute_extra_paths_parent_depth(project_path: Path, extra_paths: list[str | Path] | None) -> int:
    """
    Return how many parent (``..``) levels the deepest *relative* entry in ``extra_paths`` reaches above the
    dbt project root.

    A reference such as ``model-paths: ["models", "../shared"]`` reaches one level up; a package declared as
    ``- local: "../../dbt_utils"`` reaches two. Absolute entries, or entries that live inside the project, reach
    zero. The result tells :func:`prepare_dbt_project_clone_dir` how deeply the project clone must be nested so
    that every ``..`` reference stays inside the temporary tree.
    """
    project_path = Path(project_path).resolve()
    max_up = 0
    for extra_path in extra_paths or []:
        source = _resolve_extra_source(project_path, extra_path)
        relative_to_project = _relpath_to_project(source, project_path)
        if relative_to_project is None:  # e.g. a different Windows drive — no ``..`` chain, no nesting impact
            continue
        rel_parts = Path(relative_to_project).parts
        up = 0
        for part in rel_parts:
            if part == os.pardir:  # os.path.relpath emits all ``..`` components at the front
                up += 1
            else:
                break
        max_up = max(max_up, up)
    return max_up


def prepare_dbt_project_clone_dir(tmp_dir: Path, project_path: Path, extra_paths: list[str | Path] | None) -> Path:
    """
    Return the directory *inside* ``tmp_dir`` into which the dbt project should be cloned.

    Cosmos clones the project into a temporary directory and runs dbt from there. When ``dbt_project.yml`` or
    ``packages.yml`` reference paths above the project root (``../shared_models``, ``- local: "../../dbt_utils"``),
    those references must keep resolving after the clone. They can only do so if the clone has enough parent
    directories above it. A temporary directory sits directly under the OS temp root (e.g. ``/tmp/tmpXXXX``), so a
    ``../../`` reference from there escapes to the filesystem root — an unwritable, unreadable location on a normal
    worker, and a location shared between concurrent tasks.

    This helper nests the clone ``compute_extra_paths_parent_depth`` levels below ``tmp_dir`` (using throwaway
    placeholder parents whose names are irrelevant to dbt, which keys off ``--project-dir`` and the ``name:`` in
    ``dbt_project.yml``). Every ``..`` reference then resolves to a sibling created *inside* ``tmp_dir``, which is
    unique per invocation, writable, and cleaned up automatically when the temporary directory is torn down.

    With no upward extra paths the clone directory is ``tmp_dir`` itself, so behaviour is unchanged for projects
    that do not use this feature.
    """
    tmp_dir = Path(tmp_dir)
    depth = compute_extra_paths_parent_depth(project_path, extra_paths)
    if depth == 0:
        # No upward references: the clone is the temporary directory itself, which already exists.
        return tmp_dir
    clone_dir = tmp_dir
    for level in range(depth):
        clone_dir = clone_dir / f"__cosmos_extra_path_parent_{level}__"
    clone_dir.mkdir(parents=True, exist_ok=True)
    return clone_dir


def create_symlinks_for_extra_paths(project_path: Path, clone_dir: Path, extra_paths: list[str | Path]) -> None:
    """
    Reproduce each of ``extra_paths`` inside the project clone at the same location it occupies relative to the
    project root, so relative references in ``dbt_project.yml`` / ``packages.yml`` keep resolving.

    Cosmos runs dbt from a temporary clone of the project, symlinking (or copying) the project's own files into it.
    Anything the ``dbt_project.yml`` references *outside* the project root – for example a shared models/sources
    folder declared as ``model-paths: ["models", "../shared"]`` or a dbt local package declared in ``packages.yml``
    as ``- local: "../shared_package"`` – is otherwise missing from the clone, so dbt cannot find it.

    ``clone_dir`` must already be nested deeply enough (see :func:`prepare_dbt_project_clone_dir`) that no
    reproduced path escapes the temporary tree; every link created here therefore lives inside the temporary
    directory and needs no explicit cleanup. Symbolic links are used (consistent with :func:`create_symlinks`), so
    the user's source trees are never copied and read-only sources are supported.

    :param project_path: The dbt project root the clone is built from.
    :param clone_dir: The (possibly nested) directory the project was cloned into — the clone's project root.
    :param extra_paths: Absolute paths, or paths relative to ``project_path``, to include in the clone.
    """
    clone_dir = Path(clone_dir)
    # Resolve consistently with ``compute_extra_paths_parent_depth`` (which drives the clone nesting depth): both
    # must relativise against the *same* base, or ``relpath`` here could yield more ``..`` than the clone was
    # nested for and the reproduced link would land outside the temp tree / where dbt won't look for it.
    project_path = Path(project_path).resolve()

    for extra_path in extra_paths:
        source = _resolve_extra_source(project_path, extra_path)

        if not source.exists():
            logger.warning("Skipping extra path %s because it does not exist.", source)
            continue

        relative_to_project = _relpath_to_project(source, project_path)
        if relative_to_project is None:
            # No relative path exists (e.g. a different Windows drive); dbt resolves such an absolute reference
            # directly, so there is nothing to reproduce in the clone.
            logger.debug("Skipping extra path %s because it has no path relative to the project root.", source)
            continue
        destination = Path(os.path.normpath(clone_dir / relative_to_project))

        if destination.is_symlink() and destination.resolve() == source:
            # Already in place (e.g. the same extra path listed twice); nothing to do.
            continue
        if destination.exists() or destination.is_symlink():
            logger.warning(
                "Skipping extra path %s because destination %s already exists in the clone.",
                source,
                destination,
            )
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(source, destination)
        except FileExistsError:
            logger.debug("Extra path destination %s was created concurrently; reusing it.", destination)
            continue


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

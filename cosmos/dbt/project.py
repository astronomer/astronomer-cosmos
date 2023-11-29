from __future__ import annotations

from pathlib import Path
import os
from cosmos.constants import (
    DBT_LOG_DIR_NAME,
    DBT_TARGET_DIR_NAME,
)
from contextlib import contextmanager
from typing import Generator


def create_symlinks(project_path: Path, tmp_dir: Path) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = (DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, "dbt_packages", "profiles.yml")
    for child_name in os.listdir(project_path):
        if child_name not in ignore_paths:
            os.symlink(project_path / child_name, tmp_dir / child_name)


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

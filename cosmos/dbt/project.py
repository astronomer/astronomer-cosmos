from pathlib import Path
import os
from cosmos.constants import (
    DBT_LOG_DIR_NAME,
    DBT_TARGET_DIR_NAME,
)


def create_symlinks(project_path: Path, tmp_dir: Path) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = (DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, "dbt_packages", "profiles.yml")
    for child_name in os.listdir(project_path):
        if child_name not in ignore_paths:
            os.symlink(project_path / child_name, tmp_dir / child_name)

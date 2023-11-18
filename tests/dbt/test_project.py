from pathlib import Path
from cosmos.dbt.project import create_symlinks, environ
import os
from unittest.mock import patch

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"


def test_create_symlinks(tmp_path):
    """Tests that symlinks are created for expected files in the dbt project directory."""
    tmp_dir = tmp_path / "dbt-project"
    tmp_dir.mkdir()

    create_symlinks(DBT_PROJECTS_ROOT_DIR / "jaffle_shop", tmp_dir)
    for child in tmp_dir.iterdir():
        assert child.is_symlink()
        assert child.name not in ("logs", "target", "profiles.yml", "dbt_packages")


@patch.dict(os.environ, {"ORIGINAL_VAR": "value"})
def test_environ_context_manager():
    # Define the expected environment variables
    expected_env_vars = {"DBT_PROJECTS_ROOT_DIR": "/path/to/dbt/projects", "DBT_LOG_LEVEL": "debug"}
    # Use the environ context manager
    with environ(expected_env_vars):
        # Check if the environment variables are set correctly
        for key, value in expected_env_vars.items():
            assert value == os.environ.get(key)
        # Check if the original environment variables are still set
        assert "value" == os.environ.get("ORIGINAL_VAR")
    # Check if the environment variables are unset after exiting the context manager
    for key in expected_env_vars.keys():
        assert os.environ.get(key) is None
    # Check if the original environment variables are still set
    assert "value" == os.environ.get("ORIGINAL_VAR")

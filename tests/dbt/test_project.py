import os
from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.dbt.project import change_working_directory, copy_msgpack_for_partial_parse, create_symlinks, environ

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"


def test_create_symlinks(tmp_path):
    """Tests that symlinks are created for expected files in the dbt project directory."""
    tmp_dir = tmp_path / "dbt-project"
    tmp_dir.mkdir()

    create_symlinks(DBT_PROJECTS_ROOT_DIR / "jaffle_shop", tmp_dir, False)
    for child in tmp_dir.iterdir():
        assert child.is_symlink()
        assert child.name not in ("logs", "target", "profiles.yml", "dbt_packages")


@pytest.mark.parametrize("exists", [True, False])
def test_copy_manifest_for_partial_parse(tmp_path, exists):
    project_path = tmp_path / "project"
    target_path = project_path / "target"
    partial_parse_file = target_path / "partial_parse.msgpack"

    target_path.mkdir(parents=True)

    if exists:
        partial_parse_file.write_bytes(b"")

    tmp_dir = tmp_path / "tmp_dir"
    tmp_dir.mkdir()

    copy_msgpack_for_partial_parse(project_path, tmp_dir)

    tmp_partial_parse_file = tmp_dir / "target" / "partial_parse.msgpack"

    if exists:
        assert tmp_partial_parse_file.exists()
    else:
        assert not tmp_partial_parse_file.exists()


@patch.dict(os.environ, {"VAR1": "value1", "VAR2": "value2"})
def test_environ_context_manager():
    # Define the expected environment variables
    expected_env_vars = {"VAR2": "new_value2", "VAR3": "value3"}
    # Use the environ context manager
    with environ(expected_env_vars):
        # Check if the environment variables are set correctly
        for key, value in expected_env_vars.items():
            assert value == os.environ.get(key)
        # Check if the original non-overlapping environment variable is still set
        assert "value1" == os.environ.get("VAR1")
    # Check if the environment variables are unset after exiting the context manager
    assert os.environ.get("VAR3") is None
    # Check if the original environment variables are still set
    assert "value1" == os.environ.get("VAR1")
    assert "value2" == os.environ.get("VAR2")


@patch("os.chdir")
def test_change_working_directory(mock_chdir):
    """Tests that the working directory is changed and then restored correctly."""
    # Define the path to change the working directory to
    path = "/path/to/directory"

    # Use the change_working_directory context manager
    with change_working_directory(path):
        # Check if os.chdir is called with the correct path
        mock_chdir.assert_called_once_with(path)

    # Check if os.chdir is called with the previous working directory
    mock_chdir.assert_called_with(os.getcwd())

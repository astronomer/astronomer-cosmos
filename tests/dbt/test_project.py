import os
from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.dbt.project import change_working_directory, create_symlinks, environ, has_non_empty_dependencies_file

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"


def test_create_symlinks(tmp_path):
    """Tests that symlinks are created for expected files in the dbt project directory."""
    tmp_dir = tmp_path / "dbt-project"
    tmp_dir.mkdir()

    create_symlinks(DBT_PROJECTS_ROOT_DIR / "altered_jaffle_shop", tmp_dir, False)
    for child in tmp_dir.iterdir():
        assert child.is_symlink()
        assert child.name not in ("logs", "target", "profiles.yml", "dbt_packages")


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


@pytest.mark.parametrize("filename", ["packages.yml", "dependencies.yml"])
def test_has_non_empty_dependencies_file_is_true(tmpdir, filename):
    filepath = Path(tmpdir) / filename
    filepath.write_text("content")
    assert has_non_empty_dependencies_file(tmpdir)


@pytest.mark.parametrize("filename", ["packages.yml", "dependencies.yml"])
def test_has_non_empty_dependencies_file_is_false(tmpdir, filename):
    filepath = Path(tmpdir) / filename
    filepath.touch()
    assert not has_non_empty_dependencies_file(tmpdir)


def test_has_non_empty_dependencies_file_is_false_in_empty_dir(tmpdir):
    assert not has_non_empty_dependencies_file(tmpdir)

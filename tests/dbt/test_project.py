import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from cosmos.constants import DBT_DEFAULT_PACKAGES_FOLDER, DBT_PROJECT_FILENAME, PACKAGE_LOCKFILE_YML
from cosmos.dbt.project import (
    change_working_directory,
    copy_dbt_packages,
    copy_manifest_file_if_exists,
    create_symlinks,
    environ,
    get_dbt_packages_subpath,
    has_non_empty_dependencies_file,
)

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent.parent / "dev/dags/dbt"


def test_copy_manifest_file_if_exists(tmpdir):
    source_manifest = tmpdir / "manifest.json"
    source_manifest.write_text('{"mock_key": "mock_value"}', encoding="utf-8")

    dbt_project_folder = tmpdir / "dbt_project"

    target_manifest_path = dbt_project_folder / "target/manifest.json"
    assert not target_manifest_path.exists()

    copy_manifest_file_if_exists(source_manifest, dbt_project_folder)

    assert target_manifest_path.exists()
    assert target_manifest_path.read_text(encoding="utf-8") == '{"mock_key": "mock_value"}'


def test_does_nothing_if_manifest_missing(tmpdir):
    source_manifest = tmpdir / "nonexistent_manifest.json"
    dbt_project_folder = tmpdir / "dbt_project"

    copy_manifest_file_if_exists(source_manifest, dbt_project_folder)

    target_folder = dbt_project_folder / "target"
    assert not target_folder.exists()


def write_dbt_project_yml(path: Path, content: dict):
    with open(path / "dbt_project.yml", "w") as fp:
        yaml.dump(content, fp)
    return path


def test_returns_default_when_file_missing(tmpdir):
    result = get_dbt_packages_subpath(tmpdir)
    assert result == "dbt_packages"


def test_returns_default_when_key_missing(tmpdir):
    write_dbt_project_yml(tmpdir, {"name": "test_project"})
    result = get_dbt_packages_subpath(tmpdir)
    assert result == "dbt_packages"


def test_returns_default_on_invalid_yaml(tmpdir, caplog):
    # Write malformed YAML
    bad_content = ":\nbad_yaml: [::"
    path = tmpdir / DBT_PROJECT_FILENAME
    path.write_text(bad_content, "utf-8")

    result = get_dbt_packages_subpath(tmpdir)
    assert result == DBT_DEFAULT_PACKAGES_FOLDER
    assert "Unable to read" in caplog.text


def test_returns_custom_path_when_defined(tmpdir):
    write_dbt_project_yml(tmpdir, {"packages-install-path": "custom_dbt_packages"})
    result = get_dbt_packages_subpath(tmpdir)
    assert result == "custom_dbt_packages"


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


@patch("cosmos.dbt.project.shutil.copy2")
@patch("cosmos.dbt.project.shutil.copytree")
@patch("cosmos.dbt.project.os.makedirs")
@patch("cosmos.dbt.project.Path.is_dir", side_effect=[True, False])
@patch("cosmos.dbt.project.logger")
def test_copy_dbt_packages_all_cases(mock_logger, mock_dir, mock_makedirs, mock_copytree, mock_copy2):
    source_folder = Path("/fake/source")
    target_folder = Path("/fake/target")

    copy_dbt_packages(source_folder, target_folder)

    assert mock_makedirs.call_count == 2

    mock_copytree.assert_called_once_with(
        source_folder / DBT_DEFAULT_PACKAGES_FOLDER, target_folder / DBT_DEFAULT_PACKAGES_FOLDER, dirs_exist_ok=True
    )
    mock_copy2.assert_called_once_with(source_folder / PACKAGE_LOCKFILE_YML, target_folder / PACKAGE_LOCKFILE_YML)

    mock_logger.info.assert_any_call("Copying dbt packages to temporary folder...")
    mock_logger.info.assert_any_call("Completed copying dbt packages to temporary folder.")

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from cosmos.constants import DBT_DEFAULT_PACKAGES_FOLDER, DBT_PROJECT_FILENAME, PACKAGE_LOCKFILE_YML
from cosmos.dbt.project import (
    _resolve_dags_folder,
    _resolve_env_var,
    change_working_directory,
    copy_dbt_packages,
    copy_manifest_file_if_exists,
    create_symlinks,
    environ,
    exclude_dags_folder_from_sys_path,
    get_dbt_packages_subpath,
    has_non_empty_dependencies_file,
    remove_dags_folder_from_pythonpath,
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


@patch.dict(os.environ, {"MY_PATH": "custom_packages"})
def test_resolve_env_var_with_simple_env_var():
    """Test _resolve_env_var with and without a simple env_var reference."""

    result = _resolve_env_var("dbt_packages")
    assert result == "dbt_packages"

    result = _resolve_env_var('{{ env_var("MY_PATH") }}')
    assert result == "custom_packages"


@patch.dict(os.environ, {}, clear=False)
def test_resolve_env_var_with_default_value():
    """Test _resolve_env_var with env_var default when variable is not set."""
    # Ensure the variable is not set
    os.environ.pop("NONEXISTENT_VAR", None)
    result = _resolve_env_var('{{ env_var("NONEXISTENT_VAR", "default_path") }}')
    assert result == "default_path"


@patch.dict(os.environ, {"dbt_packages_suffix": "test"})
def test_resolve_env_var_with_complex_template():
    """Test _resolve_env_var with complex conditional templates."""
    template = 'dbt_packages{{ "_" + env_var("dbt_packages_suffix","") if env_var("dbt_packages_suffix","")!="" }}'
    result = _resolve_env_var(template)
    assert result == "dbt_packages_test"

    os.environ.pop("dbt_packages_suffix", None)
    template = 'dbt_packages{{ "_" + env_var("dbt_packages_suffix","") if env_var("dbt_packages_suffix","")!="" }}'
    result = _resolve_env_var(template)
    assert result == "dbt_packages"


@patch.dict(os.environ, {}, clear=False)
def test_resolve_env_var_with_complex_template_unset_var():
    """Test _resolve_env_var with a complex conditional template when variable is not set."""
    if "dbt_packages_suffix" in os.environ:
        del os.environ["dbt_packages_suffix"]
    template = 'dbt_packages{{ "_" + env_var("dbt_packages_suffix","") if env_var("dbt_packages_suffix","")!="" }}'
    result = _resolve_env_var(template)
    assert result == "dbt_packages"


@patch.dict(os.environ, {"ENV_SUFFIX": "prod"})
def test_get_dbt_packages_subpath_with_env_var_template(tmpdir):
    """Test get_dbt_packages_subpath with env_var in packages-install-path."""
    write_dbt_project_yml(tmpdir, {"packages-install-path": 'dbt_packages_{{ env_var("ENV_SUFFIX") }}'})
    result = get_dbt_packages_subpath(tmpdir)
    assert result == "dbt_packages_prod"


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


# https://github.com/astronomer/astronomer-cosmos/issues/1673
# dbt-core's plugin discovery imports every top-level module named ``dbt_*`` reachable from
# ``sys.path`` / ``PYTHONPATH``. Airflow puts its DAGs folder there, so a DAG file named ``dbt_*.py``
# gets imported as a side effect of Cosmos running dbt -- leaking/duplicating DAGs (in-process) or
# crashing the dbt command (subprocess). These tests cover the three helpers that keep the DAGs
# folder out of dbt's reach.


def test_resolve_dags_folder_returns_realpath(tmp_path):
    with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
        assert _resolve_dags_folder() == os.path.realpath(str(tmp_path))


def test_resolve_dags_folder_returns_none_when_unset():
    with patch("airflow.settings.DAGS_FOLDER", ""):
        assert _resolve_dags_folder() is None


def test_resolve_dags_folder_returns_none_when_airflow_settings_unimportable():
    with patch.dict(sys.modules, {"airflow.settings": None}):
        assert _resolve_dags_folder() is None


def test_exclude_dags_folder_from_sys_path_removes_and_restores(tmp_path):
    dags_folder = str(tmp_path)
    original_sys_path = list(sys.path)
    sys.path.insert(0, dags_folder)

    try:
        with patch("airflow.settings.DAGS_FOLDER", dags_folder):
            with exclude_dags_folder_from_sys_path():
                assert dags_folder not in sys.path
            assert dags_folder in sys.path
    finally:
        sys.path[:] = original_sys_path


def test_exclude_dags_folder_from_sys_path_restores_even_on_exception(tmp_path):
    dags_folder = str(tmp_path)
    original_sys_path = list(sys.path)
    sys.path.insert(0, dags_folder)

    try:
        with patch("airflow.settings.DAGS_FOLDER", dags_folder):
            with pytest.raises(ValueError):
                with exclude_dags_folder_from_sys_path():
                    assert dags_folder not in sys.path
                    raise ValueError("boom")
            assert dags_folder in sys.path
    finally:
        sys.path[:] = original_sys_path


def test_exclude_dags_folder_from_sys_path_normalizes_non_canonical_entry(tmp_path):
    """A ``sys.path`` entry that is equivalent to (but not string-identical to) ``DAGS_FOLDER`` --
    e.g. with a trailing slash or a non-canonical path segment -- must still be recognized and
    removed, since ``pkgutil.iter_modules`` would scan it regardless of exact spelling."""
    dags_folder = str(tmp_path)
    non_canonical_entry = dags_folder + os.sep
    original_sys_path = list(sys.path)
    sys.path.insert(0, non_canonical_entry)

    try:
        with patch("airflow.settings.DAGS_FOLDER", dags_folder):
            with exclude_dags_folder_from_sys_path():
                assert non_canonical_entry not in sys.path
            assert non_canonical_entry in sys.path
    finally:
        sys.path[:] = original_sys_path


def test_exclude_dags_folder_from_sys_path_preserves_order_with_multiple_entries(tmp_path):
    """When the DAGs folder appears more than once in sys.path (e.g. once via PYTHONPATH and once
    inserted directly), restoring must put each occurrence back at its original index rather than
    always at position 0, or it would reverse their relative order and could shadow unrelated
    entries sitting between them."""
    dags_folder = str(tmp_path)
    original_sys_path = list(sys.path)
    # Two occurrences of the DAGs folder, with an unrelated entry sitting between them.
    sys.path.insert(0, "/unrelated/entry")
    sys.path.insert(0, dags_folder)
    sys.path.insert(0, dags_folder)
    expected_sys_path = list(sys.path)

    try:
        with patch("airflow.settings.DAGS_FOLDER", dags_folder):
            with exclude_dags_folder_from_sys_path():
                assert dags_folder not in sys.path
            assert sys.path == expected_sys_path
    finally:
        sys.path[:] = original_sys_path


def test_exclude_dags_folder_from_sys_path_noop_when_folder_not_on_path(tmp_path):
    with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
        original_sys_path = list(sys.path)
        with exclude_dags_folder_from_sys_path():
            assert sys.path == original_sys_path
        assert sys.path == original_sys_path


def test_exclude_dags_folder_from_sys_path_noop_when_dags_folder_unresolvable(tmp_path):
    dags_folder = str(tmp_path)
    original_sys_path = list(sys.path)
    sys.path.insert(0, dags_folder)

    try:
        with patch("airflow.settings.DAGS_FOLDER", ""):
            with exclude_dags_folder_from_sys_path():
                assert dags_folder in sys.path
    finally:
        sys.path[:] = original_sys_path


def test_remove_dags_folder_from_pythonpath_strips_only_matching_entry(tmp_path):
    dags_folder = str(tmp_path)
    other_entry = "/usr/local/astronomer-cosmos"
    env = {"PYTHONPATH": os.pathsep.join([other_entry, dags_folder]), "OTHER_VAR": "unchanged"}

    with patch("airflow.settings.DAGS_FOLDER", dags_folder):
        result = remove_dags_folder_from_pythonpath(env)

    assert result["PYTHONPATH"] == other_entry
    assert result["OTHER_VAR"] == "unchanged"
    # the input env is not mutated in place
    assert env["PYTHONPATH"] == os.pathsep.join([other_entry, dags_folder])


def test_remove_dags_folder_from_pythonpath_normalizes_non_canonical_entry(tmp_path):
    dags_folder = str(tmp_path)
    non_canonical_entry = dags_folder + os.sep
    other_entry = "/usr/local/astronomer-cosmos"
    env = {"PYTHONPATH": os.pathsep.join([other_entry, non_canonical_entry])}

    with patch("airflow.settings.DAGS_FOLDER", dags_folder):
        result = remove_dags_folder_from_pythonpath(env)

    assert result["PYTHONPATH"] == other_entry


def test_remove_dags_folder_from_pythonpath_returns_unchanged_when_pythonpath_missing(tmp_path):
    env = {"OTHER_VAR": "value"}
    with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
        result = remove_dags_folder_from_pythonpath(env)
    assert result == env
    # always a copy, even on the no-op path, per the docstring's contract
    assert result is not env


def test_remove_dags_folder_from_pythonpath_returns_unchanged_when_dags_folder_unresolvable(tmp_path):
    env = {"PYTHONPATH": str(tmp_path)}
    with patch("airflow.settings.DAGS_FOLDER", ""):
        result = remove_dags_folder_from_pythonpath(env)
    assert result == env
    # always a copy, even on the no-op path, per the docstring's contract
    assert result is not env

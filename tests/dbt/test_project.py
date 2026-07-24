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


# Tests for the helpers that keep the Airflow DAGs folder out of dbt's plugin discovery (#1673).


def test_resolve_dags_folder_returns_realpath(tmp_path):
    with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
        assert _resolve_dags_folder() == os.path.realpath(str(tmp_path))


def test_resolve_dags_folder_returns_none_when_unset():
    with patch("airflow.settings.DAGS_FOLDER", ""):
        assert _resolve_dags_folder() is None


def test_resolve_dags_folder_returns_none_when_airflow_settings_unimportable():
    with patch.dict(sys.modules, {"airflow.settings": None}):
        assert _resolve_dags_folder() is None


def test_resolve_dags_folder_resolves_relative_path(tmp_path, monkeypatch):
    # Airflow only ``expanduser``s DAGS_FOLDER, so a user-configured relative path stays relative.
    # _resolve_dags_folder must still return an absolute realpath so it matches realpath'd path entries.
    (tmp_path / "my_dags").mkdir()
    monkeypatch.chdir(tmp_path)
    with patch("airflow.settings.DAGS_FOLDER", "my_dags"):
        assert _resolve_dags_folder() == os.path.realpath(tmp_path / "my_dags")


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


def test_exclude_dags_folder_from_sys_path_noop_when_flag_disabled(tmp_path):
    dags_folder = str(tmp_path)
    original_sys_path = list(sys.path)
    sys.path.insert(0, dags_folder)

    try:
        with (
            patch("airflow.settings.DAGS_FOLDER", dags_folder),
            patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", False),
        ):
            with exclude_dags_folder_from_sys_path():
                assert dags_folder in sys.path
    finally:
        sys.path[:] = original_sys_path


def test_remove_dags_folder_from_pythonpath_noop_when_flag_disabled(tmp_path):
    dags_folder = str(tmp_path)
    env = {"PYTHONPATH": dags_folder}
    with (
        patch("airflow.settings.DAGS_FOLDER", dags_folder),
        patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", False),
    ):
        result = remove_dags_folder_from_pythonpath(env)
    assert result["PYTHONPATH"] == dags_folder
    # still a copy, per the docstring's contract
    assert result is not env


def _prepare_dbt_leak_repro(tmp_path):
    """Set up a #1673 reproduction: a minimal non-connecting dbt project plus a ``dbt_*.py`` DAG file
    in a separate DAGs folder that records (via a sentinel file) whether it got imported.

    Returns ``(project_dir, dags_folder, sentinel, parse_command)``.
    """
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: probe\nprofile: probe\nconfig-version: 2\nversion: '1.0'\n")
    (project_dir / "profiles.yml").write_text(
        "probe:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: postgres\n"
        "      host: localhost\n"
        "      user: u\n"
        "      password: p\n"
        "      port: 5432\n"
        "      dbname: db\n"
        "      schema: public\n"
        "      threads: 1\n"
    )
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    sentinel = tmp_path / "imported.txt"
    (dags_folder / "dbt_victim.py").write_text(f"open({str(sentinel)!r}, 'w').close()\n")
    parse_command = ["dbt", "parse", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]
    return project_dir, dags_folder, sentinel, parse_command


def _reset_dbt_plugin_discovery_cache():
    """Reset dbt-core's process-global plugin-discovery caches so a subsequent in-process invocation
    re-runs discovery (dbt memoizes discovered ``dbt_*`` modules for the lifetime of the process)."""
    import dbt.plugins
    import dbt.plugins.manager as manager

    if hasattr(manager._get_dbt_modules, "cache_clear"):
        manager._get_dbt_modules.cache_clear()
    manager._MODULES_CACHE = None
    dbt.plugins.PLUGIN_MANAGER = None
    sys.modules.pop("dbt_victim", None)


def test_run_command_dbt_runner_excludes_dags_folder_dbt_module(tmp_path):
    """#1673, in-process ``InvocationMode.DBT_RUNNER`` path.

    Drives Cosmos's own ``cosmos.dbt.runner.run_command`` (which wraps the dbt invocation with
    :func:`exclude_dags_folder_from_sys_path`) against a real ``dbtRunner``. Toggling
    ``enable_dags_folder_exclusion_from_dbt`` shows the leak (disabled: dbt's plugin discovery imports
    the DAG file off ``sys.path``) and the fix (enabled: the DAGs folder is stripped for the invocation).
    """
    from cosmos.dbt import runner as dbt_runner

    project_dir, dags_folder, sentinel, cmd = _prepare_dbt_leak_repro(tmp_path)

    sys.path.insert(0, str(dags_folder))  # mimic Airflow putting the DAGs folder on sys.path
    try:
        # Exclusion disabled: dbt's in-process plugin discovery imports the DAG file.
        _reset_dbt_plugin_discovery_cache()
        with (
            patch("airflow.settings.DAGS_FOLDER", str(dags_folder)),
            patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", False),
        ):
            dbt_runner.run_command(command=cmd, env=dict(os.environ), cwd=str(project_dir))
        assert sentinel.exists(), "expected dbt plugin discovery to import the dbt_*.py DAG file"

        sentinel.unlink()

        # Exclusion enabled (default): run_command keeps the DAGs folder off sys.path for the invocation.
        _reset_dbt_plugin_discovery_cache()
        with (
            patch("airflow.settings.DAGS_FOLDER", str(dags_folder)),
            patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", True),
        ):
            dbt_runner.run_command(command=cmd, env=dict(os.environ), cwd=str(project_dir))
        assert not sentinel.exists(), "DAGs folder should have been kept off sys.path"
    finally:
        _reset_dbt_plugin_discovery_cache()
        if str(dags_folder) in sys.path:
            sys.path.remove(str(dags_folder))


def test_run_command_with_subprocess_excludes_dags_folder_dbt_module(tmp_path):
    """#1673, ``InvocationMode.SUBPROCESS`` path.

    Drives Cosmos's own ``cosmos.dbt.graph.run_command_with_subprocess`` (which sanitizes the env via
    :func:`remove_dags_folder_from_pythonpath`) against a real ``dbt`` subprocess. Toggling
    ``enable_dags_folder_exclusion_from_dbt`` shows the leak (disabled: the subprocess imports the DAG
    file off ``PYTHONPATH``) and the fix (enabled: the DAGs folder is stripped from ``PYTHONPATH``).
    """
    from cosmos.dbt.graph import run_command_with_subprocess

    project_dir, dags_folder, sentinel, cmd = _prepare_dbt_leak_repro(tmp_path)
    base_env = {**os.environ, "PYTHONPATH": str(dags_folder)}

    # Exclusion disabled: the dbt subprocess imports the DAG file via PYTHONPATH.
    with (
        patch("airflow.settings.DAGS_FOLDER", str(dags_folder)),
        patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", False),
    ):
        run_command_with_subprocess(cmd, tmp_dir=project_dir, env_vars=dict(base_env))
    assert sentinel.exists(), "expected the dbt subprocess to import the dbt_*.py DAG file"

    sentinel.unlink()

    # Exclusion enabled (default): the DAGs folder is stripped from the subprocess PYTHONPATH.
    with (
        patch("airflow.settings.DAGS_FOLDER", str(dags_folder)),
        patch("cosmos.settings.enable_dags_folder_exclusion_from_dbt", True),
    ):
        run_command_with_subprocess(cmd, tmp_dir=project_dir, env_vars=dict(base_env))
    assert not sentinel.exists(), "DAGs folder should have been kept off PYTHONPATH"

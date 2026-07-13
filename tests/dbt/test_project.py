import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from cosmos.constants import DBT_DEFAULT_PACKAGES_FOLDER, DBT_PROJECT_FILENAME, PACKAGE_LOCKFILE_YML
from cosmos.dbt.project import (
    _resolve_env_var,
    change_working_directory,
    compute_extra_paths_parent_depth,
    copy_dbt_packages,
    copy_manifest_file_if_exists,
    create_symlinks,
    create_symlinks_for_extra_paths,
    environ,
    get_dbt_packages_subpath,
    has_non_empty_dependencies_file,
    prepare_dbt_project_clone_dir,
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


def _make_extra_paths_layout(tmp_path):
    """
    workspace/{dbt_utils, monorepo/{dbt_sources, my_project}} — a project that references paths one and two
    levels above its root, exactly like ``model-paths: ["models", "../dbt_sources"]`` and
    ``packages.yml: - local: "../../dbt_utils"``.
    """
    base = tmp_path / "workspace"
    project = base / "monorepo" / "my_project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: my_project\n")

    shared_sources = base / "monorepo" / "dbt_sources"  # referenced as ../dbt_sources
    shared_sources.mkdir()
    (shared_sources / "source.sql").write_text("select 1")

    dbt_utils = base / "dbt_utils"  # referenced as ../../dbt_utils
    dbt_utils.mkdir()
    (dbt_utils / "dbt_project.yml").write_text("name: dbt_utils\n")

    return project, shared_sources, dbt_utils


def _clone_into(project, tmp_dir, extra_paths):
    """Mimic Cosmos: nest the clone, symlink the project's own files, then reproduce the extra paths."""
    clone_dir = prepare_dbt_project_clone_dir(tmp_dir, project, extra_paths)
    for child in os.listdir(project):
        os.symlink(project / child, clone_dir / child)
    create_symlinks_for_extra_paths(project, clone_dir, extra_paths)
    return clone_dir


def test_compute_extra_paths_parent_depth(tmp_path):
    project, _s, _u = _make_extra_paths_layout(tmp_path)
    assert compute_extra_paths_parent_depth(project, []) == 0
    assert compute_extra_paths_parent_depth(project, ["models"]) == 0  # inside the project
    assert compute_extra_paths_parent_depth(project, ["../dbt_sources"]) == 1
    assert compute_extra_paths_parent_depth(project, ["../dbt_sources", "../../dbt_utils"]) == 2


def test_prepare_dbt_project_clone_dir_no_extra_paths_uses_tmp_dir(tmp_path):
    """With no upward extra paths the clone dir is the temp dir itself (unchanged behaviour)."""
    project, _s, _u = _make_extra_paths_layout(tmp_path)
    assert prepare_dbt_project_clone_dir(tmp_path, project, []) == tmp_path


def test_prepare_dbt_project_clone_dir_nests_for_parent_references(tmp_path):
    """A ``../../`` reference nests the clone two levels below the temp dir."""
    project, _s, _u = _make_extra_paths_layout(tmp_path)
    clone = prepare_dbt_project_clone_dir(tmp_path, project, ["../../dbt_utils"])
    assert clone.is_dir()
    assert len(clone.relative_to(tmp_path).parts) == 2


def test_extra_paths_resolve_inside_temp_tree_without_leaking(tmp_path):
    """Both ``../`` and ``../../`` references resolve to the real content, from links kept inside the temp tree.

    This is the regression: an un-nested clone would materialise ``../../dbt_utils`` at the filesystem root and
    ``../dbt_sources`` in a shared ``/tmp`` sibling; nesting keeps every reproduced link inside the temp dir.
    """
    project, shared_sources, dbt_utils = _make_extra_paths_layout(tmp_path)
    extra = ["../dbt_sources", "../../dbt_utils"]

    tmp_dir = tmp_path / "clone_root"
    tmp_dir.mkdir()
    clone = _clone_into(project, tmp_dir, extra)

    # From the clone, the exact references dbt performs resolve to the original content.
    assert (clone / "../dbt_sources/source.sql").read_text() == "select 1"
    assert (clone / "../../dbt_utils/dbt_project.yml").read_text() == "name: dbt_utils\n"

    # The reproduced links live inside the temp tree (cleaned up with it), not at ``/`` or in a shared ``/tmp``.
    for ref in ("../dbt_sources", "../../dbt_utils"):
        link = Path(os.path.normpath(clone / ref))
        assert str(link).startswith(str(tmp_dir) + os.sep)

    # The user's real source directories are untouched.
    assert shared_sources.exists() and dbt_utils.exists()


def test_create_symlinks_for_extra_paths_inside_clone(tmp_path):
    """An extra path inside the project is linked in place; depth stays 0 so the clone is not nested."""
    project, _shared_sources, _dbt_utils = _make_extra_paths_layout(tmp_path)
    (project / "macros").mkdir()

    tmp_dir = tmp_path / "clone_root"
    tmp_dir.mkdir()
    clone = _clone_into(project, tmp_dir, ["macros"])

    assert clone == tmp_dir
    assert (clone / "macros").is_symlink()


def test_create_symlinks_for_extra_paths_skips_missing(tmp_path, caplog):
    """A non-existent extra path is skipped with a warning rather than raising."""
    project, _shared_sources, _dbt_utils = _make_extra_paths_layout(tmp_path)
    tmp_dir = tmp_path / "clone_root"
    tmp_dir.mkdir()
    clone = prepare_dbt_project_clone_dir(tmp_dir, project, ["../does_not_exist"])

    assert create_symlinks_for_extra_paths(project, clone, ["../does_not_exist"]) is None
    assert "does not exist" in caplog.text


def test_create_symlinks_for_extra_paths_is_idempotent(tmp_path):
    """Re-running against an already-linked destination reuses it without error."""
    project, shared_sources, _dbt_utils = _make_extra_paths_layout(tmp_path)
    tmp_dir = tmp_path / "clone_root"
    tmp_dir.mkdir()
    clone = prepare_dbt_project_clone_dir(tmp_dir, project, ["../dbt_sources"])

    create_symlinks_for_extra_paths(project, clone, ["../dbt_sources"])
    create_symlinks_for_extra_paths(project, clone, ["../dbt_sources"])  # must not raise

    assert (clone / "../dbt_sources/source.sql").read_text() == "select 1"


def test_create_symlinks_for_extra_paths_conflicting_destination_is_skipped(tmp_path, caplog):
    """A pre-existing, unrelated destination is left untouched and skipped with a warning."""
    project, _shared_sources, _dbt_utils = _make_extra_paths_layout(tmp_path)
    tmp_dir = tmp_path / "clone_root"
    tmp_dir.mkdir()
    clone = prepare_dbt_project_clone_dir(tmp_dir, project, ["../dbt_sources"])

    conflict = Path(os.path.normpath(clone / "../dbt_sources"))
    conflict.mkdir(parents=True)
    (conflict / "unrelated.txt").write_text("keep me")

    create_symlinks_for_extra_paths(project, clone, ["../dbt_sources"])

    assert "already exists" in caplog.text
    assert (conflict / "unrelated.txt").read_text() == "keep me"


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

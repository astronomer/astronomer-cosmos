from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig, CosmosConfigException
from cosmos.exceptions import CosmosValueError


DBT_PROJECTS_ROOT_DIR = Path(__file__).parent / "sample/"
PIPELINE_FOLDER = "jaffle_shop"


def test_init_with_project_path_only():
    """
    Passing dbt_project_path on its own should create a valid ProjectConfig with relative paths defined
    It should also have a project_name based on the path
    """
    project_config = ProjectConfig(dbt_project_path="path/to/dbt/project")
    assert project_config.dbt_project_path == Path("path/to/dbt/project")
    assert project_config.models_path == Path("path/to/dbt/project/models")
    assert project_config.seeds_path == Path("path/to/dbt/project/seeds")
    assert project_config.snapshots_path == Path("path/to/dbt/project/snapshots")
    assert project_config.project_name == "project"
    assert project_config.manifest_path is None


def test_init_with_manifest_path_and_project_path_succeeds():
    """
    Passing a manifest path AND project path together should succeed
    project_name in this case should be based on dbt_project_path
    """
    project_config = ProjectConfig(dbt_project_path="/tmp/some-path", manifest_path="target/manifest.json")
    assert project_config.manifest_path == Path("target/manifest.json")
    assert project_config.project_name == "some-path"


def test_init_with_no_params():
    """
    With the implementation of dbt_project_path in RenderConfig and ExecutionConfig
    dbt_project_path becomes optional here. The only requirement is that if one of
    manifest_path or project_name is defined, they should both be defined.
    We used to enforce dbt_project_path or manifest_path and project_name, but this is
    No longer the case
    """
    project_config = ProjectConfig()
    assert project_config


def test_init_with_manifest_path_and_not_project_path_and_not_project_name_fails():
    """
    Passing a manifest alone should fail since we also require a project_name
    """
    with pytest.raises(CosmosValueError) as err_info:
        ProjectConfig(manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json")
    assert err_info.value.args[0] == (
        "If ProjectConfig.dbt_project_path is not defined, ProjectConfig.manifest_path and ProjectConfig.project_name must be defined together, or both left undefined."
    )


def test_validate_with_project_path_and_manifest_path_succeeds():
    """
    Supplying both project and manifest paths as previous should be permitted
    """
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR, manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json"
    )
    assert project_config.validate_project() is None


def test_validate_with_project_path_and_not_manifest_path_succeeds():
    """
    Passing a project with no manifest should be permitted
    """
    project_config = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR)
    assert project_config.validate_project() is None


def test_validate_with_manifest_path_and_project_name_and_not_project_path_succeeds():
    """
    Passing a manifest and project name together should succeed.
    """
    project_config = ProjectConfig(manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json", project_name="test-project")
    assert project_config.validate_project() is None


def test_validate_project_missing_fails():
    """
    Passing a project dir that does not exist where specified should fail
    """
    project_config = ProjectConfig(dbt_project_path=Path("/tmp"))
    with pytest.raises(CosmosValueError) as err_info:
        assert project_config.validate_project() is None
    assert err_info.value.args[0] == "Could not find dbt_project.yml at /tmp/dbt_project.yml"


def test_is_manifest_available_is_true():
    dbt_project = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR, manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json"
    )
    assert dbt_project.is_manifest_available()


def test_is_manifest_available_is_false():
    dbt_project = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR)
    assert not dbt_project.is_manifest_available()


def test_project_name():
    dbt_project = ProjectConfig(dbt_project_path=DBT_PROJECTS_ROOT_DIR)
    assert dbt_project.project_name == "sample"


def test_profile_config_post_init():
    with pytest.raises(CosmosValueError) as err_info:
        ProfileConfig(profiles_yml_filepath="/tmp/some-profile", profile_name="test", target_name="test")
    assert err_info.value.args[0] == "The file /tmp/some-profile does not exist."


def test_profile_config_validate():
    with pytest.raises(CosmosValueError) as err_info:
        profile_config = ProfileConfig(profile_name="test", target_name="test")
        assert profile_config.validate_profile() is None
    assert err_info.value.args[0] == "Either profiles_yml_filepath or profile_mapping must be set to render a profile"


@patch("cosmos.config.shutil.which", return_value=None)
def test_render_config_without_dbt_cmd(mock_which):
    render_config = RenderConfig()
    with pytest.raises(CosmosConfigException) as err_info:
        render_config.validate_dbt_command("inexistent-dbt")

    error_msg = err_info.value.args[0]
    assert error_msg.startswith("Unable to find the dbt executable, attempted: <")
    assert error_msg.endswith("dbt> and <inexistent-dbt>.")


@patch("cosmos.config.shutil.which", return_value=None)
def test_render_config_with_invalid_dbt_commands(mock_which):
    render_config = RenderConfig(dbt_executable_path="invalid-dbt")
    with pytest.raises(CosmosConfigException) as err_info:
        render_config.validate_dbt_command()

    error_msg = err_info.value.args[0]
    assert error_msg == "Unable to find the dbt executable, attempted: <invalid-dbt>."


@patch("cosmos.config.shutil.which", side_effect=(None, "fallback-dbt-path"))
def test_render_config_uses_fallback_if_default_not_found(mock_which):
    render_config = RenderConfig()
    render_config.validate_dbt_command(Path("/tmp/fallback-dbt-path"))
    assert render_config.dbt_executable_path == "/tmp/fallback-dbt-path"


@patch("cosmos.config.shutil.which", side_effect=("user-dbt", "fallback-dbt-path"))
def test_render_config_uses_default_if_exists(mock_which):
    render_config = RenderConfig(dbt_executable_path="user-dbt")
    render_config.validate_dbt_command("fallback-dbt-path")
    assert render_config.dbt_executable_path == "user-dbt"

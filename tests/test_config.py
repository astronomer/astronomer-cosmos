from pathlib import Path

import pytest

from cosmos.config import ProjectConfig
from cosmos.exceptions import CosmosValueError


DBT_PROJECTS_ROOT_DIR = Path(__file__).parent / "sample/"
PIPELINE_FOLDER = "jaffle_shop"


def test_valid_parameters():
    project_config = ProjectConfig(dbt_project_path="path/to/dbt/project")
    assert project_config.parsed_dbt_project_path == Path("path/to/dbt/project")
    assert project_config.models_relative_path == Path("path/to/dbt/project/models")
    assert project_config.seeds_relative_path == Path("path/to/dbt/project/seeds")
    assert project_config.snapshots_relative_path == Path("path/to/dbt/project/snapshots")
    assert project_config.manifest_path is None


def test_init_with_manifest_path_and_project_path_succeeds():
    """
    Passing a manifest path AND project path together should succeed, as previous
    """
    project_config = ProjectConfig(dbt_project_path="/tmp/some-path", manifest_path="target/manifest.json")
    assert project_config.parsed_manifest_path == Path("target/manifest.json")


def test_init_with_manifest_path_and_not_project_path_succeeds():
    """
    Since dbt_project_path is optional, we should be able to operate with only a manifest
    """
    project_config = ProjectConfig(manifest_path="target/manifest.json")
    assert project_config.parsed_manifest_path == Path("target/manifest.json")


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


def test_validate_with_manifest_path_and_not_project_path_and_not_project_name_fails():
    """
    Passing a manifest alone should fail since we also require a project_name
    """
    project_config = ProjectConfig(manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json")
    with pytest.raises(CosmosValueError) as err_info:
        assert project_config.validate_project() is None
    print(err_info.value.args[0])
    assert err_info.value.args[0] == "project_name required when manifest_path is present and dbt_project_path is not."


def test_validate_with_manifest_path_and_project_name_and_not_project_path_succeeds():
    """
    Passing a manifest and project name together should succeed.
    """
    project_config = ProjectConfig(manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json", project_name="test-project")
    assert project_config.validate_project() is None


def test_validate_no_paths_fails():
    """
    Passing no manifest and no project directory should fail.
    """
    project_config = ProjectConfig()
    with pytest.raises(CosmosValueError) as err_info:
        assert project_config.validate_project() is None
    assert err_info.value.args[0] == "dbt_project_path or manifest_path are required parameters."


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

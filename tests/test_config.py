from pathlib import Path

import pytest

from cosmos.config import ProjectConfig
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
    The constructor now validates that the required base fields are present
    As such, we should test here that the correct exception is raised if these are not correctly defined
    This functionality has been moved from the validate method
    """
    with pytest.raises(CosmosValueError) as err_info:
        ProjectConfig()
        print(err_info.value.args[0])
        assert err_info.value.args[0] == (
            "ProjectConfig requires dbt_project_path and/or manifest_path to be defined."
            "If only manifest_path is defined, project_name must also be defined."
        )


def test_init_with_manifest_path_and_not_project_path_and_not_project_name_fails():
    """
    Passing a manifest alone should fail since we also require a project_name
    """
    with pytest.raises(CosmosValueError) as err_info:
        ProjectConfig(manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json")
        print(err_info.value.args[0])
        assert err_info.value.args[0] == (
            "ProjectConfig requires dbt_project_path and/or manifest_path to be defined."
            "If only manifest_path is defined, project_name must also be defined."
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

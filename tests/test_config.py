from pathlib import Path

import pytest

from cosmos.config import ProjectConfig
from cosmos.exceptions import CosmosValueError


DBT_PROJECTS_ROOT_DIR = Path(__file__).parent / "sample/"
PIPELINE_FOLDER = "jaffle_shop"


# Tests that a ProjectConfig object can be created with valid parameters
def test_valid_parameters():
    project_config = ProjectConfig(dbt_project_path="path/to/dbt/project")
    assert project_config.parsed_dbt_project_path == Path("path/to/dbt/project")
    assert project_config.models_relative_path == Path("path/to/dbt/project/models")
    assert project_config.seeds_relative_path == Path("path/to/dbt/project/seeds")
    assert project_config.snapshots_relative_path == Path("path/to/dbt/project/snapshots")
    assert project_config.manifest_path is None


# Since dbt_project_path is now an optional parameter, we should test each combination for init and validation

# Passing a manifest AND project together should succeed, as previous
def test_init_with_manifest_and_project():
    project_config = ProjectConfig(dbt_project_path="/tmp/some-path", manifest_path="target/manifest.json")
    assert project_config.parsed_manifest_path == Path("target/manifest.json")

# Since dbt_project_path is optional, we should be able to operate with only a manifest
def test_init_with_manifest_and_not_project():
    project_config = ProjectConfig(manifest_path="target/manifest.json")
    assert project_config.parsed_manifest_path == Path("target/manifest.json")

# supplying both project and manifest paths as previous should be permitted
def test_validate_project_success_project_and_manifest():
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR, manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json"
    )
    assert project_config.validate_project() is None

# with updated logic, passing a project alone should be permitted
def test_validate_project_success_project_and_not_manifest():
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECTS_ROOT_DIR
    )
    assert project_config.validate_project() is None

# with updated logic, passing a manifest alone should fail since we also require a project_name
def test_validate_project_failure_not_project_and_manifest():
    project_config = ProjectConfig(
        manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json"
    )
    with pytest.raises(CosmosValueError) as err_info:
        assert project_config.validate_project() is None
    print(err_info.value.args[0])
    assert err_info.value.args[0] == "A required project field was not present - project_name must be provided when manifest_path is provided and dbt_project_path is not."

# with updated logic, passing a manifest and project name together should succeed.
def test_validate_project_success_not_project_and_manifest():
    project_config = ProjectConfig(
        manifest_path=DBT_PROJECTS_ROOT_DIR / "manifest.json",
        project_name="test-project"
    )
    assert project_config.validate_project() is None


def test_validate_project_fails():
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

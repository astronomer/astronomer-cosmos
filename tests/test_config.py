from pathlib import Path

import pytest

from cosmos.config import ProjectConfig
from cosmos.exceptions import CosmosValueError


DBT_PROJECTS_ROOT_DIR = Path(__file__).parent / "sample/"
PIPELINE_FOLDER = "jaffle_shop"


# Tests that a ProjectConfig object can be created with valid parameters
def test_valid_parameters():
    project_config = ProjectConfig(dbt_project="path/to/dbt/project")
    assert project_config.dbt_project_path == Path("path/to/dbt/project")
    assert project_config.models_path == Path("path/to/dbt/project/models")
    assert project_config.seeds_path == Path("path/to/dbt/project/seeds")
    assert project_config.snapshots_path == Path("path/to/dbt/project/snapshots")
    assert project_config.manifest_path is None


def test_init_with_manifest():
    project_config = ProjectConfig(dbt_project="/tmp/some-path", manifest="target/manifest.json")
    assert project_config.manifest_path == Path("target/manifest.json")


def test_validate_project_succeeds():
    project_config = ProjectConfig(dbt_project=DBT_PROJECTS_ROOT_DIR, manifest=DBT_PROJECTS_ROOT_DIR / "manifest.json")
    assert project_config.validate_project() is None


def test_validate_project_fails():
    project_config = ProjectConfig(dbt_project=Path("/tmp"))
    with pytest.raises(CosmosValueError) as err_info:
        assert project_config.validate_project() is None
    assert err_info.value.args[0] == "Could not find dbt_project.yml at /tmp/dbt_project.yml"

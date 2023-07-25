from pathlib import Path

from cosmos.config import ProjectConfig


# Tests that a ProjectConfig object can be created with valid parameters
def test_valid_parameters():
    project_config = ProjectConfig(dbt_project="path/to/dbt/project")
    assert project_config.dbt_project_path == Path("path/to/dbt/project")
    assert project_config.models_path == Path("path/to/dbt/project/models")
    assert project_config.seeds_path == Path("path/to/dbt/project/seeds")
    assert project_config.snapshots_path == Path("path/to/dbt/project/snapshots")
    assert project_config.manifest_path is None

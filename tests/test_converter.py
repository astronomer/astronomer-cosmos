from pathlib import Path

from unittest.mock import patch
import pytest

from cosmos.converter import DbtToAirflowConverter, validate_arguments
from cosmos.constants import DbtResourceType, ExecutionMode
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.dbt.graph import DbtNode
from cosmos.exceptions import CosmosValueError


SAMPLE_PROFILE_YML = Path(__file__).parent / "sample/profiles.yml"
SAMPLE_DBT_PROJECT = Path(__file__).parent / "sample/"


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    selector_name = argument_key[:-1]
    select = [f"{selector_name}:a,{selector_name}:b"]
    exclude = [f"{selector_name}:b,{selector_name}:c"]
    profile_args = {}
    task_args = {}
    with pytest.raises(CosmosValueError) as err:
        validate_arguments(select, exclude, profile_args, task_args)
    expected = f"Can't specify the same {selector_name} in `select` and `exclude`: {{'b'}}"
    assert err.value.args[0] == expected


parent_seed = DbtNode(
    name="seed_parent",
    unique_id="seed_parent",
    resource_type=DbtResourceType.SEED,
    depends_on=[],
    file_path="",
)
nodes = {"seed_parent": parent_seed}

@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
        # (ExecutionMode.DOCKER, {"image": "sample-image"}),
    ]
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_creates_dag_with_seed(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test will raise exceptions if we are trying to pass incorrect arguments to operator constructors.
    """
    project_config = ProjectConfig(
        dbt_project_path=SAMPLE_DBT_PROJECT
    )
    execution_config = ExecutionConfig(
        execution_mode=execution_mode
    )
    render_config = RenderConfig(
        emit_datasets=True
    )
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    converter = DbtToAirflowConverter(
        nodes=nodes,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args=operator_args
    )
    assert converter

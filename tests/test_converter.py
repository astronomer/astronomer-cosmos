from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
import logging

import pytest
from airflow.models import DAG

from cosmos.converter import DbtToAirflowConverter, validate_arguments, validate_initial_user_config
from cosmos.constants import DbtResourceType, ExecutionMode, LoadMode
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, CosmosConfigException
from cosmos.dbt.graph import DbtNode
from cosmos.exceptions import CosmosValueError


SAMPLE_PROFILE_YML = Path(__file__).parent / "sample/profiles.yml"
SAMPLE_DBT_PROJECT = Path(__file__).parent / "sample/"
SAMPLE_DBT_MANIFEST = Path(__file__).parent / "sample/manifest.json"
LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    selector_name = argument_key[:-1]
    select = [f"{selector_name}:a,{selector_name}:b"]
    exclude = [f"{selector_name}:b,{selector_name}:c"]
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    task_args = {}
    with pytest.raises(CosmosValueError) as err:
        validate_arguments(select, exclude, profile_config, task_args, execution_mode=ExecutionMode.LOCAL)
    expected = f"Can't specify the same {selector_name} in `select` and `exclude`: {{'b'}}"
    assert err.value.args[0] == expected


@pytest.mark.parametrize(
    "execution_mode",
    (ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV),
)
def test_validate_initial_user_config_no_profile(execution_mode):
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    profile_config = None
    project_config = ProjectConfig()
    with pytest.raises(CosmosValueError) as err_info:
        validate_initial_user_config(execution_config, profile_config, project_config, None, {})
    err_msg = f"The profile_config is mandatory when using {execution_mode}"
    assert err_info.value.args[0] == err_msg


@pytest.mark.parametrize(
    "execution_mode",
    (ExecutionMode.DOCKER, ExecutionMode.KUBERNETES),
)
def test_validate_initial_user_config_expects_profile(execution_mode):
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    profile_config = None
    project_config = ProjectConfig()
    assert validate_initial_user_config(execution_config, profile_config, project_config, None, {}) is None


@pytest.mark.parametrize("operator_args", [{"env": {"key": "value"}}, {"vars": {"key": "value"}}])
def test_validate_user_config_operator_args_deprecated(operator_args):
    """Deprecating warnings should be raised when using operator_args with "vars" or "env"."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig()
    profile_config = MagicMock()

    with pytest.deprecated_call():
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


@pytest.mark.parametrize("project_config_arg, operator_arg", [("dbt_vars", "vars"), ("env_vars", "env")])
def test_validate_user_config_fails_project_config_and_operator_args_overlap(project_config_arg, operator_arg):
    """
    The validation should fail if a user specifies both a ProjectConfig and operator_args with dbt_vars/vars or env_vars/env
    that overlap.
    """
    project_config = ProjectConfig(
        project_name="fake-project",
        dbt_project_path="/some/project/path",
        **{project_config_arg: {"key": "value"}},  # type: ignore
    )
    execution_config = ExecutionConfig()
    render_config = RenderConfig()
    profile_config = MagicMock()
    operator_args = {operator_arg: {"key": "value"}}

    expected_error_msg = f"ProjectConfig.{project_config_arg} and operator_args with '{operator_arg}' are mutually exclusive and only one can be used."
    with pytest.raises(CosmosValueError, match=expected_error_msg):
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


def test_validate_user_config_fails_project_config_render_config_env_vars():
    """
    The validation should fail if a user specifies both ProjectConfig.env_vars and RenderConfig.env_vars.
    """
    project_config = ProjectConfig(env_vars={"key": "value"})
    execution_config = ExecutionConfig()
    render_config = RenderConfig(env_vars={"key": "value"})
    profile_config = MagicMock()
    operator_args = {}

    expected_error_match = "Both ProjectConfig.env_vars and RenderConfig.env_vars were provided.*"
    with pytest.raises(CosmosValueError, match=expected_error_match):
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


def test_validate_arguments_schema_in_task_args():
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    task_args = {"schema": "abcd"}
    validate_arguments(
        select=[], exclude=[], profile_config=profile_config, task_args=task_args, execution_mode=ExecutionMode.LOCAL
    )
    assert profile_config.profile_mapping.profile_args["schema"] == "abcd"


parent_seed = DbtNode(
    unique_id=f"{DbtResourceType.SEED}.{SAMPLE_DBT_PROJECT.stem}.seed_parent",
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
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_creates_dag_with_seed(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test validates that a project, given only a project path as a Path() Object, and seeds
    is able to successfully generate a converter
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    render_config = RenderConfig(emit_datasets=True)
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
        operator_args=operator_args,
    )
    assert converter


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
    ]
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_creates_dag_with_project_path_str(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test validates that a project, given only a project path as a string, and seeds
    is able to successfully generate a converter
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT.as_posix())
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    render_config = RenderConfig(emit_datasets=True)
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
        operator_args=operator_args,
    )
    assert converter

@pytest.mark.parametrize(
    "execution_mode,virtualenv_dir,operator_args",
    [
        (ExecutionMode.KUBERNETES, Path("/some/virtualenv/dir"), {}),
        # (ExecutionMode.DOCKER, {"image": "sample-image"}),
    ],
)
def test_converter_raises_warning(mock_load_dbt_graph, execution_mode, virtualenv_dir, operator_args, caplog):
    """
    This test will raise a warning if we are trying to pass ExecutionMode != `VirtualEnv` andm still pass a defined `virtualenv_dir`
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=execution_mode, virtualenv_dir=virtualenv_dir)
    render_config = RenderConfig(emit_datasets=True)
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
        operator_args=operator_args,
    )

    assert converter

    assert "`ExecutionConfig.virtualenv_dir` is only supported when \
                ExecutionConfig.execution_mode is set to ExecutionMode.VIRTUALENV." in caplog.text



@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
        # (ExecutionMode.DOCKER, {"image": "sample-image"}),
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_fails_execution_config_no_project_dir(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test validates that a project, given a manifest path and project name, with seeds
    is able to successfully generate a converter
    """
    project_config = ProjectConfig(manifest_path=SAMPLE_DBT_MANIFEST.as_posix(), project_name="sample")
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    render_config = RenderConfig(emit_datasets=True)
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    with pytest.raises(CosmosValueError) as err_info:
        DbtToAirflowConverter(
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args=operator_args,
        )
    assert (
        err_info.value.args[0]
        == "ExecutionConfig.dbt_project_path is required for the execution of dbt tasks in all execution modes."
    )


def test_converter_fails_render_config_invalid_dbt_path_with_dbt_ls():
    """
    Validate that a dbt project fails to be rendered to Airflow with DBT_LS if
    the dbt command is invalid.
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT.as_posix(), project_name="sample")
    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path="invalid-execution-dbt",
    )
    render_config = RenderConfig(
        emit_datasets=True,
        dbt_executable_path="invalid-render-dbt",
    )
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    with pytest.raises(CosmosConfigException) as err_info:
        with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                nodes=nodes,
                project_config=project_config,
                profile_config=profile_config,
                execution_config=execution_config,
                render_config=render_config,
            )
    assert (
        err_info.value.args[0]
        == "Unable to find the dbt executable, attempted: <invalid-render-dbt> and <invalid-execution-dbt>."
    )


def test_converter_fails_render_config_invalid_dbt_path_with_manifest():
    """
    Validate that a dbt project succeeds to be rendered to Airflow with DBT_MANIFEST even when
    the dbt command is invalid.
    """
    project_config = ProjectConfig(manifest_path=SAMPLE_DBT_MANIFEST.as_posix(), project_name="sample")

    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path="invalid-execution-dbt",
        dbt_project_path=SAMPLE_DBT_PROJECT.as_posix(),
    )
    render_config = RenderConfig(
        emit_datasets=True,
        dbt_executable_path="invalid-render-dbt",
    )
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    assert converter


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
        # (ExecutionMode.DOCKER, {"image": "sample-image"}),
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_fails_project_config_path_and_execution_config_path(
    mock_load_dbt_graph, execution_mode, operator_args
):
    """
    This test ensures that we fail if we defined project path in ProjectConfig and ExecutionConfig
    They are mutually exclusive, so this should be allowed.
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT.as_posix())
    execution_config = ExecutionConfig(execution_mode=execution_mode, dbt_project_path=SAMPLE_DBT_PROJECT.as_posix())
    render_config = RenderConfig(emit_datasets=True)
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    with pytest.raises(CosmosValueError) as err_info:
        DbtToAirflowConverter(
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args=operator_args,
        )
    assert (
        err_info.value.args[0]
        == "ProjectConfig.dbt_project_path is mutually exclusive with RenderConfig.dbt_project_path and ExecutionConfig.dbt_project_path.If using RenderConfig.dbt_project_path or ExecutionConfig.dbt_project_path, ProjectConfig.dbt_project_path should be None"
    )


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
        # (ExecutionMode.DOCKER, {"image": "sample-image"}),
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_fails_no_manifest_no_render_config(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test ensures that we fail if we define project path in ProjectConfig and ExecutionConfig
    They are mutually exclusive, so this should be allowed.
    """
    project_config = ProjectConfig()
    execution_config = ExecutionConfig(execution_mode=execution_mode, dbt_project_path=SAMPLE_DBT_PROJECT.as_posix())
    render_config = RenderConfig(emit_datasets=True)
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    with pytest.raises(CosmosValueError) as err_info:
        DbtToAirflowConverter(
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args=operator_args,
        )
    assert (
        err_info.value.args[0]
        == "RenderConfig.dbt_project_path is required for rendering an airflow DAG from a DBT Graph if no manifest is provided."
    )


@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.build_airflow_graph")
@patch("cosmos.dbt.graph.LegacyDbtProject")
def test_converter_project_config_dbt_vars_with_custom_load_mode(
    mock_legacy_dbt_project, mock_validate_project, mock_build_airflow_graph
):
    """Tests that if ProjectConfig.dbt_vars are used with RenderConfig.load_method of "custom" that the
    expected dbt_vars are passed to LegacyDbtProject.
    """
    project_config = ProjectConfig(
        project_name="fake-project", dbt_project_path="/some/project/path", dbt_vars={"key": "value"}
    )
    execution_config = ExecutionConfig()
    render_config = RenderConfig(load_method=LoadMode.CUSTOM)
    profile_config = MagicMock()

    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args={},
        )
    _, kwargs = mock_legacy_dbt_project.call_args
    assert kwargs["dbt_vars"] == {"key": "value"}

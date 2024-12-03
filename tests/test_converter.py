import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DAG

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DbtResourceType, ExecutionMode, InvocationMode, LoadMode
from cosmos.converter import DbtToAirflowConverter, validate_arguments, validate_initial_user_config
from cosmos.dbt.graph import DbtGraph, DbtNode
from cosmos.exceptions import CosmosValueError
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

SAMPLE_PROFILE_YML = Path(__file__).parent / "sample/profiles.yml"
SAMPLE_DBT_PROJECT = Path(__file__).parent / "sample/"
SAMPLE_DBT_MANIFEST = Path(__file__).parent / "sample/manifest.json"


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
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
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
    ],
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
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
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
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_raises_warning(mock_load_dbt_graph, execution_mode, virtualenv_dir, operator_args, caplog):
    """
    This test will raise a warning if we are trying to pass ExecutionMode != `VirtualEnv`
    and still pass a defined `virtualenv_dir`
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT.as_posix())
    execution_config = ExecutionConfig(execution_mode=execution_mode, virtualenv_dir=virtualenv_dir)
    render_config = RenderConfig(emit_datasets=True)
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )

    DbtToAirflowConverter(
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        nodes=nodes,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args=operator_args,
    )

    assert (
        "`ExecutionConfig.virtualenv_dir` is only supported when \
                ExecutionConfig.execution_mode is set to ExecutionMode.VIRTUALENV."
        in caplog.text
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


@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.build_airflow_graph")
@patch("cosmos.converter.DbtGraph.load")
def test_converter_multiple_calls_same_operator_args(
    mock_dbt_graph_load, mock_validate_project, mock_build_airflow_graph
):
    """Tests if the DbttoAirflowConverter is called more than once with the same operator_args, the
    operator_args are not modified.
    """
    project_config = ProjectConfig(project_name="fake-project", dbt_project_path="/some/project/path")
    execution_config = ExecutionConfig()
    render_config = RenderConfig()
    profile_config = MagicMock()
    operator_args = {
        "install_deps": True,
        "vars": {"key": "value"},
        "env": {"key": "value"},
    }
    original_operator_args = operator_args.copy()
    for _ in range(2):
        with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                nodes=nodes,
                project_config=project_config,
                profile_config=profile_config,
                execution_config=execution_config,
                render_config=render_config,
                operator_args=operator_args,
            )
    assert operator_args == original_operator_args


@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.build_airflow_graph")
@patch("cosmos.converter.DbtGraph.load")
def test_validate_converter_fetches_project_name_from_render_config(
    mock_dbt_graph_load, mock_build_airflow_graph, mock_validate_project
):
    """
    Allow DbtToAirflowConverter to work when using:
     - RenderMode.DBT_LS
     - ExecutionConfig(dbt_project_path)
     - RenderConfig(dbt_project_path)
    In other words, when ProjectConfig does not contain name.

    This scenario can be useful when using ExecutionMode.KUBERNETES or other similar ones and was found out during:
    https://github.com/astronomer/astronomer-cosmos/pull/1297
    """
    execution_config = ExecutionConfig(dbt_project_path="/data/project1")
    project_config = ProjectConfig()
    profile_config = MagicMock()
    render_config = RenderConfig(dbt_project_path="/home/usr/airflow/project1")

    with DAG("test-id", start_date=datetime(2022, 1, 1)) as dag:
        DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )

    mock_build_airflow_graph.assert_called_once()
    assert mock_build_airflow_graph.call_args.kwargs["dbt_project_name"] == "project1"


@pytest.mark.parametrize("invocation_mode", [None, InvocationMode.SUBPROCESS, InvocationMode.DBT_RUNNER])
@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.validate_initial_user_config")
@patch("cosmos.converter.DbtGraph")
@patch("cosmos.converter.build_airflow_graph")
def test_converter_invocation_mode_added_to_task_args(
    mock_build_airflow_graph, mock_user_config, mock_dbt_graph, mock_validate_project, invocation_mode
):
    """Tests that the `task_args` passed to build_airflow_graph has invocation_mode if it is not None."""
    project_config = ProjectConfig(project_name="fake-project", dbt_project_path="/some/project/path")
    execution_config = ExecutionConfig(invocation_mode=invocation_mode)
    render_config = MagicMock()
    profile_config = MagicMock()

    with DAG("test-id", start_date=datetime(2024, 1, 1)) as dag:
        DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args={},
        )
    _, kwargs = mock_build_airflow_graph.call_args
    if invocation_mode:
        assert kwargs["task_args"]["invocation_mode"] == invocation_mode
    else:
        assert "invocation_mode" not in kwargs["task_args"]


@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.validate_initial_user_config")
@patch("cosmos.converter.DbtGraph")
@patch("cosmos.converter.build_airflow_graph")
def test_converter_uses_cache_dir(
    mock_build_airflow_graph,
    mock_dbt_graph,
    mock_user_config,
    mock_validate_project,
):
    """Tests that DbtGraph and operator and Airflow task args contain expected cache dir ."""
    project_config = ProjectConfig(project_name="fake-project", dbt_project_path="/some/project/path")
    execution_config = ExecutionConfig()
    render_config = RenderConfig(enable_mock_profile=False)
    profile_config = MagicMock()

    with DAG("test-id", start_date=datetime(2024, 1, 1)) as dag:
        DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args={},
        )
    task_args_cache_dir = mock_build_airflow_graph.call_args[1]["task_args"]["cache_dir"]
    dbt_graph_cache_dir = mock_dbt_graph.call_args[1]["cache_dir"]

    assert Path(tempfile.gettempdir()) in task_args_cache_dir.parents
    assert task_args_cache_dir.parent.stem == "cosmos"
    assert task_args_cache_dir.stem == "test-id"
    assert task_args_cache_dir == dbt_graph_cache_dir


@patch("cosmos.settings.enable_cache", False)
@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.validate_initial_user_config")
@patch("cosmos.converter.DbtGraph")
@patch("cosmos.converter.build_airflow_graph")
def test_converter_disable_cache_sets_cache_dir_to_none(
    mock_build_airflow_graph,
    mock_dbt_graph,
    mock_user_config,
    mock_validate_project,
):
    """Tests that DbtGraph and operator and Airflow task args contain expected cache dir."""
    project_config = ProjectConfig(project_name="fake-project", dbt_project_path="/some/project/path")
    execution_config = ExecutionConfig()
    render_config = RenderConfig(enable_mock_profile=False)
    profile_config = MagicMock()

    with DAG("test-id", start_date=datetime(2024, 1, 1)) as dag:
        DbtToAirflowConverter(
            dag=dag,
            nodes=nodes,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
            operator_args={},
        )
    task_args_cache_dir = mock_build_airflow_graph.call_args[1]["task_args"]["cache_dir"]
    dbt_graph_cache_dir = mock_dbt_graph.call_args[1]["cache_dir"]

    assert dbt_graph_cache_dir is None
    assert task_args_cache_dir == dbt_graph_cache_dir


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_contains_dbt_graph(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test validates that DbtToAirflowConverter contains and exposes a DbtGraph instance
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
        dag=DAG("sample_dag", start_date=datetime(2024, 4, 16)),
        nodes=nodes,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args=operator_args,
    )
    assert isinstance(converter.dbt_graph, DbtGraph)


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
    ],
)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_contains_tasks_map(mock_load_dbt_graph, execution_mode, operator_args):
    """
    This test validates that DbtToAirflowConverter contains and exposes a tasks map instance
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
        dag=DAG("sample_dag", start_date=datetime(2024, 1, 1)),
        nodes=nodes,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args=operator_args,
    )
    assert isinstance(converter.tasks_map, dict)

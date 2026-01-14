import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DAG

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DbtResourceType, ExecutionMode, InvocationMode, LoadMode, TestBehavior
from cosmos.converter import DbtToAirflowConverter, validate_arguments, validate_initial_user_config
from cosmos.dbt.graph import DbtGraph, DbtNode
from cosmos.exceptions import CosmosValueError
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

SAMPLE_PROFILE_YML = Path(__file__).parent / "sample/profiles.yml"
SAMPLE_DBT_PROJECT = Path(__file__).parent / "sample/"
SAMPLE_DBT_MANIFEST = Path(__file__).parent / "sample/manifest.json"
MULTIPLE_PARENTS_TEST_DBT_PROJECT = Path(__file__).parent.parent / "dev/dags/dbt/multiple_parents_test/"
DBT_PROJECTS_PROJ_WITH_DEPS_DIR = Path(__file__).parent.parent / "dev/dags/dbt" / "jaffle_shop"


@pytest.mark.parametrize("argument_key", ["tags", "paths"])
def test_validate_arguments_tags(argument_key):
    selector_name = argument_key[:-1]
    project_config = ProjectConfig(manifest_path=SAMPLE_DBT_MANIFEST, project_name="xubiru")
    render_config = RenderConfig(
        select=[f"{selector_name}:a,{selector_name}:b"], exclude=[f"{selector_name}:b,{selector_name}:c"]
    )
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    task_args = {}
    with pytest.raises(CosmosValueError) as err:
        validate_arguments(
            execution_config=execution_config,
            profile_config=profile_config,
            project_config=project_config,
            render_config=render_config,
            task_args=task_args,
        )
    expected = f"Can't specify the same {selector_name} in `select` and `exclude`: {{'b'}}"
    assert err.value.args[0] == expected


def test_validate_arguments_exception():
    render_config = RenderConfig(load_method=LoadMode.DBT_LS, dbt_deps=False)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL, dbt_project_path=DBT_PROJECTS_PROJ_WITH_DEPS_DIR
    )
    project_config = ProjectConfig()

    task_args = {"install_deps": True}  # this has to be the opposite of RenderConfig.dbt_deps
    with pytest.raises(CosmosValueError) as err:
        validate_arguments(
            execution_config=execution_config,
            profile_config=profile_config,
            project_config=project_config,
            render_config=render_config,
            task_args=task_args,
        )
    expected = "When using `LoadMode.DBT_LS` and `ExecutionMode.LOCAL`, the value of `dbt_deps` in `RenderConfig` should be the same as the `operator_args['install_deps']` value."
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


@pytest.mark.parametrize("operator_args", [{"env": {"key": "value"}}, {"install_deps": {"key": "value"}}])
def test_validate_user_config_operator_args_deprecated(operator_args):
    """Deprecating warnings should be raised when using operator_args with "env" or "install_deps"."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig()
    profile_config = MagicMock()

    with pytest.deprecated_call():
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


def test_validate_user_config_fails_project_config_and_operator_args_overlap():
    """
    The validation should fail if a user specifies both a ProjectConfig and operator_args with env_vars/env
    that overlap.
    """
    project_config = ProjectConfig(
        project_name="fake-project", dbt_project_path="/some/project/path", env_vars={"key": "value"}
    )
    execution_config = ExecutionConfig()
    render_config = RenderConfig()
    profile_config = MagicMock()
    operator_args = {"env": {"key": "value"}}

    expected_error_msg = (
        "ProjectConfig.env_vars and operator_args with 'env' are mutually exclusive and only one can be used."
    )
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


@patch("cosmos.converter.is_dbt_installed_in_same_environment", return_value=False)
def test_validate_initial_user_config_dbt_runner_without_dbt_installed(mock_is_dbt_installed):
    """Test that validation fails when using DBT_RUNNER but dbt is not installed in the same environment."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig(invocation_mode=InvocationMode.DBT_RUNNER)
    profile_config = MagicMock()
    operator_args = {}

    expected_error_match = "RenderConfig.invocation_mode is set to InvocationMode.DBT_RUNNER, but dbt is not installed in the same environment as Airflow.*"
    with pytest.raises(CosmosValueError, match=expected_error_match):
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


@patch("cosmos.converter.get_system_dbt", return_value="/usr/local/bin/dbt")
@patch("cosmos.converter.is_dbt_installed_in_same_environment", return_value=True)
def test_validate_initial_user_config_dbt_runner_with_different_dbt_executable_path(
    mock_is_dbt_installed, mock_get_system_dbt
):
    """Test that validation fails when using DBT_RUNNER with a custom dbt_executable_path that differs from system dbt."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig(invocation_mode=InvocationMode.DBT_RUNNER, dbt_executable_path="/custom/path/to/dbt")
    profile_config = MagicMock()
    operator_args = {}

    expected_error_match = (
        "RenderConfig.dbt_executable_path is set, but it is not the same as the system dbt executable path.*"
    )
    with pytest.raises(CosmosValueError, match=expected_error_match):
        validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


@patch("cosmos.converter.get_system_dbt", return_value="/usr/local/bin/dbt")
@patch("cosmos.converter.is_dbt_installed_in_same_environment", return_value=True)
def test_validate_initial_user_config_dbt_runner_with_matching_dbt_executable_path(
    mock_is_dbt_installed, mock_get_system_dbt
):
    """Test that validation passes when using DBT_RUNNER with a dbt_executable_path matching system dbt."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig(invocation_mode=InvocationMode.DBT_RUNNER, dbt_executable_path="/usr/local/bin/dbt")
    profile_config = MagicMock()
    operator_args = {}

    # Should not raise any exception
    validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


@patch("cosmos.converter.is_dbt_installed_in_same_environment", return_value=True)
def test_validate_initial_user_config_dbt_runner_without_dbt_executable_path(mock_is_dbt_installed):
    """Test that validation passes when using DBT_RUNNER without setting dbt_executable_path."""
    project_config = ProjectConfig()
    execution_config = ExecutionConfig()
    render_config = RenderConfig(invocation_mode=InvocationMode.DBT_RUNNER)
    profile_config = MagicMock()
    operator_args = {}

    # Should not raise any exception
    validate_initial_user_config(execution_config, profile_config, project_config, render_config, operator_args)


def test_validate_arguments_schema_in_task_args():
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL, dbt_project_path="/tmp/project-dir")
    render_config = RenderConfig()
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    task_args = {"schema": "abcd"}
    project_config = ProjectConfig(manifest_path=SAMPLE_DBT_MANIFEST, project_name="something")
    validate_arguments(
        execution_config=execution_config,
        profile_config=profile_config,
        render_config=render_config,
        task_args=task_args,
        project_config=project_config,
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
        (ExecutionMode.DOCKER, {"image": "sample-image"}),
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


@pytest.mark.integration
def test_converter_creates_dag_with_test_with_multiple_parents():
    """
    Validate topology of a project that uses the MULTIPLE_PARENTS_TEST_DBT_PROJECT project
    """
    project_config = ProjectConfig(dbt_project_path=MULTIPLE_PARENTS_TEST_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    render_config = RenderConfig(should_detach_multiple_parents_tests=True)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )
    with DAG("sample_dag", start_date=datetime(2024, 4, 16)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    tasks = converter.tasks_map

    assert len(converter.tasks_map) == 4

    # We exclude the test that depends on combined_model and model_a from their commands
    args = tasks["model.my_dbt_project.combined_model"].children["combined_model.test"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "combined_model", "--exclude", "custom_test_combined_model_combined_model_"]

    args = tasks["model.my_dbt_project.model_a"].children["model_a.test"].build_cmd({})[0]
    assert args[1:] == [
        "test",
        "--select",
        "model_a",
        "--exclude",
        "custom_test_combined_model_combined_model_",
    ]

    # The test for model_b should not be changed, since it is not a parent of this test
    args = tasks["model.my_dbt_project.model_b"].children["model_b.test"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "model_b"]

    # We should have a task dedicated to run the test with multiple parents
    args = tasks["test.my_dbt_project.custom_test_combined_model_combined_model_.c6e4587380"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "custom_test_combined_model_combined_model_"]
    assert (
        tasks["test.my_dbt_project.custom_test_combined_model_combined_model_.c6e4587380"].task_id
        == "custom_test_combined_model_combined_model__test"
    )


@pytest.mark.integration
def test_converter_creates_dag_with_test_with_multiple_parents_with_should_detach_multiple_parents_tests_false():
    """
    Validate topology of a project that uses the MULTIPLE_PARENTS_TEST_DBT_PROJECT project
    """
    project_config = ProjectConfig(dbt_project_path=MULTIPLE_PARENTS_TEST_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    render_config = RenderConfig(should_detach_multiple_parents_tests=False)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )
    with DAG("sample_dag", start_date=datetime(2024, 4, 16)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    tasks = converter.tasks_map

    assert len(converter.tasks_map) == 3

    # We exclude the test that depends on combined_model and model_a from their commands
    args = tasks["model.my_dbt_project.combined_model"].children["combined_model.test"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "combined_model"]

    args = tasks["model.my_dbt_project.model_a"].children["model_a.test"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "model_a"]

    # The test for model_b should not be changed, since it is not a parent of this test
    args = tasks["model.my_dbt_project.model_b"].children["model_b.test"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "model_b"]


@pytest.mark.integration
def test_converter_creates_dag_with_test_with_multiple_parents_test_afterall():
    """
    Validate topology of a project that uses the MULTIPLE_PARENTS_TEST_DBT_PROJECT project
    """
    project_config = ProjectConfig(dbt_project_path=MULTIPLE_PARENTS_TEST_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    render_config = RenderConfig(test_behavior=TestBehavior.AFTER_ALL, should_detach_multiple_parents_tests=True)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )
    with DAG("sample_dag", start_date=datetime(2024, 4, 16)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    tasks = converter.tasks_map

    assert len(converter.tasks_map) == 3

    assert tasks["model.my_dbt_project.combined_model"].task_id == "combined_model_run"
    assert tasks["model.my_dbt_project.model_a"].task_id == "model_a_run"
    assert tasks["model.my_dbt_project.model_b"].task_id == "model_b_run"
    assert tasks["model.my_dbt_project.combined_model"].downstream_task_ids == {"multiple_parents_test_test"}
    assert tasks["model.my_dbt_project.model_a"].downstream_task_ids == {"combined_model_run"}
    assert tasks["model.my_dbt_project.model_b"].downstream_task_ids == {"combined_model_run"}
    multiple_parents_test_test_args = tasks["model.my_dbt_project.combined_model"].downstream_list[0].build_cmd({})[0]
    assert multiple_parents_test_test_args[1:] == ["test"]


@pytest.mark.integration
def test_converter_creates_dag_with_test_with_multiple_parents_test_none():
    """
    Validate topology of a project that uses the MULTIPLE_PARENTS_TEST_DBT_PROJECT project
    """
    project_config = ProjectConfig(dbt_project_path=MULTIPLE_PARENTS_TEST_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    render_config = RenderConfig(test_behavior=TestBehavior.NONE, should_detach_multiple_parents_tests=True)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )
    with DAG("sample_dag", start_date=datetime(2024, 4, 16)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    tasks = converter.tasks_map

    assert len(converter.tasks_map) == 3

    assert tasks["model.my_dbt_project.combined_model"].task_id == "combined_model_run"
    assert tasks["model.my_dbt_project.model_a"].task_id == "model_a_run"
    assert tasks["model.my_dbt_project.model_b"].task_id == "model_b_run"
    assert tasks["model.my_dbt_project.combined_model"].downstream_task_ids == set()
    assert tasks["model.my_dbt_project.model_b"].downstream_task_ids == {"combined_model_run"}
    assert tasks["model.my_dbt_project.model_b"].downstream_task_ids == {"combined_model_run"}


@pytest.mark.integration
def test_converter_creates_dag_with_test_with_multiple_parents_and_build():
    """
    Validate topology of a project that uses the MULTIPLE_PARENTS_TEST_DBT_PROJECT project and uses TestBehavior.BUILD
    """
    project_config = ProjectConfig(dbt_project_path=MULTIPLE_PARENTS_TEST_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
    render_config = RenderConfig(test_behavior=TestBehavior.BUILD, should_detach_multiple_parents_tests=True)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="example_conn",
            profile_args={"schema": "public"},
            disable_event_tracking=True,
        ),
    )
    with DAG("sample_dag", start_date=datetime(2024, 4, 16)) as dag:
        converter = DbtToAirflowConverter(
            dag=dag,
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=render_config,
        )
    tasks = converter.tasks_map

    assert len(converter.tasks_map) == 4

    # We exclude the test that depends on combined_model and model_a from their commands
    args = tasks["model.my_dbt_project.combined_model"].build_cmd({})[0]
    assert args[1:] == [
        "build",
        "--select",
        "combined_model",
        "--exclude",
        "custom_test_combined_model_combined_model_",
    ]

    args = tasks["model.my_dbt_project.model_a"].build_cmd({})[0]
    assert args[1:] == ["build", "--select", "model_a", "--exclude", "custom_test_combined_model_combined_model_"]

    # The test for model_b should not be changed, since it is not a parent of this test
    args = tasks["model.my_dbt_project.model_b"].build_cmd({})[0]
    assert args[1:] == ["build", "--select", "model_b"]

    # We should have a task dedicated to run the test with multiple parents
    args = tasks["test.my_dbt_project.custom_test_combined_model_combined_model_.c6e4587380"].build_cmd({})[0]
    assert args[1:] == ["test", "--select", "custom_test_combined_model_combined_model_"]


@pytest.mark.parametrize(
    "execution_mode,operator_args",
    [
        (ExecutionMode.KUBERNETES, {}),
        (ExecutionMode.DOCKER, {"image": "sample-image"}),
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
        (ExecutionMode.DOCKER, Path("/some/virtualenv/dir"), {"image": "sample-image"}),
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
        (ExecutionMode.DOCKER, {"image": "sample-image"}),
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
        (ExecutionMode.DOCKER, {"image": "sample-image"}),
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
        (ExecutionMode.DOCKER, {"image": "sample-image"}),
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
@patch("cosmos.converter.DbtGraph")
def test_converter_multiple_calls_same_operator_args(mock_dbt_graph, mock_build_airflow_graph, mock_validate_project):
    """Tests if the DbttoAirflowConverter is called more than once with the same operator_args, the
    operator_args are not modified.
    """
    project_config = ProjectConfig(
        project_name="fake-project", dbt_project_path="/some/project/path", dbt_vars={"a-key": "a-value"}
    )
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
    assert mock_build_airflow_graph.call_args.kwargs["task_args"]["vars"] == {"key": "value"}
    assert mock_dbt_graph.call_args.kwargs["dbt_vars"] == {"a-key": "a-value"}


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


@pytest.mark.parametrize(
    "execution_mode,operator_args,install_dbt_deps,expected",
    [
        (ExecutionMode.KUBERNETES, {}, False, None),
        (ExecutionMode.LOCAL, {}, False, False),
        (ExecutionMode.VIRTUALENV, {}, False, False),
        (ExecutionMode.WATCHER, {}, False, False),
        (ExecutionMode.LOCAL, {}, True, True),
        (ExecutionMode.VIRTUALENV, {}, True, True),
        (ExecutionMode.WATCHER, {}, True, True),
        (ExecutionMode.KUBERNETES, {"install_deps": True}, False, True),
        (ExecutionMode.LOCAL, {"install_deps": True}, False, True),
        (ExecutionMode.VIRTUALENV, {"install_deps": True}, False, True),
        (ExecutionMode.WATCHER, {"install_deps": True}, False, True),
    ],
)
@patch("cosmos.config.ProjectConfig.validate_project")
@patch("cosmos.converter.validate_initial_user_config")
@patch("cosmos.converter.DbtGraph")
@patch("cosmos.converter.build_airflow_graph")
def test_project_config_install_dbt_deps_overrides_operator_args(
    mock_build_airflow_graph,
    mock_user_config,
    mock_dbt_graph,
    mock_validate_project,
    execution_mode,
    operator_args,
    install_dbt_deps,
    expected,
):
    """Tests that the value project_config.install_dbt_deps is used to define operator_args["install_deps"] if
    execution mode is ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV, or ExecutionMode.WATCHER and operator_args["install_deps"] is not
    already defined.
    """
    project_config = ProjectConfig(project_name="fake-project", dbt_project_path="/some/project/path")
    project_config.install_dbt_deps = install_dbt_deps
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    render_config = MagicMock()
    profile_config = MagicMock()
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
    _, kwargs = mock_build_airflow_graph.call_args

    assert kwargs["task_args"].get("install_deps", None) == expected


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


sample_model = DbtNode(
    unique_id=f"{DbtResourceType.MODEL}.{SAMPLE_DBT_PROJECT.stem}.sample_model",
    resource_type=DbtResourceType.MODEL,
    depends_on=[],
    file_path="",
)
nodes_with_model = {"sample_model": sample_model}


@patch("cosmos.airflow.graph.settings.pre_dbt_fusion", True)
@patch("cosmos.converter.DbtGraph.filtered_nodes", nodes_with_model)
@patch("cosmos.converter.DbtGraph.load")
def test_converter_creates_model_with_pre_dbt_fusion(mock_load_dbt_graph):
    """
    This test validates that DbtToAirflowConverter contains and exposes a tasks map instance
    """
    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)
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
        operator_args={},
    )
    assert isinstance(converter.tasks_map, dict)
    assert converter.tasks_map["sample_model"].models == "sample.sample_model"
    assert converter.tasks_map["sample_model"].select is None


@patch("cosmos.converter._create_folder_version_hash")
@patch("cosmos.converter.DbtGraph.load")
def test_dag_versioning_hash_appended_to_empty_doc_md(mock_load_dbt_graph, mock_hash_func):
    """Test that dbt project hash is appended to DAG doc_md when doc_md is initially empty."""
    mock_hash_func.return_value = "abc123def456"
    dag = DAG("test_dag", start_date=datetime(2024, 1, 1))
    assert dag.doc_md is None  # Initially empty

    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    profile_config = ProfileConfig(
        profile_name="my_profile_name",
        target_name="my_target_name",
        profiles_yml_filepath=SAMPLE_PROFILE_YML,
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

    DbtToAirflowConverter(
        dag=dag,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    assert dag.doc_md == "**dbt project hash:** `abc123def456`"
    mock_hash_func.assert_called_once()


@patch("cosmos.converter._create_folder_version_hash")
@patch("cosmos.converter.DbtGraph.load")
def test_dag_versioning_hash_appended_to_existing_doc_md(mock_load_dbt_graph, mock_hash_func):
    """Test that dbt project hash is appended to existing DAG doc_md."""
    mock_hash_func.return_value = "xyz789abc123"
    existing_doc = "This is my existing DAG documentation.\n\nIt has multiple lines."
    dag = DAG("test_dag", start_date=datetime(2024, 1, 1), doc_md=existing_doc)

    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

    DbtToAirflowConverter(
        dag=dag,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    expected_doc = existing_doc + "\n\n**dbt project hash:** `xyz789abc123`"
    assert dag.doc_md == expected_doc
    mock_hash_func.assert_called_once()


@patch("cosmos.converter.logger")
@patch("cosmos.converter._create_folder_version_hash")
@patch("cosmos.converter.DbtGraph.load")
def test_dag_versioning_hash_error_handling(mock_load_dbt_graph, mock_hash_func, mock_logger):
    """Test that hash creation errors are properly handled and logged."""
    mock_hash_func.side_effect = Exception("File system error")
    dag = DAG("test_dag", start_date=datetime(2024, 1, 1))
    original_doc_md = dag.doc_md  # Should be None

    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

    DbtToAirflowConverter(
        dag=dag,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    # DAG doc_md should remain unchanged when error occurs
    assert dag.doc_md == original_doc_md

    # Error should be logged as warning
    mock_logger.warning.assert_called_once()
    warning_call = mock_logger.warning.call_args[0][0]
    assert "Failed to append dbt project hash to DAG documentation" in warning_call
    assert "File system error" in warning_call


@patch("cosmos.converter._create_folder_version_hash")
@patch("cosmos.converter.DbtGraph.load")
def test_dag_versioning_hash_with_special_characters(mock_load_dbt_graph, mock_hash_func):
    """Test hash appending works correctly with special characters in existing doc_md."""
    mock_hash_func.return_value = "hash_with_special_chars!@#$%"
    existing_doc = "DAG with **markdown**, `code`, and [links](http://example.com)"
    dag = DAG("test_dag", start_date=datetime(2024, 1, 1), doc_md=existing_doc)

    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

    DbtToAirflowConverter(
        dag=dag,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    expected_doc = existing_doc + "\n\n**dbt project hash:** `hash_with_special_chars!@#$%`"
    assert dag.doc_md == expected_doc


@patch("cosmos.converter.logger")
@patch("cosmos.converter._create_folder_version_hash")
@patch("cosmos.converter.DbtGraph.load")
def test_dag_versioning_successful_logging(mock_load_dbt_graph, mock_hash_func, mock_logger):
    """Test that successful hash appending is logged at debug level."""
    mock_hash_func.return_value = "test_hash_123"
    dag = DAG("test_dag_logging", start_date=datetime(2024, 1, 1))

    project_config = ProjectConfig(dbt_project_path=SAMPLE_DBT_PROJECT)
    profile_config = ProfileConfig(
        profile_name="test",
        target_name="test",
        profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
    )
    execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

    DbtToAirflowConverter(
        dag=dag,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    mock_logger.debug.assert_called_once_with(
        "Appended dbt project hash test_hash_123 to DAG test_dag_logging documentation"
    )

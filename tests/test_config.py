from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest.mock import Mock, PropertyMock, call, patch

import pytest

from cosmos.config import CosmosConfigException, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.exceptions import CosmosValueError
from cosmos.profiles.athena.access_key import AthenaAccessKeyProfileMapping
from cosmos.profiles.postgres.user_pass import PostgresUserPasswordProfileMapping
from cosmos.settings import AIRFLOW_IO_AVAILABLE

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent / "sample/"
SAMPLE_PROFILE_YML = Path(__file__).parent / "sample/profiles.yml"
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
    assert project_config.install_dbt_deps is True


def test_init_with_project_path_and_install_dbt_deps_succeeds():
    """
    Passing only dbt_project_path and install_dbt_deps should succeed and set install_dbt_deps to the value defined
    """
    project_config = ProjectConfig(dbt_project_path="path/to/dbt/project", install_dbt_deps=False)
    assert project_config.install_dbt_deps is False


def test_init_with_manifest_path_and_project_path_succeeds():
    """
    Passing a manifest path AND project path together should succeed
    project_name in this case should be based on dbt_project_path
    """
    project_config = ProjectConfig(dbt_project_path="/tmp/some-path", manifest_path="target/manifest.json")
    if AIRFLOW_IO_AVAILABLE:
        try:
            from airflow.sdk import ObjectStoragePath
        except ImportError:
            try:
                from airflow.io.path import ObjectStoragePath
            except ImportError:
                pass

        assert project_config.manifest_path == ObjectStoragePath("target/manifest.json")
    else:
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


def test_profile_config_validate_none():
    with pytest.raises(CosmosValueError) as err_info:
        ProfileConfig(profile_name="test", target_name="test")
    assert err_info.value.args[0] == "Either profiles_yml_filepath or profile_mapping must be set to render a profile"


def test_profile_config_validate_both():
    with pytest.raises(CosmosValueError) as err_info:
        ProfileConfig(
            profile_name="test",
            target_name="test",
            profiles_yml_filepath=SAMPLE_PROFILE_YML,
            profile_mapping=PostgresUserPasswordProfileMapping(conn_id="test", profile_args={}),
        )
    assert (
        err_info.value.args[0]
        == "Both profiles_yml_filepath and profile_mapping are defined and are mutually exclusive. Ensure only one of these is defined."
    )


def test_profile_config_validate_profiles_yml():
    profile_config = ProfileConfig(profile_name="test", target_name="test", profiles_yml_filepath="/tmp/no-exists")
    with pytest.raises(CosmosValueError) as err_info:
        profile_config.validate_profiles_yml()

    assert err_info.value.args[0] == "The file /tmp/no-exists does not exist."


@patch("cosmos.config.is_profile_cache_enabled", return_value=False)
@patch("cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping.env_vars", new_callable=PropertyMock)
@patch("cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping.get_profile_file_contents")
@patch("cosmos.config.Path")
def test_profile_config_ensure_profile_without_caching_calls_get_profile_file_content_before_env_vars(
    mock_path, mock_get_profile_file_contents, mock_env_vars, mock_cache
):
    """
    The `env_vars` should not be called if profile file is not populated.
    """
    profile_mapping = AthenaAccessKeyProfileMapping(conn_id="test", profile_args={})
    profile_config = ProfileConfig(profile_name="test", target_name="test", profile_mapping=profile_mapping)
    mock_manager = Mock()
    mock_manager.attach_mock(mock_get_profile_file_contents, "get_profile_file_contents")
    mock_manager.attach_mock(mock_env_vars, "env_vars")

    with profile_config.ensure_profile(desired_profile_path=mock_path):
        mock_get_profile_file_contents.assert_called_once()
        mock_env_vars.assert_called_once()
        expected_calls = [
            call.get_profile_file_contents(profile_name="test", target_name="test", use_mock_values=False),
            call.env_vars,
        ]
        mock_manager.assert_has_calls(expected_calls, any_order=False)


@patch("cosmos.config.create_cache_profile")
@patch("cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping.version")
@patch("cosmos.config.get_cached_profile", return_value=None)
@patch("cosmos.config.is_profile_cache_enabled", return_value=True)
@patch("cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping.env_vars", new_callable=PropertyMock)
@patch("cosmos.profiles.athena.access_key.AthenaAccessKeyProfileMapping.get_profile_file_contents")
@patch("cosmos.config.Path")
def test_profile_config_ensure_profile_with_caching_calls_get_profile_file_content_before_env_vars(
    mock_path,
    mock_get_profile_file_contents,
    mock_env_vars,
    mock_cache,
    mock_get_cached_profile,
    mock_version,
    mock_create_cache_profile,
):
    """
    The `env_vars` should not be called if profile file is not populated.
    """
    profile_mapping = AthenaAccessKeyProfileMapping(conn_id="test", profile_args={})
    profile_config = ProfileConfig(profile_name="test", target_name="test", profile_mapping=profile_mapping)
    mock_manager = Mock()
    mock_manager.attach_mock(mock_get_profile_file_contents, "get_profile_file_contents")
    mock_manager.attach_mock(mock_env_vars, "env_vars")

    with profile_config.ensure_profile(desired_profile_path=mock_path):
        mock_get_profile_file_contents.assert_called_once()
        mock_env_vars.assert_called_once()
        expected_calls = [
            call.get_profile_file_contents(profile_name="test", target_name="test", use_mock_values=False),
            call.env_vars,
        ]
        mock_manager.assert_has_calls(expected_calls, any_order=False)


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


def test_is_dbt_ls_file_available_is_true():
    render_config = RenderConfig(dbt_ls_path=DBT_PROJECTS_ROOT_DIR / "sample_dbt_ls.txt")
    assert render_config.is_dbt_ls_file_available()


def test_is_dbt_ls_file_available_is_true_for_str_path():
    render_config = RenderConfig(dbt_ls_path=str(DBT_PROJECTS_ROOT_DIR / "sample_dbt_ls.txt"))
    assert render_config.is_dbt_ls_file_available()


def test_is_dbt_ls_file_available_is_false():
    render_config = RenderConfig(dbt_ls_path=None)
    assert not render_config.is_dbt_ls_file_available()


def test_render_config_env_vars_deprecated():
    """RenderConfig.env_vars is deprecated since Cosmos 1.3, should warn user."""
    with pytest.deprecated_call():
        RenderConfig(env_vars={"VAR": "value"})


@pytest.mark.parametrize(
    "execution_mode, invocation_mode, expectation",
    [
        (ExecutionMode.LOCAL, InvocationMode.DBT_RUNNER, does_not_raise()),
        (ExecutionMode.LOCAL, InvocationMode.SUBPROCESS, does_not_raise()),
        (ExecutionMode.LOCAL, None, does_not_raise()),
        (ExecutionMode.VIRTUALENV, InvocationMode.DBT_RUNNER, pytest.raises(CosmosValueError)),
        (ExecutionMode.VIRTUALENV, InvocationMode.SUBPROCESS, does_not_raise()),
        (ExecutionMode.VIRTUALENV, None, does_not_raise()),
        (ExecutionMode.KUBERNETES, InvocationMode.DBT_RUNNER, pytest.raises(CosmosValueError)),
        (ExecutionMode.DOCKER, InvocationMode.DBT_RUNNER, pytest.raises(CosmosValueError)),
        (ExecutionMode.AZURE_CONTAINER_INSTANCE, InvocationMode.DBT_RUNNER, pytest.raises(CosmosValueError)),
    ],
)
def test_execution_config_with_invocation_option(execution_mode, invocation_mode, expectation):
    with expectation:
        ExecutionConfig(execution_mode=execution_mode, invocation_mode=invocation_mode)


@pytest.mark.parametrize(
    "execution_mode, expected_invocation_mode",
    [
        (ExecutionMode.LOCAL, None),
        (ExecutionMode.VIRTUALENV, InvocationMode.SUBPROCESS),
        (ExecutionMode.KUBERNETES, None),
        (ExecutionMode.DOCKER, None),
        (ExecutionMode.AZURE_CONTAINER_INSTANCE, None),
    ],
)
def test_execution_config_default_config(execution_mode, expected_invocation_mode):
    execution_config = ExecutionConfig(execution_mode=execution_mode)
    assert execution_config.invocation_mode == expected_invocation_mode


@pytest.mark.parametrize(
    "manifest_path, given_manifest_conn_id, used_manifest_conn_id",
    [
        ("s3://cosmos-manifest-test/manifest.json", None, "aws_default"),
        ("s3://cosmos-manifest-test/manifest.json", "aws_s3_conn", "aws_s3_conn"),
        ("gs://cosmos-manifest-test/manifest.json", None, "google_cloud_default"),
        ("gs://cosmos-manifest-test/manifest.json", "gcp_gs_conn", "gcp_gs_conn"),
        ("abfs://cosmos-manifest-test/manifest.json", None, "wasb_default"),
        ("abfs://cosmos-manifest-test/manifest.json", "azure_abfs_conn", "azure_abfs_conn"),
    ],
)
def test_remote_manifest_path(manifest_path, given_manifest_conn_id, used_manifest_conn_id):
    if AIRFLOW_IO_AVAILABLE:
        project_config = ProjectConfig(
            dbt_project_path="/tmp/some-path", manifest_path=manifest_path, manifest_conn_id=given_manifest_conn_id
        )
        try:
            from airflow.sdk import ObjectStoragePath
        except ImportError:
            try:
                from airflow.io.path import ObjectStoragePath
            except ImportError:
                pass

        assert project_config.manifest_path == ObjectStoragePath(manifest_path, conn_id=used_manifest_conn_id)
    else:
        from airflow.version import version as airflow_version

        error_msg = (
            f"The manifest path {manifest_path} uses a remote file scheme, but the required Object Storage feature is "
            f"unavailable in Airflow version {airflow_version}. Please upgrade to Airflow 2.8 or later."
        )
        with pytest.raises(CosmosValueError, match=error_msg):
            _ = ProjectConfig(
                dbt_project_path="/tmp/some-path", manifest_path=manifest_path, manifest_conn_id=given_manifest_conn_id
            )

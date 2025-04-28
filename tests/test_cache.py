from airflow import __version__ as airflow_version
from packaging import version

from cosmos.constants import _AIRFLOW3_MAJOR_VERSION

# TODO: Enable and make tests functional in the module for AF 3. Disabling to due to DB access code in this module.
#  Issue: https://github.com/astronomer/astronomer-cosmos/issues/1711
if version.parse(airflow_version).major >= _AIRFLOW3_MAJOR_VERSION:
    import pytest

    pytest.skip("Skipping Cache tests on Airflow 3.0+", allow_module_level=True)

import logging
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow import DAG
from airflow.models import DagRun, Variable
from airflow.utils.db import create_session
from airflow.utils.task_group import TaskGroup

from cosmos.cache import (
    _configure_remote_cache_dir,
    _copy_partial_parse_to_project,
    _create_cache_identifier,
    _create_folder_version_hash,
    _get_latest_cached_package_lockfile,
    _get_latest_partial_parse,
    _get_or_create_profile_cache_dir,
    _get_sha1_hash,
    _update_partial_parse_cache,
    create_cache_profile,
    delete_unused_dbt_ls_cache,
    get_cached_profile,
    is_cache_package_lockfile_enabled,
    is_profile_cache_enabled,
)
from cosmos.constants import (
    DBT_PARTIAL_PARSE_FILE_NAME,
    DBT_TARGET_DIR_NAME,
    DEFAULT_PROFILES_FILE_NAME,
    _default_s3_conn,
)
from cosmos.exceptions import CosmosValueError
from cosmos.settings import AIRFLOW_IO_AVAILABLE, dbt_profile_cache_dir_name

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)
SAMPLE_PARTIAL_PARSE_FILEPATH = Path(__file__).parent / "sample/partial_parse.msgpack"


@pytest.mark.parametrize(
    "dag, task_group, result_identifier",
    [
        (example_dag, None, "dag"),
        (None, TaskGroup(dag=example_dag, group_id="inner_tg"), "dag__inner_tg"),
        (
            None,
            TaskGroup(
                dag=example_dag, group_id="child_tg", parent_group=TaskGroup(dag=example_dag, group_id="parent_tg")
            ),
            "dag__parent_tg__child_tg",
        ),
        (
            None,
            TaskGroup(
                dag=example_dag,
                group_id="child_tg",
                parent_group=TaskGroup(
                    dag=example_dag, group_id="mum_tg", parent_group=TaskGroup(dag=example_dag, group_id="nana_tg")
                ),
            ),
            "dag__nana_tg__mum_tg__child_tg",
        ),
    ],
)
def test_create_cache_identifier(dag, task_group, result_identifier):
    assert _create_cache_identifier(dag, task_group) == result_identifier


def test_get_latest_partial_parse(tmp_path):
    old_tmp_dir = tmp_path / "old"
    old_tmp_target_dir = old_tmp_dir / DBT_TARGET_DIR_NAME
    old_tmp_target_dir.mkdir(parents=True, exist_ok=True)
    old_partial_parse_filepath = old_tmp_target_dir / DBT_PARTIAL_PARSE_FILE_NAME
    old_partial_parse_filepath.touch()

    # This is necessary in the CI, but not on local MacOS dev env, since the files
    # were being created too quickly and sometimes had the same st_mtime
    time.sleep(1)

    new_tmp_dir = tmp_path / "new"
    new_tmp_target_dir = new_tmp_dir / DBT_TARGET_DIR_NAME
    new_tmp_target_dir.mkdir(parents=True, exist_ok=True)
    new_partial_parse_filepath = new_tmp_target_dir / DBT_PARTIAL_PARSE_FILE_NAME
    new_partial_parse_filepath.touch()

    assert _get_latest_partial_parse(old_tmp_dir, new_tmp_dir) == new_partial_parse_filepath
    assert _get_latest_partial_parse(new_tmp_dir, old_tmp_dir) == new_partial_parse_filepath
    assert _get_latest_partial_parse(old_tmp_dir, old_tmp_dir) == old_partial_parse_filepath
    assert _get_latest_partial_parse(old_tmp_dir, tmp_path) == old_partial_parse_filepath
    assert _get_latest_partial_parse(tmp_path, old_tmp_dir) == old_partial_parse_filepath
    assert _get_latest_partial_parse(tmp_path, tmp_path) is None


@patch("cosmos.cache.msgpack.unpack", side_effect=ValueError)
def test__copy_partial_parse_to_project_msg_fails_msgpack(mock_unpack, tmp_path, caplog):
    caplog.set_level(logging.INFO)
    source_dir = tmp_path / DBT_TARGET_DIR_NAME
    source_dir.mkdir()
    partial_parse_filepath = source_dir / DBT_PARTIAL_PARSE_FILE_NAME
    shutil.copy(str(SAMPLE_PARTIAL_PARSE_FILEPATH), str(partial_parse_filepath))

    # actual test
    with tempfile.TemporaryDirectory() as tmp_dir:
        _copy_partial_parse_to_project(partial_parse_filepath, Path(tmp_dir))

    assert "Unable to patch the partial_parse.msgpack file due to ValueError()" in caplog.text


def test__create_folder_version_hash(tmp_path, caplog):
    """
    Test that Cosmos is still able to create the hash of a dbt project folder even when
    there is a symbolic link referencing a no longer existing file.

    This test addresses the issue:
    https://github.com/astronomer/astronomer-cosmos/issues/1096
    """
    caplog.set_level(logging.INFO)

    # Create a source folder with two files
    source_dir = tmp_path / "original_dbt_folder"
    source_dir.mkdir()
    file_1 = Path(source_dir / "file_1.sql")
    file_1.touch()
    file_2 = Path(source_dir / "file_2.sql")
    file_2.touch()

    # Create a target folder with symbolic links to the two files in the source folder
    target_dir = tmp_path / "cosmos_dbt_folder"
    target_dir.mkdir()
    file_1_symlink = Path(target_dir / "file_1.sql")
    file_1_symlink.symlink_to(file_1)
    file_2_symlink = Path(target_dir / "file_2.sql")
    file_2_symlink.symlink_to(file_2)

    # Delete one of the original files from the source folder
    file_1.unlink()

    _create_folder_version_hash(target_dir)


@patch("cosmos.cache.shutil.copyfile")
@patch("cosmos.cache.get_partial_parse_path")
def test_update_partial_parse_cache(mock_get_partial_parse_path, mock_copyfile):
    mock_get_partial_parse_path.side_effect = lambda cache_dir: cache_dir / "partial_parse.yml"

    latest_partial_parse_filepath = Path("/path/to/latest_partial_parse.yml")
    cache_dir = Path("/tmp/path/to/cache_directory")

    # Expected paths
    cache_path = cache_dir / "partial_parse.yml"
    manifest_path = cache_dir / "manifest.json"

    _update_partial_parse_cache(latest_partial_parse_filepath, cache_dir)

    # Assert shutil.copyfile was called twice with the correct arguments
    calls = [
        call(str(latest_partial_parse_filepath), str(cache_path)),
        call(str(latest_partial_parse_filepath.parent / "manifest.json"), str(manifest_path)),
    ]
    mock_copyfile.assert_has_calls(calls)


@pytest.fixture
def vars_session():
    with create_session() as session:
        var1 = Variable(key="cosmos_cache__dag_a", val='{"dag_id": "dag_a"}')
        var2 = Variable(key="cosmos_cache__dag_b", val='{"dag_id": "dag_b"}')
        var3 = Variable(key="cosmos_cache__dag_c__task_group_1", val='{"dag_id": "dag_c"}')

        dag_run_a = DagRun(
            dag_id="dag_a",
            run_id="dag_a_run_a_week_ago",
            execution_date=datetime.now(timezone.utc) - timedelta(days=7),
            state="success",
            run_type="manual",
        )
        dag_run_b = DagRun(
            dag_id="dag_b",
            run_id="dag_b_run_yesterday",
            execution_date=datetime.now(timezone.utc) - timedelta(days=1),
            state="failed",
            run_type="manual",
        )
        dag_run_c = DagRun(
            dag_id="dag_c",
            run_id="dag_c_run_on_hour_ago",
            execution_date=datetime.now(timezone.utc) - timedelta(hours=1),
            state="running",
            run_type="manual",
        )

        session.add(var1)
        session.add(var2)
        session.add(var3)
        session.add(dag_run_a)
        session.add(dag_run_b)
        session.add(dag_run_c)
        session.commit()

        yield session

        session.query(Variable).filter_by(key="cosmos_cache__dag_a").delete()
        session.query(Variable).filter_by(key="cosmos_cache__dag_b").delete()
        session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").delete()

        session.query(DagRun).filter_by(dag_id="dag_a", run_id="dag_a_run_a_week_ago").delete()
        session.query(DagRun).filter_by(dag_id="dag_b", run_id="dag_b_run_yesterday").delete()
        session.query(DagRun).filter_by(dag_id="dag_c", run_id="dag_c_run_on_hour_ago").delete()
        session.commit()


@pytest.mark.integration
def test_delete_unused_dbt_ls_cache_deletes_a_week_ago_cache(vars_session):
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(days=5), session=vars_session) == 1
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()


@pytest.mark.integration
def test_delete_unused_dbt_ls_cache_deletes_all_cache_five_minutes_ago(vars_session):
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_b").first()
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").first()
    assert delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(minutes=5), session=vars_session) == 3
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_b").first()
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").first()


@pytest.mark.parametrize(
    "enable_cache, enable_cache_profile, expected_result",
    [(True, True, True), (True, False, False), (False, True, False), (False, False, False)],
)
def test_is_profile_cache_enabled(enable_cache, enable_cache_profile, expected_result):
    with patch("cosmos.cache.enable_cache", enable_cache), patch(
        "cosmos.cache.enable_cache_profile", enable_cache_profile
    ):
        assert is_profile_cache_enabled() == expected_result


def test_get_or_create_profile_cache_dir():
    # Create a temporary directory for cache_dir
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Test case 1: Directory does not exist, should create it
        with patch("cosmos.cache.cache_dir", temp_dir_path):
            profile_cache_dir = _get_or_create_profile_cache_dir()
            expected_dir = temp_dir_path / dbt_profile_cache_dir_name
            assert profile_cache_dir == expected_dir
            assert expected_dir.exists()

        # Test case 2: Directory already exists, should return existing path
        with patch("cosmos.cache.cache_dir", temp_dir_path):
            profile_cache_dir_again = _get_or_create_profile_cache_dir()
            expected_dir = temp_dir_path / dbt_profile_cache_dir_name
            assert profile_cache_dir_again == expected_dir
            assert expected_dir.exists()


def test_get_cached_profile_not_exists():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        # Mock cache_dir to use the temporary directory
        with patch("cosmos.cache.cache_dir", temp_dir):
            # Create a dummy profile YAML file for version 'v1'
            version = "592906f650558ce1dadb75fcce84a2ec09e444441e6af6069f19204d59fe428b"
            result = get_cached_profile(version)
            assert result is None


def test_get_cached_profile():
    profile_content = """
        default:
          target: dev
          outputs:
            dev:
              type: postgres
              host: localhost
              user: myuser
              pass: mypassword
              dbname: mydatabase
        """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with patch("cosmos.cache.cache_dir", temp_dir):
            # Setup DBT profile
            version = "592906f650558ce1dadb75fcce84a2ec09e444441e6af6069f19204d59fe428b"
            create_cache_profile(version, profile_content)

            expected_yml_path = temp_dir / dbt_profile_cache_dir_name / version / DEFAULT_PROFILES_FILE_NAME
            result = get_cached_profile(version)
            assert result == expected_yml_path


def test_create_cache_profile():
    version = "592906f650558ce1dadb75fcce84a2ec09e444441e6af6069f19204d59fe428b"
    profile_content = """
    default:
      target: dev
      outputs:
        dev:
          type: postgres
          host: localhost
          user: myuser
          pass: mypassword
          dbname: mydatabase
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with patch("cosmos.cache.cache_dir", temp_dir):
            profile_yml_path = create_cache_profile(version, profile_content)

            expected_dir = temp_dir / dbt_profile_cache_dir_name / version
            expected_path = expected_dir / DEFAULT_PROFILES_FILE_NAME

            # Check if the directory and file were created
            assert expected_dir.exists()
            assert expected_path.exists()

            # Check content of the created file
            assert expected_path.read_text() == profile_content
            assert profile_yml_path == expected_path


@patch("pathlib.Path.is_file")
def test_cache_package_lockfile_enabled(mock_path_is_file):
    # Mocking the return value of Path.is_file()
    mock_path_is_file.return_value = True

    # Test case where lockfile exists
    project_dir = Path("/path/to/your/project")
    result = is_cache_package_lockfile_enabled(project_dir)
    assert result is True

    # Test case where lockfile doesn't exist
    mock_path_is_file.return_value = False
    result = is_cache_package_lockfile_enabled(project_dir)
    assert result is False


@pytest.fixture
def mock_package_lockfile():
    # Create a temporary YAML file with test data
    yaml_data = """
    packages:
      - package: dbt-labs/dbt_utils
        version: 1.1.1
    sha1_hash: a158c48c59c2bb7d729d2a4e215aabe5bb4f3353
    """
    tmp_file = Path("test_package-lock.yml")
    with open(tmp_file, "w") as file:
        file.write(yaml_data)
    yield tmp_file
    # Clean up: delete the temporary file after the test
    tmp_file.unlink()


def test_get_sha1_hash():
    profile_lock_content = """
    packages:
      - package: dbt-labs/dbt_utils
        version: 1.1.1
    sha1_hash: a158c48c59c2bb7d729d2a4e215aabe5bb4f3353
    """
    tmp_file = Path("package-lock.yml")
    with open(tmp_file, "w") as file:
        file.write(profile_lock_content)

    sha1_hash = _get_sha1_hash(tmp_file)
    assert sha1_hash == "a158c48c59c2bb7d729d2a4e215aabe5bb4f3353"
    tmp_file.unlink()


def _test_tmp_dir(dir_name: str):
    # Create a temporary directory for cache_dir
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir) / "test_cache"
        cache_dir.mkdir()
        return cache_dir


@patch("cosmos.cache.cache_dir")
@patch("cosmos.cache._get_sha1_hash")
def test_get_latest_cached_package_lockfile_with_cache(mock_get_sha, cache_dir):
    # Create a fake cached lockfile
    project_dir = _test_tmp_dir("test_project")
    cache_dir.return_value = _test_tmp_dir("test_cache")
    cache_identifier = project_dir.name
    cached_profile_lockfile = cache_dir / cache_identifier / "package-lock.yml"
    cached_profile_lockfile.parent.mkdir(parents=True, exist_ok=True)
    cached_profile_lockfile.touch()

    # Test case where there is a cached file
    result = _get_latest_cached_package_lockfile(project_dir)
    assert result == cached_profile_lockfile
    assert cached_profile_lockfile.exists()


@patch("cosmos.cache._get_sha1_hash")
def test_get_latest_cached_lockfile_with_no_cache(mock_get_sha):
    project_dir = _test_tmp_dir("test_project")
    project_package_lockfile = project_dir / "package-lock.yml"
    project_package_lockfile.parent.mkdir(parents=True, exist_ok=True)
    project_package_lockfile.touch()

    # Test case where there is a cached file
    result = _get_latest_cached_package_lockfile(project_dir)
    assert result.exists()


@patch("cosmos.cache.settings_remote_cache_dir", new=None)
def test_remote_cache_path_initialization_no_remote_cache_dir():
    configured_remote_cache_dir = _configure_remote_cache_dir()
    assert configured_remote_cache_dir is None


@patch("cosmos.cache.settings_remote_cache_dir", new="s3://some-bucket/cache")
@patch("cosmos.cache.AIRFLOW_IO_AVAILABLE", new=False)
def test_remote_cache_path_initialization_object_storage_unavailable_on_earlier_airflow_versions():
    with pytest.raises(CosmosValueError, match="Object Storage feature is unavailable"):
        _configure_remote_cache_dir()


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
@patch("cosmos.cache.settings_remote_cache_dir", new="s3://some-bucket/cache")
@patch("airflow.io.path.ObjectStoragePath")
def test_remote_cache_path_initialization_path_available_default_connection(mock_object_storage_path):
    mock_cache_dir_path = MagicMock()
    mock_cache_dir_path.exists.return_value = True
    mock_object_storage_path.return_value = mock_cache_dir_path

    configured_remote_cache_dir = _configure_remote_cache_dir()
    mock_object_storage_path.assert_called_with("s3://some-bucket/cache", conn_id=_default_s3_conn)
    assert configured_remote_cache_dir == mock_cache_dir_path


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
@patch("cosmos.cache.settings_remote_cache_dir", new="s3://some-bucket/cache")
@patch("airflow.io.path.ObjectStoragePath")
def test_remote_cache_dir_initialization_path_not_exist_creates_path(mock_object_storage_path):
    mock_cache_dir_path = MagicMock()
    mock_cache_dir_path.exists.return_value = False
    mock_object_storage_path.return_value = mock_cache_dir_path

    _ = _configure_remote_cache_dir()
    mock_cache_dir_path.mkdir.assert_called_once_with(parents=True, exist_ok=True)


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
@patch("cosmos.cache.settings_remote_cache_dir", new="s3://some-bucket/cache")
@patch("cosmos.cache.remote_cache_dir_conn_id", new="my_conn_id")
@patch("airflow.io.path.ObjectStoragePath")
def test_remote_cache_path_initialization_with_conn_id(mock_object_storage_path):
    mock_cache_path = MagicMock()
    mock_cache_path.exists.return_value = True
    mock_object_storage_path.return_value = mock_cache_path

    configured_remote_cache_dir = _configure_remote_cache_dir()
    mock_object_storage_path.assert_called_with("s3://some-bucket/cache", conn_id="my_conn_id")
    assert configured_remote_cache_dir == mock_cache_path

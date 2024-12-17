from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cosmos.exceptions import CosmosValueError
from cosmos.io import (
    _construct_dest_file_path,
    upload_artifacts_to_aws_s3,
    upload_artifacts_to_azure_wasb,
    upload_artifacts_to_cloud_storage,
    upload_artifacts_to_gcp_gs,
)
from cosmos.settings import AIRFLOW_IO_AVAILABLE


@pytest.fixture
def dummy_kwargs():
    """Fixture for reusable test kwargs."""
    return {
        "dag": MagicMock(dag_id="test_dag"),
        "run_id": "test_run_id",
        "task_instance": MagicMock(task_id="test_task", _try_number=1),
        "bucket_name": "test_bucket",
        "container_name": "test_container",
    }


def test_upload_artifacts_to_aws_s3(dummy_kwargs):
    """Test upload_artifacts_to_aws_s3."""
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook") as mock_hook, patch("os.walk") as mock_walk:
        mock_walk.return_value = [("/target", [], ["file1.txt", "file2.txt"])]

        upload_artifacts_to_aws_s3("/project_dir", **dummy_kwargs)

        mock_walk.assert_called_once_with("/project_dir/target")
        hook_instance = mock_hook.return_value
        assert hook_instance.load_file.call_count == 2


def test_upload_artifacts_to_gcp_gs(dummy_kwargs):
    """Test upload_artifacts_to_gcp_gs."""
    with patch("airflow.providers.google.cloud.hooks.gcs.GCSHook") as mock_hook, patch("os.walk") as mock_walk:
        mock_walk.return_value = [("/target", [], ["file1.txt", "file2.txt"])]

        upload_artifacts_to_gcp_gs("/project_dir", **dummy_kwargs)

        mock_walk.assert_called_once_with("/project_dir/target")
        hook_instance = mock_hook.return_value
        assert hook_instance.upload.call_count == 2


def test_upload_artifacts_to_azure_wasb(dummy_kwargs):
    """Test upload_artifacts_to_azure_wasb."""
    with patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook, patch("os.walk") as mock_walk:
        mock_walk.return_value = [("/target", [], ["file1.txt", "file2.txt"])]

        upload_artifacts_to_azure_wasb("/project_dir", **dummy_kwargs)

        mock_walk.assert_called_once_with("/project_dir/target")
        hook_instance = mock_hook.return_value
        assert hook_instance.load_file.call_count == 2


def test_configure_remote_target_path_no_remote_target():
    """Test _configure_remote_target_path when no remote target path is set."""
    with patch("cosmos.settings.remote_target_path", None):
        from cosmos.io import _configure_remote_target_path

        assert _configure_remote_target_path() == (None, None)


def test_construct_dest_file_path(dummy_kwargs):
    """Test _construct_dest_file_path."""
    dest_target_dir = Path("/dest")
    source_target_dir = Path("/project_dir/target")
    file_path = "/project_dir/target/subdir/file.txt"

    expected_path = "/dest/test_dag/test_run_id/test_task/1/target/subdir/file.txt"
    assert _construct_dest_file_path(dest_target_dir, file_path, source_target_dir, **dummy_kwargs) == expected_path


def test_upload_artifacts_to_cloud_storage_no_remote_path():
    """Test upload_artifacts_to_cloud_storage with no remote path."""
    with patch("cosmos.io._configure_remote_target_path", return_value=(None, None)):
        with pytest.raises(CosmosValueError):
            upload_artifacts_to_cloud_storage("/project_dir", **{})


@pytest.mark.skipif(not AIRFLOW_IO_AVAILABLE, reason="Airflow did not have Object Storage until the 2.8 release")
def test_upload_artifacts_to_cloud_storage_success(dummy_kwargs):
    """Test upload_artifacts_to_cloud_storage with valid setup."""
    with patch(
        "cosmos.io._configure_remote_target_path",
        return_value=(Path("/dest"), "conn_id"),
    ) as mock_configure, patch("pathlib.Path.rglob") as mock_rglob, patch(
        "airflow.io.path.ObjectStoragePath.copy"
    ) as mock_copy:
        mock_file1 = MagicMock(spec=Path)
        mock_file1.is_file.return_value = True
        mock_file1.__str__.return_value = "/project_dir/target/file1.txt"

        mock_file2 = MagicMock(spec=Path)
        mock_file2.is_file.return_value = True
        mock_file2.__str__.return_value = "/project_dir/target/file2.txt"

        mock_rglob.return_value = [mock_file1, mock_file2]

        upload_artifacts_to_cloud_storage("/project_dir", **dummy_kwargs)

        mock_configure.assert_called_once()
        assert mock_copy.call_count == 2

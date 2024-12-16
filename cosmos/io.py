from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from cosmos import settings
from cosmos.constants import FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP
from cosmos.exceptions import CosmosValueError
from cosmos.settings import remote_target_path, remote_target_path_conn_id


def upload_artifacts_to_aws_s3(project_dir: str, **kwargs: Any) -> None:
    """
    Helper function demonstrating how to upload artifacts to AWS S3 that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    target_dir = f"{project_dir}/target"
    aws_conn_id = kwargs.get("aws_conn_id", S3Hook.default_conn_name)
    bucket_name = kwargs["bucket_name"]
    hook = S3Hook(aws_conn_id=aws_conn_id)

    # Iterate over the files in the target dir and upload them to S3
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            s3_key = (
                f"{kwargs['dag'].dag_id}"
                f"/{kwargs['run_id']}"
                f"/{kwargs['task_instance'].task_id}"
                f"/{kwargs['task_instance']._try_number}"
                f"{dirpath.split(project_dir)[-1]}/{filename}"
            )
            hook.load_file(
                filename=f"{dirpath}/{filename}",
                bucket_name=bucket_name,
                key=s3_key,
                replace=True,
            )


def upload_artifacts_to_gcp_gs(project_dir: str, **kwargs: Any) -> None:
    """
    Helper function demonstrating how to upload artifacts to GCP GS that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    """
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    target_dir = f"{project_dir}/target"
    gcp_conn_id = kwargs.get("gcp_conn_id", GCSHook.default_conn_name)
    bucket_name = kwargs["bucket_name"]
    hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # Iterate over the files in the target dir and upload them to GCP GS
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            object_name = (
                f"{kwargs['dag'].dag_id}"
                f"/{kwargs['run_id']}"
                f"/{kwargs['task_instance'].task_id}"
                f"/{kwargs['task_instance']._try_number}"
                f"{dirpath.split(project_dir)[-1]}/{filename}"
            )
            hook.upload(
                filename=f"{dirpath}/{filename}",
                bucket_name=bucket_name,
                object_name=object_name,
            )


def upload_artifacts_to_azure_wasb(project_dir: str, **kwargs: Any) -> None:
    """
    Helper function demonstrating how to upload artifacts to Azure WASB that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    target_dir = f"{project_dir}/target"
    azure_conn_id = kwargs.get("azure_conn_id", WasbHook.default_conn_name)
    container_name = kwargs["container_name"]
    hook = WasbHook(wasb_conn_id=azure_conn_id)

    # Iterate over the files in the target dir and upload them to WASB container
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            blob_name = (
                f"{kwargs['dag'].dag_id}"
                f"/{kwargs['run_id']}"
                f"/{kwargs['task_instance'].task_id}"
                f"/{kwargs['task_instance']._try_number}"
                f"{dirpath.split(project_dir)[-1]}/{filename}"
            )
            hook.load_file(
                file_path=f"{dirpath}/{filename}",
                container_name=container_name,
                blob_name=blob_name,
                overwrite=True,
            )


def _configure_remote_target_path() -> tuple[Path, str] | tuple[None, None]:
    """Configure the remote target path if it is provided."""
    from airflow.version import version as airflow_version

    if not remote_target_path:
        return None, None

    _configured_target_path = None

    target_path_str = str(remote_target_path)

    remote_conn_id = remote_target_path_conn_id
    if not remote_conn_id:
        target_path_schema = urlparse(target_path_str).scheme
        remote_conn_id = FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP.get(target_path_schema, None)  # type: ignore[assignment]
    if remote_conn_id is None:
        return None, None

    if not settings.AIRFLOW_IO_AVAILABLE:
        raise CosmosValueError(
            f"You're trying to specify remote target path {target_path_str}, but the required "
            f"Object Storage feature is unavailable in Airflow version {airflow_version}. Please upgrade to "
            "Airflow 2.8 or later."
        )

    from airflow.io.path import ObjectStoragePath

    _configured_target_path = ObjectStoragePath(target_path_str, conn_id=remote_conn_id)

    if not _configured_target_path.exists():  # type: ignore[no-untyped-call]
        _configured_target_path.mkdir(parents=True, exist_ok=True)

    return _configured_target_path, remote_conn_id


def _construct_dest_file_path(
    dest_target_dir: Path,
    file_path: str,
    source_target_dir: Path,
    **kwargs: Any,
) -> str:
    """
    Construct the destination path for the artifact files to be uploaded to the remote store.
    """
    dest_target_dir_str = str(dest_target_dir).rstrip("/")

    task_run_identifier = (
        f"{kwargs['dag'].dag_id}"
        f"/{kwargs['run_id']}"
        f"/{kwargs['task_instance'].task_id}"
        f"/{kwargs['task_instance']._try_number}"
    )
    rel_path = os.path.relpath(file_path, source_target_dir).lstrip("/")

    return f"{dest_target_dir_str}/{task_run_identifier}/target/{rel_path}"


def upload_artifacts_to_cloud_storage(project_dir: str, **kwargs: Any) -> None:
    """
    Helper function demonstrating how to upload artifacts to remote blob stores that can be used as a callback. This is
    an example of a helper function that can be used if on Airflow >= 2.8 and cosmos configurations like
    ``remote_target_path`` and ``remote_target_path_conn_id`` when set can be leveraged.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    """
    dest_target_dir, dest_conn_id = _configure_remote_target_path()

    if not dest_target_dir:
        raise CosmosValueError("You're trying to upload artifact files, but the remote target path is not configured.")

    from airflow.io.path import ObjectStoragePath

    source_target_dir = Path(project_dir) / "target"
    files = [str(file) for file in source_target_dir.rglob("*") if file.is_file()]
    for file_path in files:
        dest_file_path = _construct_dest_file_path(dest_target_dir, file_path, source_target_dir, **kwargs)
        dest_object_storage_path = ObjectStoragePath(dest_file_path, conn_id=dest_conn_id)
        ObjectStoragePath(file_path).copy(dest_object_storage_path)

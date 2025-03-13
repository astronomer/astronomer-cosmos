from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from cosmos import settings
from cosmos.constants import DEFAULT_TARGET_PATH, FILE_SCHEME_AIRFLOW_DEFAULT_CONN_ID_MAP
from cosmos.exceptions import CosmosValueError


def upload_to_aws_s3(
    project_dir: str,
    bucket_name: str,
    aws_conn_id: str | None = None,
    source_subpath: str = DEFAULT_TARGET_PATH,
    **kwargs: Any,
) -> None:
    """
    Helper function demonstrating how to upload files to AWS S3 that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    :param bucket_name: Name of the S3 bucket to upload to.
    :param aws_conn_id: AWS connection ID to use when uploading files.
    :param source_subpath: Path of the source directory sub-path to upload files from.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    target_dir = f"{project_dir}/{source_subpath}"
    aws_conn_id = aws_conn_id if aws_conn_id else S3Hook.default_conn_name
    hook = S3Hook(aws_conn_id=aws_conn_id)
    context = kwargs["context"]

    # Iterate over the files in the target dir and upload them to S3
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            s3_key = (
                f"{context['dag'].dag_id}"
                f"/{context['run_id']}"
                f"/{context['task_instance'].task_id}"
                f"/{context['task_instance']._try_number}"
                f"{dirpath.split(project_dir)[-1]}/{filename}"
            )
            hook.load_file(
                filename=f"{dirpath}/{filename}",
                bucket_name=bucket_name,
                key=s3_key,
                replace=True,
            )


def upload_to_gcp_gs(
    project_dir: str,
    bucket_name: str,
    gcp_conn_id: str | None = None,
    source_subpath: str = DEFAULT_TARGET_PATH,
    **kwargs: Any,
) -> None:
    """
    Helper function demonstrating how to upload files to GCP GS that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    :param bucket_name: Name of the GCP GS bucket to upload to.
    :param gcp_conn_id: GCP connection ID to use when uploading files.
    :param source_subpath: Path of the source directory sub-path to upload files from.
    """
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    target_dir = f"{project_dir}/{source_subpath}"
    gcp_conn_id = gcp_conn_id if gcp_conn_id else GCSHook.default_conn_name
    # bucket_name = kwargs["bucket_name"]
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    context = kwargs["context"]

    # Iterate over the files in the target dir and upload them to GCP GS
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            object_name = (
                f"{context['dag'].dag_id}"
                f"/{context['run_id']}"
                f"/{context['task_instance'].task_id}"
                f"/{context['task_instance']._try_number}"
                f"{dirpath.split(project_dir)[-1]}/{filename}"
            )
            hook.upload(
                filename=f"{dirpath}/{filename}",
                bucket_name=bucket_name,
                object_name=object_name,
            )


def upload_to_azure_wasb(
    project_dir: str,
    container_name: str,
    azure_conn_id: str | None = None,
    source_subpath: str = DEFAULT_TARGET_PATH,
    **kwargs: Any,
) -> None:
    """
    Helper function demonstrating how to upload files to Azure WASB that can be used as a callback.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    :param container_name: Name of the Azure WASB container to upload files to.
    :param azure_conn_id: Azure connection ID to use when uploading files.
    :param source_subpath: Path of the source directory sub-path to upload files from.
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    target_dir = f"{project_dir}/{source_subpath}"
    azure_conn_id = azure_conn_id if azure_conn_id else WasbHook.default_conn_name
    # container_name = kwargs["container_name"]
    hook = WasbHook(wasb_conn_id=azure_conn_id)
    context = kwargs["context"]

    # Iterate over the files in the target dir and upload them to WASB container
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            blob_name = (
                f"{context['dag'].dag_id}"
                f"/{context['run_id']}"
                f"/{context['task_instance'].task_id}"
                f"/{context['task_instance']._try_number}"
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

    if not settings.remote_target_path:
        return None, None

    _configured_target_path = None

    target_path_str = str(settings.remote_target_path)

    remote_conn_id = settings.remote_target_path_conn_id
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
    source_subpath: str,
    **kwargs: Any,
) -> str:
    """
    Construct the destination path for the artifact files to be uploaded to the remote store.
    """
    dest_target_dir_str = str(dest_target_dir).rstrip("/")

    context = kwargs["context"]
    task_run_identifier = (
        f"{context['dag'].dag_id}"
        f"/{context['run_id']}"
        f"/{context['task_instance'].task_id}"
        f"/{context['task_instance']._try_number}"
    )
    rel_path = os.path.relpath(file_path, source_target_dir).lstrip("/")

    return f"{dest_target_dir_str}/{task_run_identifier}/{source_subpath}/{rel_path}"


def upload_to_cloud_storage(project_dir: str, source_subpath: str = DEFAULT_TARGET_PATH, **kwargs: Any) -> None:
    """
    Helper function demonstrating how to upload files to remote object stores that can be used as a callback. This is
    an example of a helper function that can be used if on Airflow >= 2.8 and cosmos configurations like
    ``remote_target_path`` and ``remote_target_path_conn_id`` when set can be leveraged.

    :param project_dir: Path of the cloned project directory which Cosmos tasks work from.
    :param source_subpath: Path of the source directory sub-path to upload files from.
    """
    dest_target_dir, dest_conn_id = _configure_remote_target_path()

    if not dest_target_dir:
        raise CosmosValueError("You're trying to upload artifact files, but the remote target path is not configured.")

    from airflow.io.path import ObjectStoragePath

    source_target_dir = Path(project_dir) / f"{source_subpath}"
    files = [str(file) for file in source_target_dir.rglob("*") if file.is_file()]
    for file_path in files:
        dest_file_path = _construct_dest_file_path(
            dest_target_dir, file_path, source_target_dir, source_subpath, **kwargs
        )
        dest_object_storage_path = ObjectStoragePath(dest_file_path, conn_id=dest_conn_id)
        ObjectStoragePath(file_path).copy(dest_object_storage_path)

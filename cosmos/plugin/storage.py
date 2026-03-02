"""Utility functions for Cosmos plugins."""


def get_storage_type_from_path(path: str) -> str:
    """Determine the storage type from the path.

    Args:
        path: Storage path (e.g., 's3://bucket/path', '/local/path')

    Returns:
        Storage type identifier: 's3', 'gcs', 'azure', 'http', or 'local'
    """
    path = path.strip()
    if path.startswith("s3://"):
        return "s3"
    elif path.startswith("gs://"):
        return "gcs"
    elif path.startswith("wasb://"):
        return "azure"
    elif path.startswith("http://") or path.startswith("https://"):
        return "http"
    else:
        return "local"

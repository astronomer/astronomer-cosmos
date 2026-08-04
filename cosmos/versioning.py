from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Collection
from pathlib import Path
from typing import Any

from cosmos import settings
from cosmos.log import get_logger

logger = get_logger(__name__)

# Read files in chunks so that large artifacts that survive the excluded-dirs pruning
# (e.g. a misplaced manifest.json) don't get loaded into memory whole
_HASH_READ_CHUNK_SIZE = 1024 * 1024

# dbt stamps these into every manifest's `metadata` on every invocation, even when nothing else
# in the project changed, so they must be dropped before hashing or the hash would too.
_VOLATILE_MANIFEST_METADATA_KEYS = ("generated_at", "invocation_id", "invocation_started_at")


def _create_folder_version_hash(
    dir_path: Path,
    excluded_dirs: Collection[str] | None = None,
    manifest_path: Any | None = None,
    **kwargs: Any,
) -> str:
    """
    Given a directory, iterate through its content and create a hash that will change in case the
    contents of the directory change. The value should not change if the values of the directory do not change, even if
    the command is run from different Airflow instances.

    Directory names listed in ``excluded_dirs`` are pruned from the walk wherever they appear in the
    tree. When ``excluded_dirs`` is None, the ``[cosmos] project_hash_excluded_dirs`` setting is used,
    which defaults to generated folders such as ``target/``, ``dbt_packages/``, ``logs/`` and ``.git/``.
    Pass an explicit collection (including an empty one) to override the setting.

    ``manifest_path``, when given, folds the dbt manifest's own content checksum into the result --
    needed under ``LoadMode.DBT_MANIFEST``, since the manifest conventionally lives under ``target/``
    (excluded above) and may not even live under ``dir_path`` at all. Metadata fields that dbt stamps on
    every invocation (``generated_at``, ``invocation_id``, ...) are ignored, so a manifest regenerated
    from an unchanged project still hashes the same. A read/parse failure is caught and logged, falling
    back to the folder hash alone. ``**kwargs`` absorbs future extensions without another signature bump.

    This method output must be concise and it currently changes based on operating system.
    """
    # This approach is less efficient than using modified time
    # sum([path.stat().st_mtime for path in dir_path.glob("**/*")])
    # unfortunately, the modified time approach does not work well for dag-only deployments
    # where DAGs are constantly synced to the deployed Airflow
    # for 5k files, this seems to take 0.14
    if excluded_dirs is None:
        excluded_dirs = settings.project_hash_excluded_dirs

    hasher = hashlib.md5()
    filepaths = []
    pruned_dirs = 0

    for root_dir, dirs, files in os.walk(dir_path):
        if excluded_dirs:
            before = len(dirs)
            dirs[:] = [dirname for dirname in dirs if dirname not in excluded_dirs]
            pruned_dirs += before - len(dirs)
        paths = [os.path.join(root_dir, filepath) for filepath in files]
        filepaths.extend(paths)

    if pruned_dirs:
        logger.debug("Pruned %s excluded directories while hashing the dbt project folder %s", pruned_dirs, dir_path)

    relative_posix_paths = {filepath: Path(filepath).relative_to(dir_path).as_posix() for filepath in filepaths}
    for filepath in sorted(filepaths, key=lambda fp: relative_posix_paths[fp]):
        # Include the path so that renaming a file also changes the hash; dbt derives node
        # names from file names, so a content-preserving rename still changes the project.
        # Null-byte separator avoids a path/content boundary ambiguity; as_posix() is OS-independent,
        # and sorting by it (not the OS-native filepath) keeps iteration order OS-independent too.
        hasher.update(relative_posix_paths[filepath].encode())
        hasher.update(b"\0")
        try:
            with open(str(filepath), "rb") as fp:
                while chunk := fp.read(_HASH_READ_CHUNK_SIZE):
                    hasher.update(chunk)
        except FileNotFoundError:
            logger.warning("The dbt project folder contains a symbolic link to a non-existent file: %s", filepath)

    folder_hash = hasher.hexdigest()

    if manifest_path is None:
        return folder_hash

    try:
        manifest_checksum = _manifest_content_checksum(manifest_path)
    except Exception as e:
        logger.warning("Failed to fold dbt manifest checksum into the project version hash: %s", e)
        return folder_hash

    return hashlib.md5(f"{folder_hash}\0{manifest_checksum}".encode()).hexdigest()


def _manifest_content_checksum(manifest_path: Any) -> str:
    """MD5 of a dbt manifest's content, ignoring metadata fields dbt stamps on every invocation
    (``generated_at``, ``invocation_id``, ...) even when the project itself hasn't changed."""
    with manifest_path.open("rb") as fp:
        manifest = json.load(fp)
    metadata = manifest.get("metadata")
    if isinstance(metadata, dict):
        for key in _VOLATILE_MANIFEST_METADATA_KEYS:
            metadata.pop(key, None)
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.md5(canonical).hexdigest()

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from cosmos.log import get_logger

logger = get_logger(__name__)


def _create_folder_version_hash(dir_path: Path) -> str:
    """
    Given a directory, iterate through its content and create a hash that will change in case the
    contents of the directory change. The value should not change if the values of the directory do not change, even if
    the command is run from different Airflow instances.

    This method output must be concise and it currently changes based on operating system.
    """
    # This approach is less efficient than using modified time
    # sum([path.stat().st_mtime for path in dir_path.glob("**/*")])
    # unfortunately, the modified time approach does not work well for dag-only deployments
    # where DAGs are constantly synced to the deployed Airflow
    # for 5k files, this seems to take 0.14
    hasher = hashlib.md5()
    filepaths = []

    for root_dir, dirs, files in os.walk(dir_path):
        paths = [os.path.join(root_dir, filepath) for filepath in files]
        filepaths.extend(paths)

    for filepath in sorted(filepaths):
        try:
            with open(str(filepath), "rb") as fp:
                buf = fp.read()
                hasher.update(buf)
        except FileNotFoundError:
            logger.warning(f"The dbt project folder contains a symbolic link to a non-existent file: {filepath}")

    return hasher.hexdigest()

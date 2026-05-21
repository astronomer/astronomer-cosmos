from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def safe_copy(src: Path, dst: Path) -> None:
    """
    Safely copies a file from a source path to a destination path.

    This function ensures that the copy operation is atomic by first
    copying the file to a temporary file in the same directory as the
    destination and then renaming the temporary file to the destination
    file. This approach minimizes the risk of file corruption or partial
    writes in case of a failure or interruption during the copy process.

    See the blog for atomic file operations:
    https://alexwlchan.net/2019/atomic-cross-filesystem-moves-in-python/
    """
    # Create a temporary file in the same directory as the destination
    dir_name, base_name = os.path.split(dst)
    temp_fd, temp_path = tempfile.mkstemp(dir=dir_name)

    shutil.copyfile(src, temp_path)

    # Rename the temporary file to the destination file
    os.rename(temp_path, dst)

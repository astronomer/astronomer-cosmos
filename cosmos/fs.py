from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from pathlib import Path

from cosmos.log import get_logger

logger = get_logger(__name__)

# Read the file in fixed-size chunks when checksumming so a large file (e.g. a seed CSV) does not have
# to be loaded into memory all at once.
_CHECKSUM_READ_CHUNK_SIZE = 1024 * 1024


def calculate_file_checksum(file_path: Path) -> str | None:
    """Return the MD5 checksum of a file, streaming it in chunks, or ``None`` if it cannot be read.

    MD5 is used for consistency with the other non-cryptographic content/identifier hashes across Cosmos
    (e.g. ``cosmos/cache.py``, ``cosmos/versioning.py``); this is change detection, not a security boundary.
    """
    digest = hashlib.md5()
    try:
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(_CHECKSUM_READ_CHUNK_SIZE), b""):
                digest.update(chunk)
    except OSError as exc:
        logger.warning("Unable to read file `%s` to compute its checksum: %s", file_path, exc)
        return None
    return digest.hexdigest()


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
    # Create a temporary file in the same directory as the destination.
    temp_fd, temp_path = tempfile.mkstemp(dir=dst.parent)
    os.close(temp_fd)

    try:
        shutil.copyfile(src, temp_path)

        # Replace the temporary file with the destination file atomically.
        os.replace(temp_path, dst)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)

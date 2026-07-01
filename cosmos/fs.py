from __future__ import annotations

import errno
import hashlib
import os
import shutil
import tempfile
import time
from pathlib import Path

from cosmos.log import get_logger

logger = get_logger(__name__)

# On a shared network filesystem (e.g. NFS/EFS) a concurrent writer that atomically
# replaces ``src`` (temp-file + rename) can invalidate a reader's open handle mid-copy,
# surfacing as ESTALE ("Stale file handle"). The replacement is always a complete file,
# so simply re-opening ``src`` on a retry resolves the new inode and succeeds.
_SAFE_COPY_RETRIES = 3
_SAFE_COPY_RETRY_BACKOFF_SECONDS = 0.05

# Read the file in fixed-size chunks when checksumming so a large file (e.g. a seed CSV) does not have
# to be loaded into memory all at once.
_CHECKSUM_READ_CHUNK_SIZE = 1024 * 1024


def _calculate_file_checksum(file_path: Path) -> str | None:
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

    If ``src`` lives on a shared network filesystem, another process atomically replacing it
    while we read can raise ESTALE ("Stale file handle"); because the replacement is a complete
    file, the copy is retried a few times so the caller sees the new contents instead of an error.
    """
    for attempt in range(_SAFE_COPY_RETRIES + 1):
        # Create a temporary file in the same directory as the destination.
        temp_fd, temp_path = tempfile.mkstemp(dir=dst.parent)
        os.close(temp_fd)

        try:
            shutil.copyfile(src, temp_path)

            # ``mkstemp`` creates the temp file with mode 0o600 regardless of umask, so without this
            # the destination would always end up owner-only — narrower than the source. Copy the
            # source's permission bits over so the atomic write preserves the original file mode
            # (matching the previous ``shutil.copy`` behaviour and keeping shared, group-readable
            # cache_dir setups working).
            shutil.copymode(src, temp_path)

            # Replace the temporary file with the destination file atomically.
            os.replace(temp_path, dst)
            return
        except OSError as exc:
            # ESTALE means a concurrent writer replaced ``src`` (or an involved path) on a
            # network filesystem mid-copy. Retry with a fresh open so we pick up the new inode;
            # re-raise any other error, or ESTALE once we have exhausted our retries.
            if exc.errno != errno.ESTALE or attempt == _SAFE_COPY_RETRIES:
                raise
            logger.debug("safe_copy hit a stale file handle copying %s; retrying (%d)", src, attempt + 1)
            time.sleep(_SAFE_COPY_RETRY_BACKOFF_SECONDS * (attempt + 1))
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

from __future__ import annotations

import errno
import hashlib
import logging
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.fs import _calculate_file_checksum, safe_copy


def test_safe_copy_writes_destination(tmp_path: Path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    safe_copy(src, dst)

    assert dst.read_text() == "hello"


def test_safe_copy_overwrites_existing_destination(tmp_path: Path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("new")
    dst = tmp_path / "dst.txt"
    dst.write_text("old")

    safe_copy(src, dst)

    assert dst.read_text() == "new"


def test_safe_copy_preserves_source_mode(tmp_path: Path) -> None:
    # ``mkstemp`` creates the temp file as 0o600; ``safe_copy`` must restore the source's
    # group/other-readable bits so the atomic write does not narrow access on shared cache dirs.
    src = tmp_path / "src.txt"
    src.write_text("hello")
    src.chmod(0o644)
    dst = tmp_path / "dst.txt"

    safe_copy(src, dst)

    assert (dst.stat().st_mode & 0o777) == 0o644


def test_safe_copy_cleans_up_temp_file_on_failure(tmp_path: Path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    with patch("cosmos.fs.shutil.copyfile", side_effect=OSError("boom")):
        with pytest.raises(OSError, match="boom"):
            safe_copy(src, dst)

    # The temp file created next to dst must not be left behind.
    assert list(tmp_path.iterdir()) == [src]
    assert not dst.exists()


def test_safe_copy_retries_on_stale_file_handle(tmp_path: Path) -> None:
    # A concurrent writer replacing ``src`` on a network filesystem surfaces as ESTALE; safe_copy
    # must retry (re-opening the new inode) rather than propagating the error.
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    # ``cosmos.fs.shutil`` is the same module object as ``shutil`` here, so the patch below also
    # replaces ``shutil.copyfile``. Capture the real implementation now, before patching, so the
    # retry can actually perform the copy (calling ``shutil.copyfile`` inside would recurse).
    real_copyfile = shutil.copyfile
    calls = {"n": 0}

    def flaky_copyfile(source, target, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(errno.ESTALE, "Stale file handle")
        return real_copyfile(source, target, *args, **kwargs)

    with patch("cosmos.fs.shutil.copyfile", side_effect=flaky_copyfile):
        safe_copy(src, dst)

    assert calls["n"] == 2
    assert dst.read_text() == "hello"
    # No temp artefacts left behind by the failed first attempt.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["dst.txt", "src.txt"]


def test_safe_copy_raises_after_exhausting_stale_retries(tmp_path: Path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    with patch("cosmos.fs.shutil.copyfile", side_effect=OSError(errno.ESTALE, "Stale file handle")):
        with pytest.raises(OSError) as exc_info:
            safe_copy(src, dst)

    assert exc_info.value.errno == errno.ESTALE
    assert list(tmp_path.iterdir()) == [src]
    assert not dst.exists()


def test_safe_copy_does_not_retry_non_stale_oserror(tmp_path: Path) -> None:
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    calls = {"n": 0}

    def failing_copyfile(*args, **kwargs):
        calls["n"] += 1
        raise OSError(errno.EACCES, "Permission denied")

    with patch("cosmos.fs.shutil.copyfile", side_effect=failing_copyfile):
        with pytest.raises(OSError) as exc_info:
            safe_copy(src, dst)

    # A non-ESTALE error is not retried.
    assert calls["n"] == 1
    assert exc_info.value.errno == errno.EACCES


def test_calculate_file_checksum_matches_md5(tmp_path: Path) -> None:
    content = b"id,name\n1,alice\n2,bob\n"
    file_path = tmp_path / "seed.csv"
    file_path.write_bytes(content)

    assert _calculate_file_checksum(file_path) == hashlib.md5(content).hexdigest()


def test_calculate_file_checksum_is_stable_across_calls(tmp_path: Path) -> None:
    file_path = tmp_path / "seed.csv"
    file_path.write_bytes(b"a" * (2 * 1024 * 1024))  # larger than one read chunk

    assert _calculate_file_checksum(file_path) == _calculate_file_checksum(file_path)


def test_calculate_file_checksum_returns_none_when_unreadable(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.WARNING)

    assert _calculate_file_checksum(tmp_path / "missing.csv") is None
    assert "Unable to read file" in caplog.text

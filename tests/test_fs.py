from __future__ import annotations

import hashlib
import logging
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

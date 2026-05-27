from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from cosmos.fs import safe_copy


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

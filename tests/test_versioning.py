import logging
from pathlib import Path

from cosmos.versioning import _create_folder_version_hash


def test__create_folder_version_hash(tmp_path, caplog):
    """
    Test that Cosmos is still able to create the hash of a dbt project folder even when
    there is a symbolic link referencing a no longer existing file.

    This test addresses the issue:
    https://github.com/astronomer/astronomer-cosmos/issues/1096
    """
    caplog.set_level(logging.INFO)

    # Create a source folder with two files
    source_dir = tmp_path / "original_dbt_folder"
    source_dir.mkdir()
    file_1 = Path(source_dir / "file_1.sql")
    file_1.touch()
    file_2 = Path(source_dir / "file_2.sql")
    file_2.touch()

    # Create a target folder with symbolic links to the two files in the source folder
    target_dir = tmp_path / "cosmos_dbt_folder"
    target_dir.mkdir()
    file_1_symlink = Path(target_dir / "file_1.sql")
    file_1_symlink.symlink_to(file_1)
    file_2_symlink = Path(target_dir / "file_2.sql")
    file_2_symlink.symlink_to(file_2)

    # Delete one of the original files from the source folder
    file_1.unlink()

    _create_folder_version_hash(target_dir)

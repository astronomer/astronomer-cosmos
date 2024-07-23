"""
Use this script locally to identify broken symbolic links or recursive loops locally:
  $ python -m cosmos.cleanup -p <dir-path>

To delete the issues identified, run:
  $ python -m cosmos.cleanup -p <dir-path> -d
"""

import argparse
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def identify_broken_symbolic_links(dir_path: str, should_delete: bool = False) -> None:
    """
    Given a directory, recursively inspect it in search for symbolic links.
    If should_delete is set to True, delete the symbolic links identified.

    :param dir_path: Path to the directory to be analysed
    :param should_delete: Users should set to True if they want the method to not only identify but also delete these links.
    """
    logger.info(f"Inspecting the directory {dir_path} for broken symbolic links.")
    filepaths = []
    broken_symlinks_count = 0
    deleted_symlinks_count = 0
    for root_dir, dirs, files in os.walk(dir_path):
        paths = [os.path.join(root_dir, filepath) for filepath in files]
        filepaths.extend(paths)

    for filepath in filepaths:
        try:
            os.stat(filepath)
        except OSError:
            broken_symlinks_count += 1
            logger.warning(f"The folder {dir_path} contains a symbolic link to a non-existent file: {filepath}")
            if should_delete:
                logger.info(f"Deleting the invalid symbolic link: {filepath}")
                os.unlink(filepath)
                deleted_symlinks_count += 1

    logger.info(
        f"After inspecting {dir_path}, identified {broken_symlinks_count} broken links and deleted {deleted_symlinks_count} of them."
    )


# Airflow DAG parsing fails if recursive loops are found, so this method cannot be used from within an Airflow task
def identify_recursive_loops(original_dir_path: str, should_delete: bool = False) -> None:
    """
    Given a directory, recursively inspect it in search for recursive loops.
    If should_delete is set to True, delete the (symbolic links) recursive loops identified.

    :param dir_path: Path to the directory to be analysed
    :param should_delete: Users should set to True if they want the method to not only identify but also delete these loops.
    """
    logger.info(f"Inspecting the directory {original_dir_path} for recursive loops.")
    dirs_paths = []
    broken_symlinks_count = 0
    deleted_symlinks_count = 0

    dir_path = Path(original_dir_path).absolute()

    for root_dir, dirs, files in os.walk(dir_path):
        paths = [os.path.join(root_dir, dir_name) for dir_name in dirs]
        dirs_paths.extend(paths)

    for subdir_path in dirs_paths:
        if os.path.islink(subdir_path):
            symlink_target_path = os.path.realpath(subdir_path)
            if Path(symlink_target_path) in Path(subdir_path).parents:
                logger.warning(f"Detected recursive loop from {subdir_path} to {symlink_target_path}")
                broken_symlinks_count += 1
                if should_delete:
                    logger.info(f"Deleting symbolic link: {subdir_path}")
                    os.unlink(subdir_path)
                    deleted_symlinks_count += 1

    logger.info(
        f"After inspecting {dir_path}, identified {broken_symlinks_count} recursive loops and deleted {deleted_symlinks_count} of them."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean up local directory from broken symbolic links and recursive loops."
    )
    parser.add_argument("-p", "--dir-path", help="Path to directory to be inspected", required=True)
    parser.add_argument(
        "-d", "--delete", help="Delete problems found", action="store_true", required=False, default=False
    )
    args = parser.parse_args()
    identify_recursive_loops(args.dir_path, args.delete)
    identify_broken_symbolic_links(args.dir_path, args.delete)

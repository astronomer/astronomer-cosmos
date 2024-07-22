"""
We've observed users who had dbt project directories containing symbolic links to files that no longer existed.

Although this issue was not created by Cosmos itself, since this issue was already observed by two users, we thought it
was useful to give an example DAG illustrating how to clean the problematic directories.

Assuming the cause of the issue no longer exists, this DAG can be run only once.
"""

# [START dirty_dir_example]
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
dbt_project_folder = Path(__file__).parent / "dbt"


def identify_broken_symbolic_links(dir_path: str, should_delete: bool = False) -> None:
    """
    Given a directory, recursively inspect it in search for symbolic links.
    If should_delete is set to True, delete the symbolic links identified.

    :param dir_path: Path to the directory to be analysed
    :param should_delete: Users should set to True if they want the method to not only identify but also delete these links.
    """
    logger.info(f"Inspecting the directory {dir_path} for broken symbolic links.")
    filepaths = []
    broken_links_count = 0
    deleted_links_count = 0
    for root_dir, dirs, files in os.walk(dir_path):
        paths = [os.path.join(root_dir, filepath) for filepath in files]
        filepaths.extend(paths)

    for filepath in filepaths:
        try:
            os.stat(filepath)
        except OSError:
            broken_links_count += 1
            logger.warning(f"The folder {dir_path} contains a symbolic link to a non-existent file: {filepath}")
            if should_delete:
                logger.info(f"Deleting the invalid symbolic link: {filepath}")
                os.unlink(filepath)
                deleted_links_count += 1

    logger.info(
        f"After inspecting {dir_path}, identified {broken_links_count} broken links and deleted {deleted_links_count} of them."
    )


@dag(
    schedule_interval="0 0 * * 0",  # Runs every Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
)
def example_cosmos_cleanup_dir_dag():

    @task()
    def clear_dbt_project_dir(dir_path: str, should_delete: bool, session=None):
        """ """
        identify_broken_symbolic_links(dir_path, should_delete)

    clear_dbt_project_dir(dir_path=dbt_project_folder, should_delete=True)


# [END dirty_dir_example]

example_cosmos_cleanup_dir_dag()

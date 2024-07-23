"""
We've observed users who had dbt project directories containing symbolic links to files that no longer existed.

Although this issue was not created by Cosmos itself, since this issue was already observed by two users, we thought it
was useful to give an example DAG illustrating how to clean the problematic directories.

Assuming the cause of the issue no longer exists, this DAG can be run only once.
"""

# [START dirty_dir_example]
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from cosmos.cleanup import identify_broken_symbolic_links

dbt_project_folder = Path(__file__).parent / "dbt"


@dag(
    schedule_interval="0 0 * * 0",  # Runs every Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
)
def example_cosmos_cleanup_dir_dag():

    @task()
    def clear_broken_symlinks(session=None):
        identify_broken_symbolic_links(dir_path=dbt_project_folder, should_delete=True)

    clear_broken_symlinks()


# [END dirty_dir_example]

example_cosmos_cleanup_dir_dag()

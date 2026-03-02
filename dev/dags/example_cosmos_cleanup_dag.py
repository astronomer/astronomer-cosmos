"""
Example of cleanup DAG that can be used to clear cache originated from running the dbt ls command while
parsing the DbtDag or DbtTaskGroup since Cosmos 1.5.
"""

# [START cache_example]
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from cosmos.cache import (
    delete_unused_dbt_ls_cache,
    delete_unused_dbt_ls_remote_cache_files,
    delete_unused_dbt_yaml_selectors_cache,
    delete_unused_dbt_yaml_selectors_remote_cache_files,
)

with DAG(
    dag_id="example_cosmos_cleanup_dag",
    schedule="0 0 * * 0",  # Runs every Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
):

    @task()
    def clear_dbt_ls_cache(session=None):
        """
        Delete the dbt ls cache that has not been used for the last five days.
        """
        delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(days=5))

    clear_dbt_ls_cache()

    @task()
    def clear_dbt_ls_remote_cache(session=None):
        """
        Delete the dbt ls remote cache files that have not been used for the last five days.
        """
        delete_unused_dbt_ls_remote_cache_files(max_age_last_usage=timedelta(days=5))

    clear_dbt_ls_remote_cache()

    @task()
    def clear_dbt_yaml_selectors_cache(session=None):
        """
        Delete the dbt yaml selectors cache that has not been used for the last five days.
        """
        delete_unused_dbt_yaml_selectors_cache(max_age_last_usage=timedelta(days=5))

    clear_dbt_yaml_selectors_cache()

    @task()
    def clear_dbt_yaml_selectors_remote_cache(session=None):
        """
        Delete the dbt yaml selectors remote cache files that have not been used for the last five days.
        """
        delete_unused_dbt_yaml_selectors_remote_cache_files(max_age_last_usage=timedelta(days=5))

    clear_dbt_yaml_selectors_remote_cache()


# [END cache_example]

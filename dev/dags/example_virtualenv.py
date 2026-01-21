"""
An example DAG that uses Cosmos to render a dbt project as an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


# [START virtualenv_example]
@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def example_virtualenv() -> None:
    start_task = EmptyOperator(task_id="start-venv-examples")
    end_task = EmptyOperator(task_id="end-venv-examples")

    # This first task group creates a new Cosmos virtualenv every time a task is run
    # and deletes it afterwards
    # It is much slower than if the user sets the `virtualenv_dir`
    tmp_venv_task_group = DbtTaskGroup(
        group_id="tmp-venv-group",
        # dbt/cosmos-specific parameters
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            # Without setting virtualenv_dir="/some/path/persistent-venv",
            # Cosmos creates a new Python virtualenv for each dbt task being executed
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-postgres"],
            "install_deps": True,
            "emit_datasets": False,  # Example of how to not set inlets and outlets
            # --------------------------------------------------------------------------
            # For the sake of avoiding additional latency observed while uploading files for each of the tasks, the
            # below callback functions to be executed are commented, but you can uncomment them if you'd like to
            # enable callback execution.
            # Callback function to upload files using Airflow Object storage and Cosmos remote_target_path setting
            # "callback": upload_to_cloud_storage,
            # --------------------------------------------------------------------------
            # Callback function if you'd like to upload files from the target directory to remote store e.g. AWS S3 that
            # works with Airflow < 2.8 too
            # "callback": upload_to_aws_s3,
            # "callback_args": {"aws_conn_id": "aws_s3_conn", "bucket_name": "cosmos-artifacts-upload"}
            # --------------------------------------------------------------------------
        },
    )

    # The following task group reuses the Cosmos-managed Python virtualenv across multiple tasks.
    # It runs approximately 70% faster than the previous TaskGroup.
    cached_venv_task_group = DbtTaskGroup(
        group_id="cached-venv-group",
        # dbt/cosmos-specific parameters
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            # We can set the argument `virtualenv_dir` if we want Cosmos to create one Python virtualenv
            # and reuse that to run all the dbt tasks within the same worker node
            virtualenv_dir=Path("/tmp/persistent-venv2"),
        ),
        operator_args={
            "py_system_site_packages": False,
            "py_requirements": ["dbt-postgres"],
            "install_deps": True,
        },
    )

    start_task >> [tmp_venv_task_group, cached_venv_task_group] >> end_task


example_virtualenv()
# [END virtualenv_example]

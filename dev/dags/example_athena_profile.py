"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.

It uses the automatic profile rendering from an Airflow connection.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.athena.access_key import AthenaAccessKeyProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

DATABASE = "AwsDataCatalog"
REGION_NAME = "us-east-1"
S3_STAGING_DIR = "s3://staging-dir-example/"
SCHEMA = "example_schema"


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def example_athena_profile() -> None:
    """
    Turns a dbt project into a TaskGroup with a profile mapping.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=ProfileConfig(
            profile_name="athena",
            target_name="dev",
            profile_mapping=AthenaAccessKeyProfileMapping(
                conn_id="athena_db",
                profile_args={
                    "database": DATABASE,
                    "region_name": REGION_NAME,
                    "s3_staging_dir": S3_STAGING_DIR,
                    "schema": SCHEMA,
                    "threads": 4,
                },
            ),
        ),
        operator_args={"install_deps": True},
        default_args={"retries": 2},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


example_athena_profile()

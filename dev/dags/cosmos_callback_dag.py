"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.io import upload_to_cloud_storage
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)

# [START cosmos_callback_example]
cosmos_callback_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
        # --------------------------------------------------------------
        # Callback function to upload files using Airflow Object storage and Cosmos remote_target_path setting on Airflow 2.8 and above
        "callback": upload_to_cloud_storage,
        # --------------------------------------------------------------
        # Callback function to upload files to AWS S3, works for Airflow < 2.8 too
        # "callback": upload_to_aws_s3,
        # "callback_args": {"aws_conn_id": "aws_s3_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload files to GCP GS, works for Airflow < 2.8 too
        # "callback": upload_to_gcp_gs,
        # "callback_args": {"gcp_conn_id": "gcp_gs_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload files to Azure WASB, works for Airflow < 2.8 too
        # "callback": upload_to_azure_wasb,
        # "callback_args": {"azure_conn_id": "azure_wasb_conn", "container_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="cosmos_callback_dag",
    default_args={"retries": 0},
)
# [END cosmos_callback_example]

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from cosmos import DbtCloneLocalOperator, DbtRunLocalOperator, DbtSeedLocalOperator, ProfileConfig
from cosmos.io import upload_to_aws_s3

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJ_DIR = DBT_ROOT_PATH / "jaffle_shop"
DBT_PROFILE_PATH = DBT_PROJ_DIR / "profiles.yml"
DBT_ARTIFACT = DBT_PROJ_DIR / "target"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILE_PATH,
)


def check_s3_file(bucket_name: str, file_key: str, aws_conn_id: str = "aws_default", **context: Any) -> bool:
    """Check if a file exists in the given S3 bucket."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_key = f"{context['dag'].dag_id}/{context['run_id']}/seed/0/{file_key}"
    print(f"Checking if file {s3_key} exists in S3 bucket...")
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return hook.check_for_key(key=s3_key, bucket_name=bucket_name)


with DAG("example_operators", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    # [START single_operator_callback]
    seed_operator = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
        # --------------------------------------------------------------
        # Callback function to upload artifacts to AWS S3
        callback=upload_to_aws_s3,
        callback_args={"aws_conn_id": "aws_s3_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload artifacts to GCP GS
        # callback=upload_to_gcp_gs,
        # callback_args={"gcp_conn_id": "gcp_gs_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload artifacts to Azure WASB
        # callback=upload_to_azure_wasb,
        # callback_args={"azure_conn_id": "azure_wasb_conn", "container_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
    )
    # [END single_operator_callback]

    check_file_uploaded_task = PythonOperator(
        task_id="check_file_uploaded_task",
        python_callable=check_s3_file,
        op_kwargs={
            "aws_conn_id": "aws_s3_conn",
            "bucket_name": "cosmos-artifacts-upload",
            "file_key": "target/run_results.json",
        },
    )

    run_operator = DbtRunLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="run",
        dbt_cmd_flags=["--models", "stg_customers"],
        install_deps=True,
        append_env=True,
    )

    # [START clone_example]
    clone_operator = DbtCloneLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="clone",
        dbt_cmd_flags=["--models", "stg_customers", "--state", DBT_ARTIFACT],
        install_deps=True,
        append_env=True,
    )
    # [END clone_example]

    # [START seed_local_example]
    seed_raw_orders = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed_raw_orders",
        dbt_cmd_flags=["--select", "raw_orders"],
        install_deps=True
    )
    # [END seed_local_example]

    [seed_operator, seed_raw_orders] >> run_operator >> clone_operator
    [seed_operator, seed_raw_orders] >> check_file_uploaded_task

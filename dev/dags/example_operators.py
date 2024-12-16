import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import DbtCloneLocalOperator, DbtRunLocalOperator, DbtSeedLocalOperator, ProfileConfig
from cosmos.io import upload_artifacts_to_aws_s3

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

with DAG("example_operators", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    # [START single_operator_callback]
    seed_operator = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
        # Callback function to upload artifacts to AWS S3
        callback=upload_artifacts_to_aws_s3,
        callback_args={"aws_conn_id": "aws_s3_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload artifacts to GCP GS
        # "callback": upload_artifacts_to_gcp_gs,
        # "callback_args": {"gcp_conn_id": "gcp_gs_conn", "bucket_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
        # Callback function to upload artifacts to Azure WASB
        # "callback": upload_artifacts_to_azure_wasb,
        # "callback_args": {"azure_conn_id": "azure_wasb_conn", "container_name": "cosmos-artifacts-upload"},
        # --------------------------------------------------------------
    )
    # [END single_operator_callback]

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

    seed_operator >> run_operator >> clone_operator

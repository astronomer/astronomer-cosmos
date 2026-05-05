import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import DbtCloneLocalOperator, DbtRunLocalOperator, DbtSeedLocalOperator, ProfileConfig

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


with DAG("example_fail_on_nothing_to_do", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    stage_customers = DbtRunLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="run",
        dbt_cmd_flags=["--models", "stage_customers"],  # Should really be stg_customers
        install_deps=True,
        append_env=True,
        fail_on_nothing_to_do=True,
    )

    stage_customers

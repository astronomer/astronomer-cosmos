import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos.config import ProfileConfig
from cosmos.operators.local import DbtCloneLocalOperator, DbtRunLocalOperator, DbtSeedLocalOperator

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent.parent.parent / "dev/dags/dbt"
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
    seed_operator = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
    )

    clone_operator = DbtCloneLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="clone",
        dbt_cmd_flags=["--models", "stg_customers", "--state", DBT_ARTIFACT],
        install_deps=True,
        append_env=True,
    )

    run_operator = DbtRunLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="run",
        dbt_cmd_flags=["--models", "stg_customers"],
        install_deps=True,
        append_env=True,
    )

    seed_operator >> [run_operator, clone_operator]

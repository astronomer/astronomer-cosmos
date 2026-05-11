import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.dataset import Dataset, DatasetAlias

from cosmos import (
    DbtRunLocalOperator,
    DbtSeedLocalOperator,
    ProfileConfig,
)

# Set contains to be used by local operators
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

with DAG("example_outlets", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    seed_operator = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
    )

    seed_raw_orders = DbtSeedLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed_raw_orders",
        dbt_cmd_flags=["--select", "raw_orders"],
        install_deps=True,
    )

    run_operator__dataset = DbtRunLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="run__dataset",
        dbt_cmd_flags=["--select", "stg_customers"],
        install_deps=True,
        append_env=True,
        emit_datasets=True,
        outlets=[Dataset("stg_customers__dataset")],
    )

    run_operator__dataset_alias = DbtRunLocalOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="run__dataset_alias",
        dbt_cmd_flags=["--select", "stg_orders"],
        install_deps=True,
        append_env=True,
        emit_datasets=True,
        outlets=[DatasetAlias("stg_orders__dataset_alias")],
    )

    [seed_operator, seed_raw_orders] >> run_operator__dataset >> run_operator__dataset_alias

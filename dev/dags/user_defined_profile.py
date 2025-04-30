"""
A DAG that uses Cosmos with a custom profile.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, LoadMode, ProfileConfig, ProjectConfig, RenderConfig

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROFILES_FILE_PATH = Path(DBT_ROOT_PATH, "jaffle_shop", "profiles.yml")
DBT_LS_PATH = Path(DBT_ROOT_PATH, "jaffle_shop", "dbt_ls_models_staging.txt")


with DAG(
    dag_id="user_defined_profile",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    """
    A DAG that uses Cosmos with a custom profile.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profiles_yml_filepath=PROFILES_FILE_PATH,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS_FILE,
            dbt_ls_path=DBT_LS_PATH,
        ),
        operator_args={"append_env": True, "install_deps": True},
        default_args={"retries": 2},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt

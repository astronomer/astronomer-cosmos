"""
An example DAG that uses Cosmos to render a dbt project as an Airflow TaskGroup.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)

shared_execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.SUBPROCESS,
)


with DAG(
    dag_id="basic_cosmos_task_group",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    customers = DbtTaskGroup(
        group_id="customers",
        project_config=ProjectConfig((DBT_PROJECT_PATH).as_posix(), dbt_vars={"var": "2"}),
        render_config=RenderConfig(
            select=["path:seeds/raw_customers.csv"],
            enable_mock_profile=False,
            env_vars={"PURGE": os.getenv("PURGE", "0")},
            airflow_vars_to_purge_dbt_ls_cache=["purge"],
        ),
        execution_config=shared_execution_config,
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 0},
    )

    orders = DbtTaskGroup(
        group_id="orders",
        project_config=ProjectConfig(
            (DBT_PROJECT_PATH).as_posix(),
        ),
        render_config=RenderConfig(
            select=["path:seeds/raw_orders.csv"],
            enable_mock_profile=False,  # This is necessary to benefit from partial parsing when using ProfileMapping
        ),
        execution_config=shared_execution_config,
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 0},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> customers >> post_dbt
    pre_dbt >> orders >> post_dbt

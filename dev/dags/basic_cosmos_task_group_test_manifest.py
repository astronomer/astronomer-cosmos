"""
An example DAG that uses Cosmos to render a dbt project as an Airflow TaskGroup.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, LoadMode
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

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


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group_test_manifest() -> None:
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    customers = DbtTaskGroup(
        group_id="customers_raw",
        project_config=ProjectConfig((DBT_ROOT_PATH / "altered_jaffle_shop").as_posix(), dbt_vars={"var": "2"}),
        render_config=RenderConfig(
            select=["path:seeds/raw_customers.csv"],
            enable_mock_profile=False,
            env_vars={"PURGE": os.getenv("PURGE", "0")},
            airflow_vars_to_purge_dbt_ls_cache=["purge"], 
        ),
        execution_config=shared_execution_config,
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    customers_ods = DbtTaskGroup(
        group_id="customers_ods",
        project_config=ProjectConfig((DBT_ROOT_PATH / "altered_jaffle_shop").as_posix(), 
                                     manifest_path = os.path.join(DBT_ROOT_PATH, "altered_jaffle_shop", 'target', 'manifest.json'),
                                     dbt_vars={"var": "2"}),
        render_config=RenderConfig(
            select=["+public.customer2"],
            load_method=LoadMode.DBT_MANIFEST,
            enable_mock_profile=False,
            env_vars={"PURGE": os.getenv("PURGE", "0")},
            airflow_vars_to_purge_dbt_ls_cache=["purge"],
        ),
        execution_config=shared_execution_config,
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> customers >> customers_ods >> post_dbt


basic_cosmos_task_group_test_manifest()

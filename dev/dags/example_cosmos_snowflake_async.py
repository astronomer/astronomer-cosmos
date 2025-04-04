import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

DBT_ADAPTER_VERSION = os.getenv("DBT_ADAPTER_VERSION", "1.9")

profile_config = ProfileConfig(
    profile_name="default",  # TODO: Figure this out
    target_name="dev",  # TODO: Figure this out
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
    ),
)


# [START airflow_async_execution_mode_example]
simple_dag_async = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "original_jaffle_shop",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.AIRFLOW_ASYNC,
        async_py_requirements=[f"dbt-snowflake=={DBT_ADAPTER_VERSION}"],
    ),
    render_config=RenderConfig(select=["path:models"], test_behavior=TestBehavior.NONE),
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="example_cosmos_snowflake_async",
    tags=["async"],
    operator_args={"install_deps": True},
)

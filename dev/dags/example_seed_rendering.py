"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG using Cosmos seed rendering behaviors.

This demonstrates how to configure different seed rendering behaviors:
- ALWAYS: Always run seeds (default behavior)
- NONE: Don't render any seeds in the DAG/TaskGroup
- WHEN_SEED_CHANGES: Only run a seed if the CSV file has changed since last execution

Note: Test behavior for seeds is controlled separately via TestBehavior configuration.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import SeedRenderingBehavior, TestBehavior
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

# [START cosmos_seed_always_example]
# Always run seeds - this is the default behavior
seed_always_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    render_config=RenderConfig(seed_rendering_behavior=SeedRenderingBehavior.ALWAYS),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="seed_always_dag",
    default_args={"retries": 0},
)
# [END cosmos_seed_always_example]

# [START cosmos_seed_none_example]
# Don't render any seeds - useful when seeds are managed outside of the DAG
# or when you've already loaded all seed data
seed_none_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    render_config=RenderConfig(seed_rendering_behavior=SeedRenderingBehavior.NONE),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="seed_none_dag",
    default_args={"retries": 0},
)
# [END cosmos_seed_none_example]

# [START cosmos_seed_when_changes_example]
# Only run seeds when the CSV file has changed - useful for optimizing DAG runs
# Note: This is currently only supported with ExecutionMode.LOCAL
seed_when_changes_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    render_config=RenderConfig(seed_rendering_behavior=SeedRenderingBehavior.WHEN_SEED_CHANGES),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="seed_when_changes_dag",
    default_args={"retries": 0},
)
# [END cosmos_seed_when_changes_example]

# [START cosmos_seed_with_test_behavior_example]
# Test behavior is controlled via TestBehavior, not SeedRenderingBehavior
# Here we show how to run seeds always but skip tests using TestBehavior.NONE
seed_without_tests_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    render_config=RenderConfig(
        seed_rendering_behavior=SeedRenderingBehavior.ALWAYS,
        test_behavior=TestBehavior.NONE,
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="seed_without_tests_dag",
    default_args={"retries": 0},
)
# [END cosmos_seed_with_test_behavior_example]

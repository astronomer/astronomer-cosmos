"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig
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

example_multiple_parents_test = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "multiple_parents_test",
    ),
    profile_config=profile_config,
    # normal dag parameters
    start_date=datetime(2023, 1, 1),
    dag_id="example_multiple_parents_test",
)

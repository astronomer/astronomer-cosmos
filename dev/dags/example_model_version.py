"""
An example DAG that uses Cosmos to render a dbt project as an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# [START templated_conn_id]
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="{{ params.conn_id }}",
        profile_args={"schema": "public"},
    ),
)
# [END templated_conn_id]

# [START local_example]
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "model_version",
    ),
    profile_config=profile_config,
    operator_args={"install_deps": True},
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_model_version",
    default_args={"retries": 0},
    params={"conn_id": "example_conn"},
)
# [END local_example]

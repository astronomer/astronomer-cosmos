"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp_gs_conn",
        disable_event_tracking=True,
    ),
)

dbt_deps_example_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(DBT_ROOT_PATH / "simple", copy_dbt_packages=True),
    profile_config=profile_config,
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_deps_example",
    default_args={"retries": 0},
)

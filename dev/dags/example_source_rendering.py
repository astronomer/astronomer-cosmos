"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG using Cosmos source rendering.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import SourceRenderingBehavior
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

# [START cosmos_source_node_example]

source_rendering_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "altered_jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    render_config=RenderConfig(source_rendering_behavior=SourceRenderingBehavior.ALL),
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="source_rendering_dag",
    default_args={"retries": 2},
    on_warning_callback=lambda context: print(context),
)
# [END cosmos_source_node_example]

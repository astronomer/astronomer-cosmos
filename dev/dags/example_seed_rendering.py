"""
An example Airflow DAG showing how to control the way Cosmos renders and runs dbt seeds using
``RenderConfig.seed_rendering_behavior``.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import SeedRenderingBehavior
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

# [START seed_rendering_example]
example_seed_rendering = DbtDag(
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    render_config=RenderConfig(
        # Render every seed, but only run `dbt seed` when a seed's CSV content has changed
        # since the last successful run.
        seed_rendering_behavior=SeedRenderingBehavior.WHEN_SEED_CHANGES,
    ),
    profile_config=profile_config,
    operator_args={"install_deps": True},
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_seed_rendering",
    default_args={"retries": 0},
)
# [END seed_rendering_example]

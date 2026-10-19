"""
Example DAGs that render a dbt project containing a Snowflake semantic view, built with the
Snowflake-Labs/dbt_semantic_view package, which Cosmos renders as its own resource type
when ``AIRFLOW__COSMOS__ENABLE_SEMANTIC_METRIC_VIEW_RECLASSIFICATION=True`` (see the "Managing semantic metric
views" guide).

They rely on the ``snowflake_default`` connection, for example:

export AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type": "snowflake", "login": "<user>", "password": "<password>", "schema": "public", "extra": {"account": "<account>", "warehouse": "<warehouse>", "database": "<database>", "role": "<role>"}}'

The role needs CREATE SCHEMA on the database. Optional environment variable: ``COSMOS_DEMO_SCHEMA``
(default ``cosmos_demo``).
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ProfileConfig,
    ProjectConfig,
)
from cosmos.profiles import get_automatic_profile_mapping

DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "snowflake_semantic_view"
SCHEMA = os.getenv("COSMOS_DEMO_SCHEMA", "cosmos_demo")

profile_config = ProfileConfig(
    profile_name="snowflake_semantic_view",
    target_name="dev",
    profile_mapping=get_automatic_profile_mapping("snowflake_default", profile_args={"schema": SCHEMA}),
)

snowflake_semantic_view_local = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={"install_deps": True, "full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="snowflake_semantic_view_local",
    default_args={"retries": 0},
    tags=["semantic-view", "snowflake"],
)

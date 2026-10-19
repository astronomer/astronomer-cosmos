"""
Example DAGs that render a dbt project containing a Databricks metric view, which Cosmos renders as its own
resource type when ``AIRFLOW__COSMOS__ENABLE_SEMANTIC_METRIC_VIEW_RECLASSIFICATION=True`` (see the
"Managing semantic metric views" guide).

They rely on the ``databricks_default`` connection, as in example_cosmos_python_models.py, for example:

export AIRFLOW_CONN_DATABRICKS_DEFAULT='{"conn_type": "databricks", "host": "<workspace-host>", "password": "<token>", "extra": {"http_path": "/sql/1.0/warehouses/<warehouse-id>"}}'

Optional environment variables: ``DATABRICKS_CATALOG`` (default ``main``) and ``COSMOS_DEMO_SCHEMA``
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
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "databricks_metric_view"
CATALOG = os.getenv("DATABRICKS_CATALOG", "main")
SCHEMA = os.getenv("COSMOS_DEMO_SCHEMA", "cosmos_demo")

profile_config = ProfileConfig(
    profile_name="databricks_metric_view",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={"catalog": CATALOG, "schema": SCHEMA},
    ),
)

databricks_metric_view_local = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={"full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="databricks_metric_view_local",
    default_args={"retries": 0},
    tags=["metric-view", "databricks"],
)

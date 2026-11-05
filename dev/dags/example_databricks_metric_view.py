"""
Example DAGs (local and watcher) for a dbt project with a Databricks metric view, loaded with ``dbt ls`` and
tested with ``TestBehavior.AFTER_ALL``, rendered as a ``semantic_metric_view`` resource when
``AIRFLOW__COSMOS__ENABLE_SEMANTIC_METRIC_VIEW_RECLASSIFICATION=True`` (see "Managing semantic metric views").

Runs with the ``databricks_default`` connection (see example_cosmos_python_models.py). Optional:
``DATABRICKS_CATALOG`` (default ``main``), ``COSMOS_DEMO_SCHEMA`` (default ``cosmos_demo``).
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "databricks_metric_view"

profile_config = ProfileConfig(
    profile_name="databricks_metric_view",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={
            "catalog": os.getenv("DATABRICKS_CATALOG", "main"),
            "schema": os.getenv("COSMOS_DEMO_SCHEMA", "cosmos_demo"),
        },
    ),
)

databricks_metric_view_local = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL),
    operator_args={"full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="databricks_metric_view_local",
    default_args={"retries": 0},
    tags=["metric-view", "databricks"],
)

databricks_metric_view_watcher = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL),
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
    operator_args={"full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="databricks_metric_view_watcher",
    default_args={"retries": 0},
    tags=["metric-view", "databricks"],
)

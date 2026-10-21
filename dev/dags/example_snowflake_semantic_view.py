"""
Example DAGs (local and watcher) for a dbt project with a Snowflake semantic view (Snowflake-Labs/dbt_semantic_view
package), loaded from the committed manifest, rendered as a ``semantic_metric_view`` resource when ``AIRFLOW__COSMOS__ENABLE_SEMANTIC_METRIC_VIEW_RECLASSIFICATION=True``
(see "Managing semantic metric views").

Runs with the ``snowflake_default`` connection, authenticated with a private key (``private_key_content``).
Optional: ``COSMOS_DEMO_SCHEMA`` (default ``cosmos_demo``) and ``RESOURCE_PREFIX``, which prefixes every object name
so runs can share a schema.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "snowflake_semantic_view"

profile_config = ProfileConfig(
    profile_name="snowflake_semantic_view",
    target_name="dev",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
        conn_id="snowflake_default",
        profile_args={"schema": os.getenv("COSMOS_DEMO_SCHEMA", "cosmos_demo")},
    ),
)

snowflake_semantic_view_local = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH, manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json"),
    profile_config=profile_config,
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    operator_args={"install_deps": True, "full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="snowflake_semantic_view_local",
    default_args={"retries": 0},
    tags=["semantic-view", "snowflake"],
)

snowflake_semantic_view_watcher = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH, manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json"),
    profile_config=profile_config,
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
    operator_args={"install_deps": True, "full_refresh": True},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="snowflake_semantic_view_watcher",
    default_args={"retries": 0},
    tags=["semantic-view", "snowflake"],
)

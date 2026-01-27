"""
Example DAG for cross project reference demonstration - Using Manifest Load Mode for both upstream and downstream dbt Projects

This example demonstrates how Cosmos works with dbt-loom for cross-project references
using LoadMode.DBT_MANIFEST for faster DAG parsing (no dbt ls execution required).

Architecture:
    upstream                →         downstream
    ├── stg_customers                 ├── fct_revenue
    ├── stg_orders                    ├── fct_customer_revenue
    ├── stg_order_items               ├── dim_payment_methods
    ├── stg_products                  └── rpt_revenue_summary
    ├── int_orders_enriched
    └── int_customer_orders

Prerequisites:
1. Generate manifest.json for both projects BEFORE deploying:
   cd upstream && dbt compile
   cd downstream && dbt compile

   Or use CI/CD to generate and store manifests in S3/GCS.

2. For remote manifests (S3/GCS/Azure), ensure the connection is configured.

Key Benefits of DBT_MANIFEST mode:
- No dbt installation required on scheduler
- Fastest parsing method
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# Airflow connection ID for PostgreSQL
POSTGRES_CONN_ID = "example_conn"

# Project paths
DBT_UPSTREAM_PROJECT_PATH = DBT_ROOT_PATH / "cross_project" / "upstream"
DBT_DOWNSTREAM_PROJECT_PATH = DBT_ROOT_PATH / "cross_project" / "downstream"

# Manifest paths (local)
UPSTREAM_MANIFEST_PATH = DBT_UPSTREAM_PROJECT_PATH / "target" / "manifest.json"
DOWNSTREAM_MANIFEST_PATH = DBT_DOWNSTREAM_PROJECT_PATH / "target" / "manifest.json"

# =============================================================================
# Alternative: Remote Manifest Paths (S3/GCS/Azure) - Uncomment to use
# =============================================================================
# UPSTREAM_MANIFEST_PATH = "s3://your-bucket/dbt-manifests/upstream/manifest.json"
# DOWNSTREAM_MANIFEST_PATH = "s3://your-bucket/dbt-manifests/downstream/manifest.json"
# MANIFEST_CONN_ID = "aws_default"  # or "google_cloud_default" for GCS


# =============================================================================
# Combined DAG with Task Groups - Using DBT_MANIFEST Load Mode
# =============================================================================

with DAG(
    dag_id="cross_project_manifest_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["dbt-loom", "manifest"],
    doc_md=__doc__,
) as dag:

    # -------------------------------------------------------------------------
    # Upstream Task Group - Core Data Platform (upstream)
    # -------------------------------------------------------------------------

    upstream_profile_config = ProfileConfig(
        profile_name="upstream",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "platform", "threads": 4},
        ),
    )

    upstream_task_group = DbtTaskGroup(
        group_id="upstream",
        project_config=ProjectConfig(
            # Specify the manifest path for faster parsing
            manifest_path=str(UPSTREAM_MANIFEST_PATH),
            project_name="upstream",
            # For remote manifests (S3/GCS/Azure), add:
            # manifest_conn_id=MANIFEST_CONN_ID,
        ),
        profile_config=upstream_profile_config,
        execution_config=ExecutionConfig(
            dbt_project_path=DBT_UPSTREAM_PROJECT_PATH, dbt_executable_path="/usr/local/bin/dbt"
        ),
        render_config=RenderConfig(
            # Use manifest-based parsing (no dbt ls required)
            load_method=LoadMode.DBT_MANIFEST,
            # Note: dbt_deps is not needed for manifest mode parsing
            # but you may still want install_deps=True for task execution
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # -------------------------------------------------------------------------
    # Downstream Task Group - Finance Domain Models
    # -------------------------------------------------------------------------

    downstream_profile_config = ProfileConfig(
        profile_name="downstream",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "finance"},
        ),
    )

    # Environment variables for dbt-loom to find the upstream manifest
    # dbt_loom_env_vars = {
    #     "PLATFORM_MANIFEST_PATH": str(DBT_UPSTREAM_PROJECT_PATH / "target" / "manifest.json"),
    # }

    downstream_task_group = DbtTaskGroup(
        group_id="downstream_finance",
        project_config=ProjectConfig(
            # Specify the manifest path for faster parsing
            manifest_path=str(DOWNSTREAM_MANIFEST_PATH),
            project_name="downstream",
            # For remote manifests (S3/GCS/Azure), add:
            # manifest_conn_id=MANIFEST_CONN_ID,
            # For dbt loom environment variable configured upstream project's manifest
            # env_vars=dbt_loom_env_vars,
        ),
        profile_config=downstream_profile_config,
        execution_config=ExecutionConfig(
            dbt_project_path=DBT_DOWNSTREAM_PROJECT_PATH, dbt_executable_path="/usr/local/bin/dbt"
        ),
        render_config=RenderConfig(
            # Use manifest-based parsing (no dbt ls required)
            load_method=LoadMode.DBT_MANIFEST,
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # Chain: Upstream runs first, then Downstream
    upstream_task_group >> downstream_task_group

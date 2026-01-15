"""
Example DAG for dbt Loom demonstration - Upstream and Downstream dbt Projects

This example demonstrates how Cosmos works with dbt-loom for cross-project references.

Architecture:
    dbt_loom_upstream_platform    →  dbt_loom_downstream_finance
    ├── stg_customers                 ├── fct_revenue
    ├── stg_orders                    ├── fct_customer_revenue
    ├── stg_order_items               ├── dim_payment_methods
    ├── stg_products                  └── rpt_revenue_summary
    ├── int_orders_enriched
    └── int_customer_orders

The downstream project uses dbt-loom to reference upstream models via:
    ref('dbt_loom_upstream_platform', 'stg_customers')

Key Points:
1. Upstream project must generate manifest.json first (via dbt parse/compile/ls)
2. Downstream project must be able to query upstream tables (same DB, cross-DB, etc.)
3. Cosmos correctly handles dbt-loom's external node references (skips them)

Database Setup (this example):
- Upstream models: 'platform' schema
- Downstream models: 'finance' schema
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# Airflow connection ID for PostgreSQL
POSTGRES_CONN_ID = "example_conn"

# Project paths
DBT_UPSTREAM_PROJECT_PATH = DBT_ROOT_PATH / "dbt_loom_upstream_platform"
DBT_DOWNSTREAM_PROJECT_PATH = DBT_ROOT_PATH / "dbt_loom_downstream_finance"


# =============================================================================
# Combined DAG with Task Groups - Upstream runs first, then Downstream
# =============================================================================

with DAG(
    dag_id="dbt_loom_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},
    tags=["dbt-loom"],
    doc_md=__doc__,
) as dag:

    # -------------------------------------------------------------------------
    # Upstream Task Group - Core Data Platform (dbt_loom_upstream_platform)
    # -------------------------------------------------------------------------
    # Contains foundational models (staging, intermediate) exposed as public
    # models for the downstream project to reference via dbt-loom.

    upstream_profile_config = ProfileConfig(
        profile_name="dbt_loom_upstream_platform",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "platform"},
        ),
    )

    upstream_task_group = DbtTaskGroup(
        group_id="upstream_platform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_UPSTREAM_PROJECT_PATH,
        ),
        profile_config=upstream_profile_config,
        render_config=RenderConfig(
            dbt_deps=True,
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # -------------------------------------------------------------------------
    # Downstream Task Group - Finance Domain Models (dbt_loom_downstream_finance)
    # -------------------------------------------------------------------------
    # Uses dbt-loom to reference public models from the upstream project.
    # Cosmos skips external nodes (those without file paths) during parsing
    # and only creates tasks for this project's own models.

    downstream_profile_config = ProfileConfig(
        profile_name="dbt_loom_downstream_finance",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "finance"},
        ),
    )

    # Environment variables for dbt-loom to find the upstream manifest
    dbt_loom_env_vars = {
        "PLATFORM_MANIFEST_PATH": str(DBT_UPSTREAM_PROJECT_PATH / "target" / "manifest.json"),
    }

    downstream_task_group = DbtTaskGroup(
        group_id="downstream_finance",
        project_config=ProjectConfig(
            dbt_project_path=DBT_DOWNSTREAM_PROJECT_PATH,
        ),
        profile_config=downstream_profile_config,
        render_config=RenderConfig(
            dbt_deps=True,
            env_vars=dbt_loom_env_vars,
        ),
        operator_args={
            "install_deps": True,
            "env": dbt_loom_env_vars,
        },
    )

    # Chain: Upstream runs first, then Downstream
    upstream_task_group >> downstream_task_group

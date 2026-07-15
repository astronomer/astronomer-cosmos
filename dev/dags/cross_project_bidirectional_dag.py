"""
Example DAG for a BIDIRECTIONAL cross-project reference demonstration - Using Manifest Load Mode for both projects

Unlike ``cross_project_manifest_dag.py`` / ``cross_project_dbt_ls_dag.py``, where a single
"upstream" project publishes models consumed by a "downstream" project, this example has
TWO projects that each expose a public model consumed by the other:

Architecture:
    project_a                          project_b
    └── stg_a_customers (public)  <---  consumed by rpt_b_customer_summary
    project_a.rpt_a_customer_orders  --->  consumes project_b.stg_b_orders (public)

    project_b
    └── stg_b_orders (public)  <---  consumed by rpt_a_customer_orders
    project_b.rpt_b_customer_summary  --->  consumes project_a.stg_a_customers (public)

This is a "diamond", not a cycle: the two public leaf models don't depend on each other, and
the two report models don't depend on each other either - each just depends on the *other*
project's leaf model. Each project is split into a "stage1" (public leaf) and "stage2" (report)
TaskGroup so both leaves materialize before either report model runs.

For the manifest-generation bootstrap order this example requires, and why Cosmos needs the
Airflow-level ordering built by hand instead of relying on dbt-loom, see:
https://astronomer.github.io/astronomer-cosmos/guides/multi_project/multi-project.html#bidirectional-cross-project-references
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
DBT_PROJECT_A_PATH = DBT_ROOT_PATH / "cross_project" / "project_a_bidirectional"
DBT_PROJECT_B_PATH = DBT_ROOT_PATH / "cross_project" / "project_b_bidirectional"

# Manifest paths (local)
PROJECT_A_MANIFEST_PATH = DBT_PROJECT_A_PATH / "target" / "manifest.json"
PROJECT_B_MANIFEST_PATH = DBT_PROJECT_B_PATH / "target" / "manifest.json"

# dbt executable path, that contains the dbt-loom adapter
DEFAULT_DBT_EXECUTABLE_PATH = Path(__file__).parent.parent.parent / "venv-subprocess" / "bin" / "dbt"
DBT_EXECUTABLE_PATH = Path(os.getenv("DBT_EXECUTABLE_PATH", DEFAULT_DBT_EXECUTABLE_PATH))


def _profile_config(profile_name: str, schema: str) -> ProfileConfig:
    return ProfileConfig(
        profile_name=profile_name,
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": schema, "threads": 4},
        ),
    )


def _task_group(group_id: str, project_path: Path, manifest_path: Path, project_name: str, schema: str, select):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            # Specify the manifest path for faster parsing
            manifest_path=str(manifest_path),
            project_name=project_name,
        ),
        profile_config=_profile_config(project_name, schema),
        execution_config=ExecutionConfig(dbt_project_path=project_path, dbt_executable_path=DBT_EXECUTABLE_PATH),
        render_config=RenderConfig(
            # Use manifest-based parsing (no dbt ls required)
            load_method=LoadMode.DBT_MANIFEST,
            select=select,
        ),
        operator_args={
            "install_deps": True,
        },
    )


# [START cross_project_bidirectional_dag]
# =============================================================================
# Combined DAG with Task Groups - Using DBT_MANIFEST Load Mode
# =============================================================================

with DAG(
    dag_id="cross_project_bidirectional_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["dbt-loom", "manifest", "bidirectional"],
    doc_md=__doc__,
) as dag:

    # Both leaf (stage1) models are independent of each other and run first.
    # The "+" pulls in each model's seed as well.
    a_stage1 = _task_group(
        "project_a_stage1",
        DBT_PROJECT_A_PATH,
        PROJECT_A_MANIFEST_PATH,
        "project_a",
        "project_a",
        select=["+stg_a_customers"],
    )

    b_stage1 = _task_group(
        "project_b_stage1",
        DBT_PROJECT_B_PATH,
        PROJECT_B_MANIFEST_PATH,
        "project_b",
        "project_b",
        select=["+stg_b_orders"],
    )

    # Both report (stage2) models each need BOTH leaves materialized first.
    a_stage2 = _task_group(
        "project_a_stage2",
        DBT_PROJECT_A_PATH,
        PROJECT_A_MANIFEST_PATH,
        "project_a",
        "project_a",
        select=["rpt_a_customer_orders"],
    )

    b_stage2 = _task_group(
        "project_b_stage2",
        DBT_PROJECT_B_PATH,
        PROJECT_B_MANIFEST_PATH,
        "project_b",
        "project_b",
        select=["rpt_b_customer_summary"],
    )

    [a_stage1, b_stage1] >> a_stage2
    [a_stage1, b_stage1] >> b_stage2
# [END cross_project_bidirectional_dag]

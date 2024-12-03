"""
An example DAG that demonstrates how to walk over the dbt graph. It also shows how to use the mapping from
{dbt graph unique_id} -> {Airflow tasks/task groups}.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.operators.empty import EmptyOperator

from cosmos import DbtDag, DbtResourceType, ProfileConfig, ProjectConfig
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

# [START example_tasks_map]
with DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "jaffle_shop",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="customized_cosmos_dag",
    default_args={"retries": 2},
) as dag:
    # Walk the dbt graph
    for unique_id, dbt_node in dag.dbt_graph.filtered_nodes.items():
        # Filter by any dbt_node property you prefer. In this case, we are adding upstream tasks to source nodes.
        if dbt_node.resource_type == DbtResourceType.SOURCE:
            # Look up the corresponding Airflow task or task group in the DbtToAirflowConverter.tasks_map property.
            task = dag.tasks_map[unique_id]
            # Create a task upstream of this Airflow source task/task group.
            upstream_task = EmptyOperator(task_id=f"upstream_of_{unique_id}")
            upstream_task >> task
# [END example_tasks_map]

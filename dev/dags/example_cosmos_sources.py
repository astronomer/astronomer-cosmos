"""
This example DAG illustrates how to customize the way a dbt node is converted into an Airflow task or task group when
using Cosmos.

There are circumstances when choosing specific Airflow operators to represent a dbt node is helpful.
An example could be to use an S3 sensor to represent dbt sources or to create custom operators to handle exposures.
Your pipeline may even have specific node types not part of the standard dbt definitions.

When defining the mapping for a new type that is not part of Cosmos' ``DbtResourceType`` enumeration,
users should use the syntax ``DbtResourceType("new-node-type")`` as opposed to ``DbtResourceType.EXISTING_TYPE``.
It will dynamically add the new type to the enumeration ``DbtResourceType`` so that Cosmos can parse these dbt nodes and
convert them into the Airflow DAG.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.operators.dummy import DummyOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

DBT_SQLITE_PATH = str(DEFAULT_DBT_ROOT_PATH / "data")


profile_config = ProfileConfig(
    profile_name="simple",
    target_name="dev",
    profiles_yml_filepath=(DBT_ROOT_PATH / "simple/profiles.yml"),
)


# [START custom_dbt_nodes]
# Cosmos will use this function to generate a DummyOperator task when it finds a source node, in the manifest.
# A more realistic use case could be to use an Airflow sensor to represent a source.
def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of DummyOperator to represent a dbt "source" node.
    """
    return DummyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source")


# Cosmos will use this function to generate a DummyOperator task when it finds a exposure node, in the manifest.
def convert_exposure(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of DummyOperator to represent a dbt "exposure" node.
    """
    return DummyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_exposure")


# Use `RenderConfig` to tell Cosmos, given a node type, how to convert a dbt node into an Airflow task or task group.
# In this example, we are telling Cosmos how to convert dbt source and exposure nodes.
# When building the Airflow DAG, if the user defined the conversion function, Cosmos will use it.
# Otherwise, it will use its standard conversion function.
render_config = RenderConfig(
    node_converters={
        DbtResourceType("source"): convert_source,  # known dbt node type to Cosmos (part of DbtResourceType)
        DbtResourceType("exposure"): convert_exposure,  # dbt node type new to Cosmos (will be added to DbtResourceType)
    }
)

# `ProjectConfig` can pass dbt variables and environment variables to dbt commands. Below is an example of
# passing a required env var for the profiles.yml file and a dbt variable that is used for rendering and
# executing dbt models.
project_config = ProjectConfig(
    DBT_ROOT_PATH / "simple",
    env_vars={"DBT_SQLITE_PATH": DBT_SQLITE_PATH},
    dbt_vars={"animation_alias": "top_5_animated_movies"},
)


example_cosmos_sources = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config,
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_cosmos_sources",
)
# [END custom_dbt_nodes]

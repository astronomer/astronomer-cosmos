import os
from datetime import datetime
from pathlib import Path

from airflow.operators.dummy import DummyOperator

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import DbtResourceType

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

os.environ["DBT_SQLITE_PATH"] = str(DEFAULT_DBT_ROOT_PATH / "simple")

"""
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)
"""
profile_config = ProfileConfig(
    profile_name="simple",
    target_name="dev",
    profiles_yml_filepath=(DBT_ROOT_PATH / "simple/profiles.yml"),
)


def convert_source(dag, task_group, node, **kwargs):
    return DummyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source")


render_config = RenderConfig(dbt_resource_converter={DbtResourceType.SOURCE: convert_source})

# [START local_example]
example_cosmos_sources = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "simple",
    ),
    profile_config=profile_config,
    render_config=render_config,
    operator_args={"append_env": True},
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_cosmos_sources",
)
# [END local_example]

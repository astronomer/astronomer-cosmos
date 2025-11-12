"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "jaffle_shop")
DBT_PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


operator_args = {
    "install_deps": True,  # install any necessary dependencies before running any dbt command
    "execution_timeout": timedelta(seconds=120),
}

# Currently airflow dags test ignores priority_weight and  weight_rule, for this reason, we're setting the following in the CI only:
if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"


from cosmos.constants import InvocationMode

# [START example_watcher_deferrable]
example_watcher_deferrable = DbtDag(
    # dbt/cosmos-specific parameters
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER, invocation_mode=InvocationMode.DBT_RUNNER),
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=RenderConfig(exclude=["raw_payments"]),
    operator_args=operator_args,
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher",
    default_args={"retries": 0},
)
# [END example_watcher_deferrable]


# This is not being executed in the CI, but it works in Airflow locally via standalone and in Astro CLI
"""
from airflow.models import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup

# [START example_watcher_taskgroup]
with DAG(
    dag_id="example_watcher_taskgroup",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    first_dbt_task_group = DbtTaskGroup(
        group_id="first_dbt_task_group",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
        ),
        render_config=RenderConfig(select=["*customers*"], exclude=["path:seeds"]),
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args=operator_args,
    )

    pre_dbt >> first_dbt_task_group
# [END example_watcher_taskgroup]
"""

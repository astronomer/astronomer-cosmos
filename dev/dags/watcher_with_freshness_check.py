"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    # Airflow 3.1 onwards
    from airflow.sdk import Context, TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.sdk.definitions.context import Context
except ImportError:
    from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, SourceRenderingBehavior
from cosmos.dbt.graph import DbtNode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "altered_jaffle_shop")
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


def freshness_callback(
    context: Context,
    dag: Any,
    task_group: TaskGroup | None,
    nodes: dict[str, DbtNode] | None,
    sources_json: dict[str, Any] | None,
) -> list[tuple[str, str]]:
    return [("model.jaffle_shop.stg_orders", "skipped")]


# [START example_watcher_with_freshness]
example_watcher_with_freshness = DbtDag(
    # dbt/cosmos-specific parameters
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        invocation_mode=InvocationMode.DBT_RUNNER,
        setup_operator_args={"freshness_callback": freshness_callback},
    ),
    render_config=RenderConfig(source_rendering_behavior=SourceRenderingBehavior.ALL),
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args=operator_args,
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_watcher_with_freshness",
    default_args={"retries": 0},
)
# [END example_watcher_with_freshness]

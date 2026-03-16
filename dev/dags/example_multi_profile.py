"""
An example DAG that demonstrates Cosmos multi-profile support.

``profile_config_dict`` lets you assign different ``ProfileConfig`` instances
to individual dbt models within a single DAG, enabling models to run against
different databases or adapters.

How to configure a model to use a specific profile:

.. code-block:: yaml

    # models/schema.yml
    models:
      - name: stg_payments
        meta:
          cosmos:
            profile_config_key: "secondary"   # <-- picks "secondary" from profile_config_dict

      - name: stg_orders
        # no profile_config_key -> uses "default" from profile_config_dict

Rules:
- ``profile_config_dict`` must contain a ``"default"`` key.
- Models without ``profile_config_key`` fall back to ``"default"``.
- ``profile_config`` and ``profile_config_dict`` are mutually exclusive;
  pass only one of them.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "multi_profile"

# [START multi_profile_config]
# "default" profile — used by models that do not specify profile_config_key.
default_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)

# "secondary" profile — used by models that set:
#   meta:
#     cosmos:
#       profile_config_key: "secondary"
secondary_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn_secondary",
        profile_args={"schema": "secondary"},
    ),
)

# profile_config_dict must always contain a "default" key.
# Add as many named profiles as your project requires.
profile_config_dict = {
    "default": default_profile_config,
    "secondary": secondary_profile_config,
}
# [END multi_profile_config]


with DAG(
    dag_id="example_multi_profile",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    # [START multi_profile_task_group]
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_multi_profile",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        render_config=RenderConfig(
            enable_mock_profile=False,
        ),
        execution_config=ExecutionConfig(),
        # Pass profile_config_dict instead of profile_config.
        # Each model selects its profile via meta.cosmos.profile_config_key.
        profile_config_dict=profile_config_dict,
        operator_args={"install_deps": True},
        default_args={"retries": 0},
    )
    # [END multi_profile_task_group]

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> dbt_task_group >> post_dbt

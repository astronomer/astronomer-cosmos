"""
## Jaffle Shop Airflow DAG using ExecutionMode.WATCHER_KUBERNETES
[Jaffle Shop](https://github.com/dbt-labs/jaffle_shop) is a fictional eCommerce store. This dbt project originates from
dbt labs as an example project with dummy data to demonstrate a working dbt core project. This DAG uses the cosmos dbt
parser to generate an Airflow TaskGroup from the dbt project folder.


The step-by-step to run this DAG are described in:
https://astronomer.github.io/astronomer-cosmos/getting_started/kubernetes.html#kubernetes

"""

import os
from pathlib import Path

from airflow.providers.cncf.kubernetes.secret import Secret
from pendulum import datetime

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent / "dbt"

DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
AIRFLOW_PROJECT_DIR = DBT_ROOT_PATH / "jaffle_shop"

K8S_PROJECT_DIR = "dags/dbt/jaffle_shop"

DBT_IMAGE = "dbt-jaffle-shop:1.0.0"

project_seeds = [{"project": "jaffle_shop", "seeds": ["raw_customers", "raw_payments", "raw_orders"]}]

postgres_password_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_PASSWORD",
    secret="postgres-secrets",
    key="password",
)

postgres_host_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_HOST",
    secret="postgres-secrets",
    key="host",
)

operator_args = {
    "deferrable": False,
    "image": DBT_IMAGE,
    "get_logs": True,
    "is_delete_operator_pod": False,
    "log_events_on_failure": True,
    "secrets": [postgres_password_secret, postgres_host_secret],
    "env_vars": {
        "POSTGRES_DB": "postgres",
        "POSTGRES_SCHEMA": "public",
        "POSTGRES_USER": "postgres",
    },
    "retry": 0,
}

# Currently airflow dags test ignores priority_weight and  weight_rule, for this reason, we're setting the following in the CI only:
if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"

dag = DbtDag(
    dag_id="jaffle_shop_watcher_kubernetes",
    start_date=datetime(2022, 11, 27),
    doc_md=__doc__,
    catchup=False,
    # Cosmos-specific parameters:
    project_config=ProjectConfig(),
    profile_config=ProfileConfig(
        profile_name="postgres_profile",
        target_name="dev",
        # The following profile mapping works for the DAG parsing
        # However, it is not exposed during the K8s pod operators execution
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_default",
            profile_args={
                "schema": "public",
            },
        ),
    ),
    render_config=RenderConfig(dbt_project_path=AIRFLOW_PROJECT_DIR),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER_KUBERNETES,
        dbt_project_path=K8S_PROJECT_DIR,
    ),
    operator_args=operator_args,
)

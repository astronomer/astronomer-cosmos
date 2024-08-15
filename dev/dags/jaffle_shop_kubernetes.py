"""
## Jaffle Shop DAG
[Jaffle Shop](https://github.com/dbt-labs/jaffle_shop) is a fictional eCommerce store. This dbt project originates from
dbt labs as an example project with dummy data to demonstrate a working dbt core project. This DAG uses the cosmos dbt
parser to generate an Airflow TaskGroup from the dbt project folder.


The step-by-step to run this DAG are described in:
https://astronomer.github.io/astronomer-cosmos/getting_started/kubernetes.html#kubernetes

"""

from pathlib import Path

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from pendulum import datetime

from cosmos import (
    DbtSeedKubernetesOperator,
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_IMAGE = "dbt-jaffle-shop:1.0.0"

PROJECT_PATH = Path("/home/runner/work/astronomer-cosmos/astronomer-cosmos/dev/dags/dbt/jaffle_shop")
# PROJECT_PATH = Path("/Users/pankaj/Documents/astro_code/astronomer-cosmos/dev/dags/dbt/jaffle_shop")
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

with DAG(
    dag_id="jaffle_shop_kubernetes",
    start_date=datetime(2022, 11, 27),
    doc_md=__doc__,
    catchup=False,
) as dag:
    # [START kubernetes_seed_example]
    load_seeds = DbtSeedKubernetesOperator(
        task_id="load_seeds",
        project_dir="dags/dbt/jaffle_shop",
        get_logs=True,
        schema="public",
        image=DBT_IMAGE,
        is_delete_operator_pod=False,
        secrets=[postgres_password_secret, postgres_host_secret],
        profile_config=ProfileConfig(
            profile_name="postgres_profile",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={
                    "schema": "public",
                },
            ),
        ),
    )
    # [END kubernetes_seed_example]

    # [START kubernetes_tg_example]
    run_models = DbtTaskGroup(
        profile_config=ProfileConfig(
            profile_name="postgres_profile",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_default",
                profile_args={
                    "schema": "public",
                },
            ),
        ),
        project_config=ProjectConfig(dbt_project_path=PROJECT_PATH),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
            dbt_executable_path="/usr/local/bin/dbt",
        ),
        operator_args={
            "image": DBT_IMAGE,
            "get_logs": True,
            "is_delete_operator_pod": False,
            "secrets": [postgres_password_secret, postgres_host_secret],
        },
    )
    # [END kubernetes_tg_example]

    load_seeds >> run_models

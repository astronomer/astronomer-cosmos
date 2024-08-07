"""
## Jaffle Shop DAG
[Jaffle Shop](https://github.com/dbt-labs/jaffle_shop) is a fictional eCommerce store. This dbt project originates from
dbt labs as an example project with dummy data to demonstrate a working dbt core project. This DAG uses the cosmos dbt
parser to generate an Airflow TaskGroup from the dbt project folder.


The step-by-step to run this DAG are described in:
https://astronomer.github.io/astronomer-cosmos/getting_started/kubernetes.html#kubernetes

"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from pendulum import datetime

from cosmos import (
    DbtSeedKubernetesOperator,
    ProfileConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

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

with DAG(
    dag_id="jaffle_shop_kubernetes",
    start_date=datetime(2022, 11, 27),
    doc_md=__doc__,
    catchup=False,
) as dag:
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

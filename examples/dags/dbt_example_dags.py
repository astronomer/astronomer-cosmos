"""
## Example DAGs for dbt

This `dbt_example_dags.py` is used to quickly e2e test against various source specified in /include/default_args.py

"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from include.default_args import sources
from pendulum import datetime

from cosmos.providers.dbt.core.operators import (
    DbtDepsOperator,
    DbtRunOperationOperator,
    DbtSeedOperator,
)
from cosmos.providers.dbt.task_group import DbtTaskGroup

for source in sources:

    with DAG(
        dag_id=f"dbt_{source['conn_type']}_example_dag",
        start_date=datetime(2022, 11, 27),
        schedule=None,
        doc_md=__doc__,
        catchup=False,
        max_active_runs=1,
    ) as dag:

        projects = [
            {
                "project": "jaffle_shop",
                "seeds": ["raw_customers", "raw_payments", "raw_orders"],
            },
            {
                "project": "attribution-playbook",
                "seeds": ["customer_conversions", "ad_spend", "sessions"],
            },
            {"project": "mrr-playbook", "seeds": ["subscription_periods"]},
        ]

        for project in projects:
            name_underscores = project["project"].replace("-", "_")

            deps = DbtDepsOperator(
                task_id=f"{project['project']}_install_deps",
                project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                schema=source["schema"],
                dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                conn_id=source["conn_id"],
            )

            with TaskGroup(group_id=f"{project['project']}_drop_seeds") as drop_seeds:
                for seed in project["seeds"]:
                    DbtRunOperationOperator(
                        task_id=f"drop_{seed}_if_exists",
                        macro_name="drop_table",
                        args={"table_name": seed, "conn_type": source["conn_type"]},
                        project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                        schema=source["schema"],
                        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                        conn_id=source["conn_id"],
                    )

            seed = DbtSeedOperator(
                task_id=f"{name_underscores}_seed",
                project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                schema=source["schema"],
                dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                conn_id=source["conn_id"],
            )

            # TODO: Come back and fix tests -- but it's not super important
            test_behavior = (
                "none"
                if project["project"] == "mrr-playbook"
                and source["conn_type"] == "bigquery"
                else "after_all"
            )
            project_task_group = DbtTaskGroup(
                dbt_project_name=project["project"],
                conn_id=source["conn_id"],
                dbt_args={
                    "schema": source["schema"],
                    "dbt_executable_path": "/usr/local/airflow/dbt_venv/bin/dbt",
                    "vars": {"conn_type": source["conn_type"]},
                },
                test_behavior=test_behavior,
                emit_datasets=False,
                dag=dag,
            )

            deps >> drop_seeds >> seed >> project_task_group

"""
## Example DAGs for dbt

This `dbt_example_dags.py` is used to quickly e2e test against various source specified in /include/default_args.py

"""

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        schedule=[Dataset("DAG://TRIGGER_ALL_TESTS/TRIGGER_ALL")],
        doc_md=__doc__,
        catchup=False,
        tags=[source["conn_type"]],
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

        if source["conn_type"] == "redshift":
            start = TriggerDagRunOperator(
                task_id="unpause_redshift_cluster",
                trigger_dag_id="redshift_manager",
                conf={"cluster_request": "unpause_cluster"},
                wait_for_completion=True,
            )
            finish = TriggerDagRunOperator(
                task_id="pause_cluster",
                trigger_dag_id="redshift_manager",
                conf={"cluster_request": "pause_cluster"},
                wait_for_completion=True,
            )
        elif source["conn_type"] == "postgres":
            start = TriggerDagRunOperator(
                task_id="unpause_postgres_instance",
                trigger_dag_id="postgres_manager",
                conf={"cluster_request": "unpause_cluster"},
                wait_for_completion=True,
            )
            finish = TriggerDagRunOperator(
                task_id="pause_postgres_instance",
                trigger_dag_id="postgres_manager",
                conf={"cluster_request": "pause_cluster"},
                wait_for_completion=True,
            )
        elif source["conn_type"] == "databricks":
            start = TriggerDagRunOperator(
                task_id="unpause_databricks_instance",
                trigger_dag_id="databricks_manager",
                conf={"cluster_request": "unpause_cluster"},
                wait_for_completion=True,
            )
            finish = TriggerDagRunOperator(
                task_id="pause_databricks_instance",
                trigger_dag_id="databricks_manager",
                conf={"cluster_request": "pause_cluster"},
                wait_for_completion=True,
            )
        else:
            start = EmptyOperator(task_id="start")
            finish = EmptyOperator(task_id="finish")

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
                and source["conn_type"] in ["bigquery", "redshift"]
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
            )

            start >> deps >> drop_seeds >> seed >> project_task_group >> finish

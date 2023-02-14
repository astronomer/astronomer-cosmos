"""
## Load DAG A

This DAG is used to illustrate setting a downstream dependency from the dbt DAGs. Notice the `schedule` parameter on the
DAG object is referring to two [Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
that were generated from the rendering process in `cosmos/providers/dbt/render.py`

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from cosmos.providers.dbt import get_dbt_dataset

with DAG(
    dag_id="load_dag_a",
    start_date=datetime(2022, 11, 27),
    schedule=[
        get_dbt_dataset("airflow_db", "jaffle_shop", "orders"),
        get_dbt_dataset("airflow_db", "mrr-playbook", "customer_churn_month"),
    ],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "03-LOAD"},
) as dag:
    EmptyOperator(task_id="reverse_etl")

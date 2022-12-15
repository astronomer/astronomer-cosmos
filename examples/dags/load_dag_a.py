"""
## Load DAG A

This DAG is used to illustrate setting a downstream dependency from the dbt DAGs. Notice the `schedule` parameter on the
DAG object is referring to two [Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
that were generated from the dbt parsing utility in `/include/utils/dbt_dag_parser.py`

"""

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="load_dag_a",
    start_date=datetime(2022, 11, 27),
    schedule=[
        Dataset("DBT://AIRFLOW_DB/JAFFLE_SHOP/ORDERS"),
        Dataset("DBT://AIRFLOW_DB/MRR-PLAYBOOK/CUSTOMER_CHURN_MONTH"),
    ],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "03-LOAD"},
) as dag:

    EmptyOperator(task_id="reverse_etl")

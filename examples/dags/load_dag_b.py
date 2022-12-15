"""
## Load DAG B

This DAG is used to illustrate setting a downstream dependency from the dbt DAGs. Notice the `schedule` parameter on the
DAG object is referring to a [Dataset](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
that was generated from the dbt parsing utility in `/include/utils/dbt_dag_parser.py`

"""

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="load_dag_b",
    start_date=datetime(2022, 11, 27),
    schedule=[Dataset("DBT://AIRFLOW_DB/ATTRIBUTION-PLAYBOOK/ATTRIBUTION_TOUCHES")],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "03-LOAD"},
) as dag:

    EmptyOperator(task_id="reporting_workflow")

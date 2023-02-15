"""
## Load DAG B

This DAG is used to illustrate setting a downstream dependency from the dbt DAGs. Notice the `schedule` parameter on the
DAG object is referring to a [Dataset](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
that was generated from the rendering process in `cosmos/providers/dbt/render.py`

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from cosmos.providers.dbt import get_dbt_dataset

with DAG(
    dag_id="load_dag_b",
    start_date=datetime(2022, 11, 27),
    schedule=[
        get_dbt_dataset("airflow_db", "attribution-playbook", "attribution_touches")
    ],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "03-LOAD"},
) as dag:
    EmptyOperator(task_id="reporting_workflow")

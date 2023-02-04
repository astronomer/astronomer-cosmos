"""
triggers all test dags
"""

import pendulum
from airflow import Dataset
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="trigger_all_tests",
    schedule=None,  # triggered manually
    start_date=pendulum.datetime(2023, 2, 2, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
):

    trigger_all = EmptyOperator(
        task_id="trigger_all", outlets=[Dataset("DAG://TRIGGER_ALL_TESTS/TRIGGER_ALL")]
    )

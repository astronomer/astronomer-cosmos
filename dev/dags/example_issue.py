from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator



with DAG(dag_id='empty_operator_example', start_date=datetime(2022, 1, 1), schedule_interval=None) as dag:

    task1 = EmptyOperator(
        task_id='empty_task1',
        dag=dag,
        outlets=[Dataset("postgres://postgres:5432/postgres.dbt.stg_customers")]
    )
    
    task2 = EmptyOperator(
        task_id='empty_task2',
        dag=dag
    )

    task1 >> task2

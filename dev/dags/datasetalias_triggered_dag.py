from datetime import datetime

from airflow import DAG
from airflow.datasets import DatasetAlias
from airflow.operators.empty import EmptyOperator


with DAG(
    "datasetalias_triggered_dag",
    description="A DAG that should be triggered via Dataset/Dataset alias",
    start_date=datetime(2024, 9, 1),
    schedule=[DatasetAlias(name='basic_cosmos_dag__orders__run')],
) as dag:
    t1 = EmptyOperator(
            task_id="task_1",
        )
    t2 = EmptyOperator(
            task_id="task_2",
        )
    t3 = EmptyOperator(
            task_id="task_3",
        )
    
    t1 >> t2 >> t3

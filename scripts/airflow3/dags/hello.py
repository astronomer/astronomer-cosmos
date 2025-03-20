from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Define the default_args dictionary for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "hello_world_bash",
    default_args=default_args,
    description="A simple Hello World Bash command DAG",
    schedule=None,  # None means it doesn't have a regular schedule, we can trigger manually
)

# Define the BashOperator task to execute the Bash command
hello_world_task = BashOperator(
    task_id="hello_world_task",
    bash_command='echo "Hello, World!"',  # The Bash command to execute
    dag=dag,
)

# Set the task to run
hello_world_task

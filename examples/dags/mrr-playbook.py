"""
## MRR Playbook DAG
[MRR Playbook](https://github.com/dbt-labs/mrr-playbook) is a working dbt project demonstrating how to model
subscription revenue. This dbt project originates from dbt labs as an example project with dummy data to demonstrate a
working dbt core project. This DAG uses the cosmos dbt parser to generate a DAG from the dbt project folder

"""

from airflow.datasets import Dataset
from pendulum import datetime

from cosmos.providers.dbt.dag import DbtDag

mrr_playbook = DbtDag(
    dbt_project_name="mrr-playbook",
    conn_id="airflow_db",
    dbt_args={"schema": "public", "python_venv": "/usr/local/airflow/dbt_venv"},
    dag_id="mrr_playbook",
    start_date=datetime(2022, 11, 27),
    schedule=[Dataset("SEED://MRR_PLAYBOOK")],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "02-TRANSFORM"},
)

mrr_playbook

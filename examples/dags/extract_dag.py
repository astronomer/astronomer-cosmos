"""
## Extract DAG

This DAG is used to illustrate setting an upstream dependency from the dbt DAGs. Notice the `outlets` parameter on the
`DbtSeedOperator` objects are creating
[Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html) these are used in the
`schedule` parameter of the dbt DAGs (`attribution-playbook`, `jaffle_shop`, `mrr-playbook`).

We're using the dbt seed command here to populate the database for the purpose of this demo. Normally an extract DAG
would be ingesting data from various sources (i.e. sftp, blob like s3 or gcs, http endpoint, database, etc.)

"""

from airflow import DAG
from airflow.datasets import Dataset
from pendulum import datetime

from cosmos.providers.dbt.core.operators import DBTSeedOperator

with DAG(
    dag_id="extract_dag",
    start_date=datetime(2022, 11, 27),
    schedule="@daily",
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "01-EXTRACT"},
) as dag:

    for project in ["jaffle_shop", "mrr-playbook", "attribution-playbook"]:
        name_underscores = project.replace("-", "_")
        DBTSeedOperator(
            task_id=f"{name_underscores}_seed",
            project_dir=f"/usr/local/airflow/dbt/{project}",
            schema="public",
            conn_id="airflow_db",
            python_venv="/usr/local/airflow/dbt_venv/bin/activate",
            outlets=[Dataset(f"SEED://{name_underscores.upper()}")],
        )

"""Example DAG for using the DatabricksNotebookOperator."""
import os
import uuid
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from cosmos.providers.databricks.notebook import DatabricksNotebookOperator

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

DATABRICKS_CONN_ID = os.getenv("ASTRO_DATABRICKS_CONN_ID", "databricks_conn")
NEW_CLUSTER_SPEC = {
    "cluster_name": "",
    "spark_version": "11.3.x-scala2.12",
    "aws_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "us-east-2b",
        "spot_bid_price_percent": 100,
        "ebs_volume_count": 0,
    },
    "node_type_id": "i3.xlarge",
    "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
    "enable_elastic_disk": False,
    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    "runtime_engine": "STANDARD",
    "num_workers": 8,
}

dag = DAG(
    dag_id="example_databricks_notebook",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "databricks"],
)
with dag:
    notebook_1 = DatabricksNotebookOperator(
        task_id="notebook_1",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_path="/Shared/Notebook_1",
        notebook_packages=[
            {
                "pypi": {
                    "package": "simplejson==3.18.0",
                    "repo": "https://pypi.org/simple",
                }
            },
            {"pypi": {"package": "Faker"}},
        ],
        source="WORKSPACE",
        job_cluster_key=str(uuid.uuid4()),
        new_cluster=NEW_CLUSTER_SPEC,
    )

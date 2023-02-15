"""Example DAG for using the DatabricksWorkflowTaskGroup and DatabricksNotebookOperator."""
import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from cosmos.providers.databricks.notebook import DatabricksNotebookOperator
from cosmos.providers.databricks.workflow import DatabricksWorkflowTaskGroup

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

DATABRICKS_CONN_ID = os.getenv("ASTRO_DATABRICKS_CONN_ID", "databricks_conn")
job_cluster_spec = [
    {
        "job_cluster_key": "Shared_job_cluster",
        "new_cluster": {
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
        },
    }
]
dag = DAG(
    dag_id="example_databricks_workflow",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "databricks"],
)
with dag:
    # [START howto_databricks_workflow_notebook]
    task_group = DatabricksWorkflowTaskGroup(
        group_id="test_workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_spec,
        notebook_params=[],
    )
    with task_group:
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/daniel@astronomer.io/Test workflow",
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
        )
        notebook_2 = DatabricksNotebookOperator(
            task_id="notebook_2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/daniel@astronomer.io/Test workflow",
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
            notebook_params={
                "foo": "bar",
            },
        )
        notebook_1 >> notebook_2
    # [END howto_databricks_workflow_notebook]

if __name__ == "__main__":
    dag.test()

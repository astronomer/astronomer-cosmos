"""
pauses and starts cloud sql postgres instance for testing
"""

import pendulum
from airflow.decorators import branch_task, task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook

with DAG(
    dag_id="postgres_manager",
    schedule=None,  # triggered by dbt_redshift_example_dag
    start_date=pendulum.datetime(2022, 6, 13, tz="UTC"),
    max_active_runs=1,
    tags=["gcp", "postgres"],
    params={"cluster_request": ""},  # Set parameters as a dictionary
    catchup=True,
    doc_md=__doc__,
):

    finish = EmptyOperator(task_id="finish")

    @branch_task
    def get_cluster_status(**kwargs):
        cloud_sql = CloudSQLHook(api_version="v1beta4", gcp_conn_id="bigquery_default")
        instance_details = cloud_sql.get_instance(
            instance="astronomer-cosmos", project_id="astronomer-success"
        )
        state = instance_details["settings"][
            "activationPolicy"
        ]  # NEVER = paused #ALWAYS = unpaused
        cluster_request = kwargs["dag_run"].conf.get("cluster_request")
        if cluster_request == "unpause_cluster" and state == "ALWAYS":
            return "finish"
        elif cluster_request == "unpause_cluster" and state != "ALWAYS":
            return "start"
        elif cluster_request == "pause_cluster" and state == "ALWAYS":
            return "pause"
        else:
            return "finish"

    @task
    def start():
        cloud_sql = CloudSQLHook(api_version="v1beta4", gcp_conn_id="bigquery_default")
        payload = {"settings": {"activationPolicy": "ALWAYS"}}
        r = cloud_sql.patch_instance(
            body=payload, instance="astronomer-cosmos", project_id="astronomer-success"
        )
        print(r)
        return r

    @task
    def pause():
        cloud_sql = CloudSQLHook(api_version="v1beta4", gcp_conn_id="bigquery_default")
        payload = {"settings": {"activationPolicy": "NEVER"}}
        r = cloud_sql.patch_instance(
            body=payload, instance="astronomer-cosmos", project_id="astronomer-success"
        )
        print(r)
        return r

    get_cluster_status() >> [finish, start(), pause()]

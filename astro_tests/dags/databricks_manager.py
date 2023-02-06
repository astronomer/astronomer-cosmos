"""
pauses and starts databricks instance for testing
"""
import time

import pendulum
from airflow.decorators import branch_task, task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

with DAG(
    dag_id="databricks_manager",
    schedule=None,  # triggered by dbt_databricks_example_dag
    start_date=pendulum.datetime(2022, 6, 13, tz="UTC"),
    max_active_runs=1,
    tags=["azure", "databricks"],
    params={"cluster_request": ""},  # Set parameters as a dictionary
    catchup=True,
    doc_md=__doc__,
):

    finish = EmptyOperator(task_id="finish")

    @branch_task
    def get_cluster_status(**kwargs):
        databricks = BaseDatabricksHook()
        r = databricks._do_api_call(
            ("GET", "api/2.0/clusters/get"), json={"cluster_id": "0202-015137-knh58lbv"}
        )
        cluster_request = kwargs["dag_run"].conf.get("cluster_request")
        state = r["state"]
        if cluster_request == "unpause_cluster" and state == "RUNNING":
            return "finish"
        elif cluster_request == "unpause_cluster" and state != "RUNNING":
            return "start"
        elif cluster_request == "pause_cluster" and state == "RUNNING":
            return "pause"
        else:
            return "finish"

    @task
    def start():
        databricks = BaseDatabricksHook()
        r = databricks._do_api_call(
            ("POST", "api/2.0/clusters/start"),
            json={"cluster_id": "0202-015137-knh58lbv"},
        )
        state = "TERMINATED"
        while state != "RUNNING":
            r = databricks._do_api_call(
                ("GET", "api/2.0/clusters/get"),
                json={"cluster_id": "0202-015137-knh58lbv"},
            )
            state = r["state"]
            time.sleep(30)
        print(r)

    @task
    def pause():
        databricks = BaseDatabricksHook()
        r = databricks._do_api_call(
            ("POST", "api/2.0/clusters/delete"),
            json={"cluster_id": "0202-015137-knh58lbv"},
        )
        state = "RUNNING"
        while state != "TERMINATED":
            r = databricks._do_api_call(
                ("GET", "api/2.0/clusters/get"),
                json={"cluster_id": "0202-015137-knh58lbv"},
            )
            state = r["state"]
            time.sleep(30)
        print(r)

    get_cluster_status() >> [finish, start(), pause()]

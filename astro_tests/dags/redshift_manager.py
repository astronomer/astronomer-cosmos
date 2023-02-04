"""
resuming and pausing clusters requires the following permissions on aws
- redshift:DescribeClusters
- redshift:PauseCluster
- redshift:ResumeCluster
"""

import pendulum
from airflow.decorators import branch_task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)

with DAG(
    dag_id="redshift_manager",
    schedule=None,  # triggered by dbt_redshift_example_dag
    start_date=pendulum.datetime(2022, 6, 13, tz="UTC"),
    max_active_runs=1,
    tags=["aws", "redshift"],
    params={"cluster_request": ""},  # Set parameters as a dictionary
    catchup=True,
    doc_md=__doc__,
):

    finish = EmptyOperator(task_id="finish")

    resume = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift", cluster_identifier="redshift-cluster-1"
    )

    pause = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift",
        cluster_identifier="redshift-cluster-1",
    )

    @branch_task
    def get_cluster_status(**kwargs):
        redshift = RedshiftHook()
        cluster_status = redshift.cluster_status(
            cluster_identifier="redshift-cluster-1"
        )
        cluster_request = kwargs["dag_run"].conf.get("cluster_request")
        if cluster_request == "unpause_cluster" and cluster_status == "available":
            return "finish"
        elif cluster_request == "unpause_cluster" and cluster_status != "available":
            return "resume_redshift"
        elif cluster_request == "pause_cluster" and cluster_status == "available":
            return "pause_redshift"
        else:
            return "finish"

    get_cluster_status() >> [finish, resume, pause]

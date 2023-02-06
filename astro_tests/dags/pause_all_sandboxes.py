"""
pauses all sandboxes at the end of the day
"""
import pendulum
from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="pause_all_sandboxes",
    schedule="0 4 * * *",  # 11pm ST
    start_date=pendulum.datetime(2023, 2, 2, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
):

    for manager_dag in ["postgres_manager", "redshift_manager", "databricks_manager"]:
        TriggerDagRunOperator(
            task_id=f"pause_{manager_dag}",
            trigger_dag_id=manager_dag,
            conf={"cluster_request": "pause_cluster"},
            wait_for_completion=True,
        )

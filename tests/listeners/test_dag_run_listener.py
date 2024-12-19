from datetime import datetime
from pathlib import Path

from airflow.models import DAG

from cosmos import DbtRunLocalOperator, ProfileConfig, ProjectConfig
from cosmos.airflow.dag import DbtDag
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.listeners.dag_run_listener import is_cosmos_dag, total_cosmos_task_groups, total_cosmos_tasks, uses_cosmos
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_ROOT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt"
DBT_PROJECT_NAME = "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


def test_is_cosmos_dag_is_true():
    dag = DbtDag(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "jaffle_shop",
        ),
        profile_config=profile_config,
        start_date=datetime(2023, 1, 1),
        dag_id="basic_cosmos_dag",
    )

    assert is_cosmos_dag(dag)
    assert total_cosmos_task_groups(dag) == 0
    assert uses_cosmos(dag)


def test_total_cosmos_task_groups():
    with DAG("test-id-dbt-compile", start_date=datetime(2022, 1, 1)) as dag:
        _ = DbtTaskGroup(
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "jaffle_shop",
            ),
            profile_config=profile_config,
        )

    assert not is_cosmos_dag(dag)
    assert total_cosmos_task_groups(dag) == 1
    assert uses_cosmos(dag)


def test_total_cosmos_tasks_in_task_group():
    with DAG("test-id-dbt-compile", start_date=datetime(2022, 1, 1)) as dag:
        _ = DbtTaskGroup(
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "jaffle_shop",
            ),
            profile_config=profile_config,
        )

    assert total_cosmos_tasks(dag) == 13
    assert uses_cosmos(dag)


def test_total_cosmos_tasks_is_one():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        run_operator = DbtRunLocalOperator(
            profile_config=profile_config,
            project_dir=DBT_ROOT_PATH / "jaffle_shop",
            task_id="run",
            install_deps=True,
            append_env=True,
        )
        run_operator

    assert not is_cosmos_dag(dag)
    assert total_cosmos_task_groups(dag) == 0
    assert total_cosmos_tasks(dag) == 1
    assert uses_cosmos(dag)


def test_not_cosmos_dag():

    with DAG("test-id-1", start_date=datetime(2022, 1, 1)) as dag:
        pass

    assert not is_cosmos_dag(dag)
    assert total_cosmos_task_groups(dag) == 0
    assert total_cosmos_tasks(dag) == 0
    assert not uses_cosmos(dag)

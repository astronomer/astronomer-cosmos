from datetime import datetime

import pytest
from airflow import DAG

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos.dataset import get_dataset_alias_name

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)


@pytest.mark.parametrize(
    "dag, task_group,task_id,result_identifier",
    [
        (example_dag, None, "task_id", "dag__task_id"),
        (None, TaskGroup(dag=example_dag, group_id="inner_tg"), "task_id", "dag__inner_tg__task_id"),
        (
            None,
            TaskGroup(
                dag=example_dag, group_id="child_tg", parent_group=TaskGroup(dag=example_dag, group_id="parent_tg")
            ),
            "task_id",
            "dag__child_tg__task_id",
        ),
        (
            None,
            TaskGroup(
                dag=example_dag,
                group_id="child_tg",
                parent_group=TaskGroup(
                    dag=example_dag, group_id="mum_tg", parent_group=TaskGroup(dag=example_dag, group_id="nana_tg")
                ),
            ),
            "task_id",
            "dag__child_tg__task_id",
        ),
        (
            None,
            TaskGroup(
                dag=example_dag,
                group_id="another_tg",
            ),
            "another_tg.task_id",  # Airflow injects this during task execution time when outside of standalone
            "dag__another_tg__task_id",
        ),
    ],
)
def test_get_dataset_alias_name(dag, task_group, task_id, result_identifier):
    assert get_dataset_alias_name(dag, task_group, task_id) == result_identifier

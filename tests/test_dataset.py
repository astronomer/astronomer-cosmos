from datetime import datetime

import pytest
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.dataset import get_dataset_alias_name

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)


@pytest.mark.parametrize(
    "dag, task_group, result_identifier",
    [
        (example_dag, None, "dag__task_id"),
        (None, TaskGroup(dag=example_dag, group_id="inner_tg"), "dag__inner_tg__task_id"),
        (
            None,
            TaskGroup(
                dag=example_dag, group_id="child_tg", parent_group=TaskGroup(dag=example_dag, group_id="parent_tg")
            ),
            "dag__parent_tg__child_tg__task_id",
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
            "dag__nana_tg__mum_tg__child_tg__task_id",
        ),
    ],
)
def test_get_dataset_alias_name(dag, task_group, result_identifier):
    assert get_dataset_alias_name(dag, task_group, "task_id") == result_identifier

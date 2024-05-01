import time
from datetime import datetime

import pytest
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.cache import _create_cache_identifier, _get_latest_partial_parse
from cosmos.constants import DBT_PARTIAL_PARSE_FILE_NAME, DBT_TARGET_DIR_NAME

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)


@pytest.mark.parametrize(
    "dag, task_group, result_identifier",
    [
        (example_dag, None, "dag"),
        (None, TaskGroup(dag=example_dag, group_id="inner_tg"), "dag__inner_tg"),
        (
            None,
            TaskGroup(
                dag=example_dag, group_id="child_tg", parent_group=TaskGroup(dag=example_dag, group_id="parent_tg")
            ),
            "dag__parent_tg__child_tg",
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
            "dag__nana_tg__mum_tg__child_tg",
        ),
    ],
)
def test_create_cache_identifier(dag, task_group, result_identifier):
    assert _create_cache_identifier(dag, task_group) == result_identifier


def test_get_latest_partial_parse(tmp_path):
    old_tmp_dir = tmp_path / "old"
    old_tmp_target_dir = old_tmp_dir / DBT_TARGET_DIR_NAME
    old_tmp_target_dir.mkdir(parents=True, exist_ok=True)
    old_partial_parse_filepath = old_tmp_target_dir / DBT_PARTIAL_PARSE_FILE_NAME
    old_partial_parse_filepath.touch()

    # This is necessary in the CI, but not on local MacOS dev env, since the files
    # were being created too quickly and sometimes had the same st_mtime
    time.sleep(1)

    new_tmp_dir = tmp_path / "new"
    new_tmp_target_dir = new_tmp_dir / DBT_TARGET_DIR_NAME
    new_tmp_target_dir.mkdir(parents=True, exist_ok=True)
    new_partial_parse_filepath = new_tmp_target_dir / DBT_PARTIAL_PARSE_FILE_NAME
    new_partial_parse_filepath.touch()

    assert _get_latest_partial_parse(old_tmp_dir, new_tmp_dir) == new_partial_parse_filepath
    assert _get_latest_partial_parse(new_tmp_dir, old_tmp_dir) == new_partial_parse_filepath
    assert _get_latest_partial_parse(old_tmp_dir, old_tmp_dir) == old_partial_parse_filepath
    assert _get_latest_partial_parse(old_tmp_dir, tmp_path) == old_partial_parse_filepath
    assert _get_latest_partial_parse(tmp_path, old_tmp_dir) == old_partial_parse_filepath
    assert _get_latest_partial_parse(tmp_path, tmp_path) is None

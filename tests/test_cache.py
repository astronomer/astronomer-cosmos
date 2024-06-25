import logging
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import call, patch

import pytest
from airflow import DAG
from airflow.models import DagRun, Variable
from airflow.utils.db import create_session
from airflow.utils.task_group import TaskGroup

from cosmos.cache import (
    _copy_partial_parse_to_project,
    _create_cache_identifier,
    _get_latest_partial_parse,
    _update_partial_parse_cache,
    delete_unused_dbt_ls_cache,
)
from cosmos.constants import DBT_PARTIAL_PARSE_FILE_NAME, DBT_TARGET_DIR_NAME

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)
SAMPLE_PARTIAL_PARSE_FILEPATH = Path(__file__).parent / "sample/partial_parse.msgpack"


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


@patch("cosmos.cache.msgpack.unpack", side_effect=ValueError)
def test__copy_partial_parse_to_project_msg_fails_msgpack(mock_unpack, tmp_path, caplog):
    caplog.set_level(logging.INFO)
    source_dir = tmp_path / DBT_TARGET_DIR_NAME
    source_dir.mkdir()
    partial_parse_filepath = source_dir / DBT_PARTIAL_PARSE_FILE_NAME
    shutil.copy(str(SAMPLE_PARTIAL_PARSE_FILEPATH), str(partial_parse_filepath))

    # actual test
    with tempfile.TemporaryDirectory() as tmp_dir:
        _copy_partial_parse_to_project(partial_parse_filepath, Path(tmp_dir))

    assert "Unable to patch the partial_parse.msgpack file due to ValueError()" in caplog.text


@patch("cosmos.cache.shutil.copyfile")
@patch("cosmos.cache.get_partial_parse_path")
def test_update_partial_parse_cache(mock_get_partial_parse_path, mock_copyfile):
    mock_get_partial_parse_path.side_effect = lambda cache_dir: cache_dir / "partial_parse.yml"

    latest_partial_parse_filepath = Path("/path/to/latest_partial_parse.yml")
    cache_dir = Path("/path/to/cache_directory")

    # Expected paths
    cache_path = cache_dir / "partial_parse.yml"
    manifest_path = cache_dir / "manifest.json"

    _update_partial_parse_cache(latest_partial_parse_filepath, cache_dir)

    # Assert shutil.copyfile was called twice with the correct arguments
    calls = [
        call(str(latest_partial_parse_filepath), str(cache_path)),
        call(str(latest_partial_parse_filepath.parent / "manifest.json"), str(manifest_path)),
    ]
    mock_copyfile.assert_has_calls(calls)


@pytest.fixture
def vars_session():
    with create_session() as session:
        var1 = Variable(key="cosmos_cache__dag_a", val='{"dag_id": "dag_a"}')
        var2 = Variable(key="cosmos_cache__dag_b", val='{"dag_id": "dag_b"}')
        var3 = Variable(key="cosmos_cache__dag_c__task_group_1", val='{"dag_id": "dag_c"}')

        dag_run_a = DagRun(
            dag_id="dag_a",
            run_id="dag_a_run_a_week_ago",
            execution_date=datetime.now(timezone.utc) - timedelta(days=7),
            state="success",
            run_type="manual",
        )
        dag_run_b = DagRun(
            dag_id="dag_b",
            run_id="dag_b_run_yesterday",
            execution_date=datetime.now(timezone.utc) - timedelta(days=1),
            state="failed",
            run_type="manual",
        )
        dag_run_c = DagRun(
            dag_id="dag_c",
            run_id="dag_c_run_on_hour_ago",
            execution_date=datetime.now(timezone.utc) - timedelta(hours=1),
            state="running",
            run_type="manual",
        )

        session.add(var1)
        session.add(var2)
        session.add(var3)
        session.add(dag_run_a)
        session.add(dag_run_b)
        session.add(dag_run_c)
        session.commit()

        yield session

        session.query(Variable).filter_by(key="cosmos_cache__dag_a").delete()
        session.query(Variable).filter_by(key="cosmos_cache__dag_b").delete()
        session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").delete()

        session.query(DagRun).filter_by(dag_id="dag_a", run_id="dag_a_run_a_week_ago").delete()
        session.query(DagRun).filter_by(dag_id="dag_b", run_id="dag_b_run_yesterday").delete()
        session.query(DagRun).filter_by(dag_id="dag_c", run_id="dag_c_run_on_hour_ago").delete()
        session.commit()


@pytest.mark.integration
def test_delete_unused_dbt_ls_cache_deletes_a_week_ago_cache(vars_session):
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(days=5), session=vars_session) == 1
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()


@pytest.mark.integration
def test_delete_unused_dbt_ls_cache_deletes_all_cache_five_minutes_ago(vars_session):
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_b").first()
    assert vars_session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").first()
    assert delete_unused_dbt_ls_cache(max_age_last_usage=timedelta(minutes=5), session=vars_session) == 3
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_a").first()
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_b").first()
    assert not vars_session.query(Variable).filter_by(key="cosmos_cache__dag_c__task_group_1").first()

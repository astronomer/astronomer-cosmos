"""Unit tests for cosmos.airflow.task_group module."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dev/dags/dbt/jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)


@patch("cosmos.settings.enable_cache", False)
def _make_task_group(execution_mode):
    """Create a DbtTaskGroup inside a DAG context with caching disabled."""
    with DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1)):
        return DbtTaskGroup(
            group_id="test_group",
            execution_config=ExecutionConfig(execution_mode=execution_mode),
            project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            profile_config=profile_config,
            render_config=RenderConfig(test_behavior=TestBehavior.NONE),
        )


@pytest.mark.parametrize(
    "execution_mode, expected",
    [
        (ExecutionMode.WATCHER, True),
        (ExecutionMode.LOCAL, False),
    ],
)
def test_is_watcher_mode(execution_mode, expected):
    tg = _make_task_group(execution_mode)
    assert tg.is_watcher_mode is expected


@patch("cosmos.settings.propagate_watcher_trigger_rule", True)
def test_rshift_sets_trigger_rule_in_watcher_mode():
    tg = _make_task_group(ExecutionMode.WATCHER)
    task = MagicMock(trigger_rule="all_success")

    tg >> task

    assert task.trigger_rule == "none_failed"


@patch("cosmos.settings.propagate_watcher_trigger_rule", False)
def test_rshift_noop_when_setting_disabled():
    tg = _make_task_group(ExecutionMode.WATCHER)
    task = MagicMock(trigger_rule="all_success")

    tg >> task

    assert task.trigger_rule == "all_success"


@patch("cosmos.settings.propagate_watcher_trigger_rule", True)
def test_rshift_sets_trigger_rule_on_downstream_task_group_roots():
    tg = _make_task_group(ExecutionMode.WATCHER)

    root_task = MagicMock(trigger_rule="all_success")
    downstream_group = MagicMock()
    downstream_group.get_roots.return_value = [root_task]
    # isinstance(item, TaskGroup) check needs to pass
    downstream_group.__class__ = TaskGroup

    tg >> downstream_group

    assert root_task.trigger_rule == "none_failed"


@patch("cosmos.settings.propagate_watcher_trigger_rule", True)
def test_set_downstream_sets_trigger_rule():
    tg = _make_task_group(ExecutionMode.WATCHER)
    task = MagicMock(trigger_rule="all_success")

    tg.set_downstream(task)

    assert task.trigger_rule == "none_failed"


@patch("cosmos.settings.propagate_watcher_trigger_rule", True)
def test_rlshift_sets_trigger_rule():
    tg = _make_task_group(ExecutionMode.WATCHER)
    task = MagicMock(trigger_rule="all_success")

    tg.__rlshift__(task)

    assert task.trigger_rule == "none_failed"

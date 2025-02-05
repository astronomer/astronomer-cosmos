import inspect
import sys
from datetime import datetime
from unittest.mock import patch

import pytest
from airflow.models import BaseOperator
from airflow.utils.context import Context

from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCompileMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtTestMixin,
)


@pytest.mark.skipif(
    (sys.version_info.major, sys.version_info.minor) == (3, 12),
    reason="The error message for the abstract class instantiation seems to have changed between Python 3.11 and 3.12",
)
def test_dbt_base_is_abstract():
    """Tests that the abstract base operator cannot be instantiated since the base_cmd is not defined."""
    expected_error = (
        "Can't instantiate abstract class AbstractDbtBase with abstract methods base_cmd, build_and_run_cmd"
    )
    with pytest.raises(TypeError, match=expected_error):
        AbstractDbtBase(project_dir="project_dir")


@pytest.mark.skipif(
    (sys.version_info.major, sys.version_info.minor) != (3, 12),
    reason="The error message for the abstract class instantiation seems to have changed between Python 3.11 and 3.12",
)
def test_dbt_base_operator_is_abstract_py12():
    """Tests that the abstract base operator cannot be instantiated since the base_cmd is not defined."""
    expected_error = (
        "Can't instantiate abstract class AbstractDbtBase without an implementation for abstract methods "
        "'base_cmd', 'build_and_run_cmd'"
    )
    with pytest.raises(TypeError, match=expected_error):
        AbstractDbtBase(project_dir="project_dir")


@pytest.mark.parametrize("cmd_flags", [["--some-flag"], []])
@patch("cosmos.operators.base.AbstractDbtBase.build_and_run_cmd")
def test_dbt_base_operator_execute(mock_build_and_run_cmd, cmd_flags, monkeypatch):
    """Tests that the base operator execute method calls the build_and_run_cmd method with the expected arguments."""
    monkeypatch.setattr(AbstractDbtBase, "add_cmd_flags", lambda _: cmd_flags)
    AbstractDbtBase.__abstractmethods__ = set()

    base_operator = AbstractDbtBase(task_id="fake_task", project_dir="fake_dir")

    base_operator.execute(context={})
    mock_build_and_run_cmd.assert_called_once_with(context={}, cmd_flags=cmd_flags)


@patch("cosmos.operators.base.context_merge")
def test_dbt_base_operator_context_merge_called(mock_context_merge):
    """Tests that the base operator execute method calls the context_merge method with the expected arguments."""
    base_operator = AbstractDbtBase(
        task_id="fake_task",
        project_dir="fake_dir",
        extra_context={"extra": "extra"},
    )

    base_operator.execute(context={})
    mock_context_merge.assert_called_once_with({}, {"extra": "extra"})


@pytest.mark.parametrize(
    "context, extra_context, expected_context",
    [
        (
            Context(
                start_date=datetime(2021, 1, 1),
            ),
            {
                "extra": "extra",
            },
            Context(
                start_date=datetime(2021, 1, 1),
                extra="extra",
            ),
        ),
        (
            Context(
                start_date=datetime(2021, 1, 1),
                end_date=datetime(2023, 1, 1),
            ),
            {
                "extra": "extra",
                "extra_2": "extra_2",
            },
            Context(
                start_date=datetime(2021, 1, 1),
                end_date=datetime(2023, 1, 1),
                extra="extra",
                extra_2="extra_2",
            ),
        ),
        (
            Context(
                overwrite="to_overwrite",
                start_date=datetime(2021, 1, 1),
                end_date=datetime(2023, 1, 1),
            ),
            {
                "overwrite": "overwritten",
            },
            Context(
                start_date=datetime(2021, 1, 1),
                end_date=datetime(2023, 1, 1),
                overwrite="overwritten",
            ),
        ),
    ],
)
def test_dbt_base_operator_context_merge(
    context,
    extra_context,
    expected_context,
):
    """Tests that the base operator execute method calls and update context"""
    base_operator = AbstractDbtBase(
        task_id="fake_task",
        project_dir="fake_dir",
        extra_context=extra_context,
    )

    base_operator.execute(context=context)
    assert context == expected_context


@pytest.mark.parametrize(
    "dbt_command, dbt_operator_class",
    [
        ("test", DbtTestMixin),
        ("snapshot", DbtSnapshotMixin),
        ("ls", DbtLSMixin),
        ("seed", DbtSeedMixin),
        ("run", DbtRunMixin),
        ("build", DbtBuildMixin),
        ("compile", DbtCompileMixin),
    ],
)
def test_dbt_mixin_base_cmd(dbt_command, dbt_operator_class):
    assert [dbt_command] == dbt_operator_class.base_cmd


@pytest.mark.parametrize("dbt_operator_class", [DbtSeedMixin, DbtRunMixin, DbtBuildMixin])
@pytest.mark.parametrize(
    "full_refresh, expected_flags", [("True", ["--full-refresh"]), (True, ["--full-refresh"]), (False, [])]
)
def test_dbt_mixin_add_cmd_flags_full_refresh(full_refresh, expected_flags, dbt_operator_class):
    dbt_mixin = dbt_operator_class(full_refresh=full_refresh)
    flags = dbt_mixin.add_cmd_flags()
    assert flags == expected_flags


@pytest.mark.parametrize("args, expected_flags", [(None, []), ({"arg1": "val1"}, ["--args", "arg1: val1\n"])])
def test_dbt_mixin_add_cmd_flags_run_operator(args, expected_flags):
    macro_name = "some_macro"
    run_operation = DbtRunOperationMixin(macro_name=macro_name, args=args)
    assert run_operation.base_cmd == ["run-operation", "some_macro"]

    flags = run_operation.add_cmd_flags()
    assert flags == expected_flags


def test_abstract_dbt_base_operator_append_env_is_false_by_default():
    """Tests that the append_env attribute is set to False by default."""
    AbstractDbtBase.__abstractmethods__ = set()
    base_operator = AbstractDbtBase(task_id="fake_task", project_dir="fake_dir")
    assert base_operator.append_env is False


def test_abstract_dbt_base_is_not_airflow_base_operator():
    AbstractDbtBase.__abstractmethods__ = set()
    base_operator = AbstractDbtBase(task_id="fake_task", project_dir="fake_dir")
    assert not isinstance(base_operator, BaseOperator)


def test_abstract_dbt_base_init_no_super():
    """Test that super().__init__ is not called in AbstractDbtBase"""
    init_method = getattr(AbstractDbtBase, "__init__", None)
    assert init_method is not None

    source = inspect.getsource(init_method)
    assert "super().__init__" not in source

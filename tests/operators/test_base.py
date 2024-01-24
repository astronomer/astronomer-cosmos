import pytest
from unittest.mock import patch

from cosmos.operators.base import (
    AbstractDbtBaseOperator,
    DbtLSMixin,
    DbtSeedMixin,
    DbtRunOperationMixin,
    DbtTestMixin,
    DbtSnapshotMixin,
    DbtRunMixin,
)


def test_dbt_base_operator_is_abstract():
    """Tests that the abstract base operator cannot be instantiated since the base_cmd is not defined."""
    expected_error = (
        "Can't instantiate abstract class AbstractDbtBaseOperator with abstract methods base_cmd, build_and_run_cmd"
    )
    with pytest.raises(TypeError, match=expected_error):
        AbstractDbtBaseOperator()


@pytest.mark.parametrize("cmd_flags", [["--some-flag"], []])
@patch("cosmos.operators.base.AbstractDbtBaseOperator.build_and_run_cmd")
def test_dbt_base_operator_execute(mock_build_and_run_cmd, cmd_flags, monkeypatch):
    """Tests that the base operator execute method calls the build_and_run_cmd method with the expected arguments."""
    monkeypatch.setattr(AbstractDbtBaseOperator, "add_cmd_flags", lambda _: cmd_flags)
    AbstractDbtBaseOperator.__abstractmethods__ = set()

    base_operator = AbstractDbtBaseOperator(task_id="fake_task", project_dir="fake_dir")

    base_operator.execute(context={})
    mock_build_and_run_cmd.assert_called_once_with(context={}, cmd_flags=cmd_flags)


@pytest.mark.parametrize(
    "dbt_command, dbt_operator_class",
    [
        ("test", DbtTestMixin),
        ("snapshot", DbtSnapshotMixin),
        ("ls", DbtLSMixin),
        ("seed", DbtSeedMixin),
        ("run", DbtRunMixin),
    ],
)
def test_dbt_mixin_base_cmd(dbt_command, dbt_operator_class):
    assert [dbt_command] == dbt_operator_class.base_cmd


@pytest.mark.parametrize("dbt_operator_class", [DbtSeedMixin, DbtRunMixin])
@pytest.mark.parametrize("full_refresh, expected_flags", [(True, ["--full-refresh"]), (False, [])])
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

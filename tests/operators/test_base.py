import inspect
import sys
from datetime import datetime
from unittest.mock import patch

import pytest
import yaml
from airflow.models import BaseOperator

try:
    from airflow.sdk.definitions.context import Context
except ImportError:
    from airflow.utils.context import Context

from cosmos.config import ProfileConfig
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtCompileMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSemanticMetricViewMixin,
    DbtSnapshotMixin,
    DbtTestMixin,
)


def test_dbt_base_is_abstract():
    if sys.version_info >= (3, 12):
        expected_error = (
            "Can't instantiate abstract class AbstractDbtBase without an implementation for abstract methods "
            "'base_cmd', 'build_and_run_cmd'"
        )
    else:
        expected_error = (
            "Can't instantiate abstract class AbstractDbtBase with abstract methods base_cmd, build_and_run_cmd"
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
        ("run", DbtSemanticMetricViewMixin),
        ("build", DbtBuildMixin),
        ("compile", DbtCompileMixin),
    ],
)
def test_dbt_mixin_base_cmd(dbt_command, dbt_operator_class):
    assert [dbt_command] == dbt_operator_class.base_cmd


def test_dbt_semantic_mixin_ui_color_differs_from_run_mixin():
    """DbtSemanticMetricViewMixin reuses DbtRunMixin's command/flags but must render distinctly in the UI."""
    assert DbtSemanticMetricViewMixin.ui_color != DbtRunMixin.ui_color


@pytest.mark.parametrize("dbt_operator_class", [DbtSeedMixin, DbtRunMixin, DbtSemanticMetricViewMixin, DbtBuildMixin])
@pytest.mark.parametrize(
    "full_refresh, expected_flags", [("True", ["--full-refresh"]), (True, ["--full-refresh"]), (False, [])]
)
def test_dbt_mixin_add_cmd_flags_full_refresh(full_refresh, expected_flags, dbt_operator_class):
    dbt_mixin = dbt_operator_class(full_refresh=full_refresh)
    flags = dbt_mixin.add_cmd_flags()
    assert flags == expected_flags


@pytest.mark.parametrize("args, expected_flags", [(None, []), ({"arg1": "val1"}, ["--args", "{arg1: val1}"])])
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


@patch("cosmos.operators.base.settings")
@patch("cosmos.operators.base.AbstractDbtBase.build_and_run_cmd")
def test_dbt_base_operator_execute_debug_mode_exception_stops_memory_tracking(
    mock_build_and_run_cmd, mock_settings, monkeypatch
):
    """Tests that stop_memory_tracking is called when an exception occurs during debug mode execution."""
    mock_settings.enable_debug_mode = True
    mock_build_and_run_cmd.side_effect = RuntimeError("Test exception")

    monkeypatch.setattr(AbstractDbtBase, "add_cmd_flags", lambda _: [])
    AbstractDbtBase.__abstractmethods__ = set()

    base_operator = AbstractDbtBase(task_id="fake_task", project_dir="fake_dir")

    with patch("cosmos.debug.start_memory_tracking") as mock_start_tracking:
        with patch("cosmos.debug.stop_memory_tracking") as mock_stop_tracking:
            with pytest.raises(RuntimeError, match="Test exception"):
                base_operator.execute(context={})

            # Verify memory tracking was started
            mock_start_tracking.assert_called_once_with({})
            # Verify memory tracking was stopped even though an exception occurred
            mock_stop_tracking.assert_called_once_with({})


@patch("cosmos.operators.base.settings")
@patch("cosmos.operators.base.AbstractDbtBase.build_and_run_cmd")
def test_dbt_base_operator_execute_debug_mode_success_stops_memory_tracking(
    mock_build_and_run_cmd, mock_settings, monkeypatch
):
    """Tests that stop_memory_tracking is called when execution succeeds in debug mode."""
    mock_settings.enable_debug_mode = True

    monkeypatch.setattr(AbstractDbtBase, "add_cmd_flags", lambda _: [])
    AbstractDbtBase.__abstractmethods__ = set()

    base_operator = AbstractDbtBase(task_id="fake_task", project_dir="fake_dir")

    with patch("cosmos.debug.start_memory_tracking") as mock_start_tracking:
        with patch("cosmos.debug.stop_memory_tracking") as mock_stop_tracking:
            base_operator.execute(context={})

            # Verify memory tracking was started
            mock_start_tracking.assert_called_once_with({})
            # Verify memory tracking was stopped after successful execution
            mock_stop_tracking.assert_called_once_with({})


def test_process_global_flag_dumps_dicts_as_one_line_flow_yaml_that_round_trips():
    """dbt 2 rejects block YAML with quoted strings, and dbt 1.x parses the flag with PyYAML."""
    value = {"s": "2", "f": 1e-06, "e": "😀", "m": {1: "one", "default": "other"}, "long": "x" * 200}
    flag_name, flag_value = AbstractDbtBase._process_global_flag("--vars", value)
    assert flag_name == "--vars"
    assert "\n" not in flag_value
    assert yaml.safe_load(flag_value) == value


class _ConcreteDbtBase(AbstractDbtBase):
    base_cmd = ["run"]

    def build_and_run_cmd(self, *args, **kwargs):
        pass


def _operator_with_profile(profile_config):
    operator = _ConcreteDbtBase(project_dir="fake_dir", dbt_executable_path="dbt")
    operator.profile_config = profile_config
    return operator


def test_abstract_dbt_base_profile_config_defaults_to_none():
    assert _ConcreteDbtBase(project_dir="fake_dir").profile_config is None


@pytest.mark.parametrize(
    "profile_name, target_name, expected_flags",
    [
        ("my_profile", "my_target", ["--profile", "my_profile", "--target", "my_target"]),
        ("my_profile", "", ["--profile", "my_profile"]),
        ("", "my_target", ["--target", "my_target"]),
        ("", "", []),
    ],
)
def test_add_profile_flags(profile_name, target_name, expected_flags):
    profile_config = ProfileConfig(
        profile_name=profile_name, target_name=target_name, profiles_yml_filepath="/path/profiles.yml"
    )

    assert _operator_with_profile(profile_config).add_profile_flags() == expected_flags


def test_add_profile_flags_without_profile_config():
    assert _ConcreteDbtBase(project_dir="fake_dir").add_profile_flags() == []


def test_build_cmd_does_not_add_profile_flags():
    """Each execution mode decides where the profile flags go, so ``build_cmd`` leaves them out."""
    profile_config = ProfileConfig(profile_name="p", target_name="t", profiles_yml_filepath="/path/profiles.yml")

    cmd, _ = _operator_with_profile(profile_config).build_cmd(
        context=Context(execution_date=datetime(2023, 2, 15, 12, 30))
    )

    assert cmd == ["dbt", "run"]

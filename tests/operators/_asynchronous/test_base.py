from __future__ import annotations

from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from cosmos.config import ProfileConfig
from cosmos.hooks.subprocess import FullOutputSubprocessResult
from cosmos.operators._asynchronous import SetupAsyncOperator, TeardownAsyncOperator
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator, _create_async_operator_class
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators._asynchronous.databricks import DbtRunAirflowAsyncDatabricksOperator
from cosmos.operators.local import DbtRunLocalOperator


@pytest.mark.parametrize(
    "profile_type, dbt_class, expected_operator_class",
    [
        ("bigquery", "DbtRun", DbtRunAirflowAsyncBigqueryOperator),
        ("databricks", "DbtRun", DbtRunAirflowAsyncDatabricksOperator),
    ],
)
def test_create_async_operator_class(profile_type, dbt_class, expected_operator_class):
    """Test the successful loading of the async operator class."""

    operator_class = _create_async_operator_class(profile_type, dbt_class)

    assert operator_class == expected_operator_class


def test_create_async_operator_class_unsupported():

    with pytest.raises(ImportError, match="Error in loading class"):
        _create_async_operator_class("test_profile", "DbtRun")


@pytest.fixture
def profile_config_mock():
    """Fixture to create a mock ProfileConfig."""
    mock_config = MagicMock(spec=ProfileConfig)
    mock_config.get_profile_type.return_value = "bigquery"
    return mock_config


def test_create_async_operator_class_valid():
    """Test _create_async_operator_class returns the correct async operator class if available."""
    with patch("cosmos.operators._asynchronous.base.importlib.import_module") as mock_import:
        mock_class = MagicMock()
        mock_import.return_value = MagicMock()
        setattr(mock_import.return_value, "DbtRunAirflowAsyncBigqueryOperator", mock_class)

        result = _create_async_operator_class("bigquery", "DbtRun")
        assert result == mock_class


class MockAsyncOperator(DbtRunLocalOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@patch("cosmos.operators._asynchronous.base._create_async_operator_class", return_value=MockAsyncOperator)
def test_dbt_run_airflow_async_factory_operator_init(mock_create_class, profile_config_mock):

    operator = DbtRunAirflowAsyncFactoryOperator(
        task_id="test_task",
        project_dir="some/path",
        profile_config=profile_config_mock,
    )

    assert operator is not None
    assert isinstance(operator, MockAsyncOperator)


@patch("cosmos.operators.local.DbtRunLocalOperator.build_and_run_cmd")
def test_teardown_execute(mock_build_and_run_cmd):
    operator = TeardownAsyncOperator(
        task_id="fake-task",
        profile_config=Mock(),
        project_dir="fake-dir",
    )
    operator.execute({})
    mock_build_and_run_cmd.assert_called_once()


@pytest.fixture
def mock_operator_params():
    return {
        "task_id": "test_task",
        "project_dir": "/tmp",
        "profile_config": MagicMock(get_profile_type=MagicMock(return_value="bigquery")),
    }


@pytest.fixture
def mock_load_method():
    """Mock load_method_from_module to return a fake function."""
    mock_function = MagicMock()
    mock_function.__name__ = "_mock_bigquery_adapter"
    mock_function.__module__ = "cosmos.operators._asynchronous.bigquery"
    with patch("cosmos._utils.importer.load_method_from_module", return_value=mock_function):
        yield mock_function


@pytest.fixture
def mock_file_operations():
    """Mock file reading/writing operations."""
    with patch("builtins.open", mock_open(read_data="#!/usr/bin/env python\n")) as mock_file:
        yield mock_file


@pytest.fixture
def mock_super_run_subprocess():
    with patch(
        "cosmos.operators.virtualenv.DbtRunVirtualenvOperator.run_subprocess",
        return_value=FullOutputSubprocessResult(0, "", ""),
    ) as mock_run:
        yield mock_run


def test_setup_run_subprocess(mock_operator_params, mock_load_method, mock_file_operations, mock_super_run_subprocess):
    op = SetupAsyncOperator(**mock_operator_params)
    op._py_bin = "/fake/venv/bin/python"
    command = ["dbt", "run"]
    env = {}
    cwd = "/tmp"

    op.run_subprocess(command, env, cwd)

    mock_file_operations.assert_called_with("/fake/venv/bin/dbt", "w")
    mock_super_run_subprocess.assert_called_once_with(command, env, cwd)


def test_teardown_run_subprocess(
    mock_operator_params, mock_load_method, mock_file_operations, mock_super_run_subprocess
):
    op = TeardownAsyncOperator(**mock_operator_params)
    op._py_bin = "/fake/venv/bin/python"

    command = ["dbt", "clean"]
    env = {}
    cwd = "/tmp"

    op.run_subprocess(command, env, cwd)

    mock_file_operations.assert_called_with("/fake/venv/bin/dbt", "w")
    mock_super_run_subprocess.assert_called_once_with(command, env, cwd)


def test_setup_execute(mock_operator_params):
    op = SetupAsyncOperator(**mock_operator_params)

    with patch.object(op, "build_and_run_cmd") as mock_build_and_run:
        op.execute(context={})

        mock_build_and_run.assert_called_once_with(
            context={}, cmd_flags=op.dbt_cmd_flags, run_as_async=True, async_context={"profile_type": "bigquery"}
        )


def test_setup_run_subprocess_py_bin_unset(
    mock_operator_params, mock_load_method, mock_file_operations, mock_super_run_subprocess
):
    op = SetupAsyncOperator(**mock_operator_params)
    command = ["dbt", "run"]
    env = {}
    cwd = "/tmp"

    with pytest.raises(AttributeError, match="_py_bin attribute not set for VirtualEnv operator"):
        op.run_subprocess(command, env, cwd)


def test_teardown_run_subprocess_py_bin_unset(
    mock_operator_params, mock_load_method, mock_file_operations, mock_super_run_subprocess
):
    op = TeardownAsyncOperator(**mock_operator_params)
    command = ["dbt", "run"]
    env = {}
    cwd = "/tmp"

    with pytest.raises(AttributeError, match="_py_bin attribute not set for VirtualEnv operator"):
        op.run_subprocess(command, env, cwd)

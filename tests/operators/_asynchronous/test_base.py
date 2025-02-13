from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest

from cosmos.config import ProfileConfig
from cosmos.operators._asynchronous import TeardownAsyncOperator
from cosmos.operators._asynchronous.base import DbtRunAirflowAsyncFactoryOperator, _create_async_operator_class
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators._asynchronous.databricks import DbtRunAirflowAsyncDatabricksOperator
from cosmos.operators.local import DbtRunLocalOperator

_ASYNC_PROFILE = ["bigquery", "databricks"]


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

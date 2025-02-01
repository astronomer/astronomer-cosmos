import pytest

from cosmos.operators._asynchronous.base import _create_async_operator_class
from cosmos.operators._asynchronous.bigquery import DbtRunAirflowAsyncBigqueryOperator
from cosmos.operators.local import DbtRunLocalOperator


@pytest.mark.parametrize(
    "profile_type, dbt_class, expected_operator_class",
    [
        ("bigquery", "DbtRun", DbtRunAirflowAsyncBigqueryOperator),
        ("snowflake", "DbtRun", DbtRunLocalOperator),
        ("bigquery", "DbtTest", DbtRunLocalOperator),
    ],
)
def test_create_async_operator_class_success(profile_type, dbt_class, expected_operator_class):
    """Test the successful loading of the async operator class."""

    operator_class = _create_async_operator_class(profile_type, dbt_class)

    assert operator_class == expected_operator_class

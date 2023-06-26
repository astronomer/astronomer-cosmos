"""
Tests Dataset export override
"""

import airflow
import pytest

from cosmos.dataset import Dataset, get_dbt_dataset


def test_import() -> None:
    "Tests that we can import the Dataset object in all versions of Airflow."

    if airflow.__version__ >= "2.4.0":
        assert Dataset.__module__ == "airflow.datasets"
    else:
        assert Dataset.__module__ == "cosmos.dataset"


@pytest.mark.parametrize(
    ["connection_id", "project_name", "model_name", "expected_dataset"],
    [
        (
            "my_connection",
            "jaffle_shop",
            "orders",
            Dataset("DBT://MY_CONNECTION/JAFFLE_SHOP/ORDERS"),
        ),
        pytest.param("my_connection", "jafflé_shop", "orders", None, marks=pytest.mark.xfail),
    ],
)
def test_get_dbt_dataset(connection_id: str, project_name: str, model_name: str, expected_dataset: Dataset) -> None:
    assert get_dbt_dataset(connection_id, project_name, model_name) == expected_dataset

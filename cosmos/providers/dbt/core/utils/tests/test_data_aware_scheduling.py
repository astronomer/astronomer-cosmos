import pytest

from cosmos.providers.dbt.core.utils.data_aware_scheduling import get_dbt_dataset
from cosmos.providers.dbt.dataset import Dataset


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

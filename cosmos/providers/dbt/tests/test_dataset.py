"""
Tests Dataset export override
"""
import importlib
import sys
from unittest import mock

import pytest

@pytest.fixture(autouse=False)
def reset_dataset_import() -> None:
    """
    Remove all dataset imports from sys.modules.
    """
    for key in list(sys.modules.keys()):
        if "dataset" in key.lower():
            del sys.modules[key]


def test_failed_dataset_import(reset_dataset_import: None) -> None:
    """
    Test that you can still import the Dataset class even if Airflow <= 2.5 is installed.
    """
    with mock.patch.dict(
        sys.modules, {"airflow.datasets": None}
    ):
        from cosmos.providers.dbt.dataset import Dataset
        print(Dataset)

        assert Dataset.cosmos_override is True
        assert Dataset.__module__ == "cosmos.providers.dbt.dataset"


def test_successful_dataset_import() -> None:
    """
    Test that you can still import the Dataset class even if Airflow <= 2.5 is installed.
    """
    from cosmos.providers.dbt.dataset import Dataset

    # this should be the airflow.datasets.Dataset class
    assert Dataset.__module__ == "airflow.datasets"

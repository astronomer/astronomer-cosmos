"""
Tests Dataset export override
"""

import airflow

from cosmos.providers.dbt.dataset import Dataset


def test_import() -> None:
    "Tests that we can import the Dataset object in all versions of Airflow."

    if airflow.__version__ >= "2.4.0":
        assert Dataset.__module__ == "airflow.datasets"
    else:
        assert Dataset.__module__ == "cosmos.providers.dbt.dataset"

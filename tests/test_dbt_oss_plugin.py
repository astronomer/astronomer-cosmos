import importlib
import sys

import pytest
from airflow.exceptions import AirflowNotFoundException

import cosmos.profiles
import cosmos.profiles.postgres
from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping

PLUGIN = "tests.dbt_oss_plugin"


@pytest.fixture
def dbt_oss_plugin(monkeypatch):
    """Import the plugin without leaving its Postgres-to-BigQuery swap behind for the tests that run after this one."""
    for module in (cosmos.profiles, cosmos.profiles.postgres):
        # Importing the plugin replaces this attribute, and monkeypatch puts the original back.
        monkeypatch.setattr(module, "PostgresUserPasswordProfileMapping", module.PostgresUserPasswordProfileMapping)
    monkeypatch.setattr(cosmos.profiles, "get_automatic_profile_mapping", cosmos.profiles.get_automatic_profile_mapping)
    already_loaded = sys.modules.pop(PLUGIN, None)  # the dbt-oss job loads it through PYTEST_PLUGINS
    yield importlib.import_module(PLUGIN)
    sys.modules.pop(PLUGIN, None)
    if already_loaded:
        sys.modules[PLUGIN] = already_loaded


def test_bigquery_swap_keeps_schema_in_profile_args_but_not_in_the_profile(monkeypatch, dbt_oss_plugin):
    monkeypatch.setenv("COSMOS_TEST_BIGQUERY_DATASET", "job_dataset")
    # The BigQuery mapping puts `profile_args` into the profile, so a `schema` that leaks shows up in it.
    monkeypatch.setattr(
        GoogleCloudServiceAccountDictProfileMapping,
        "profile",
        property(lambda self: {"type": "bigquery", **self.profile_args}),
    )

    mapping = cosmos.profiles.PostgresUserPasswordProfileMapping(
        conn_id="postgres_conn", profile_args={"schema": "public"}
    )

    assert mapping.profile_args["schema"] == "public"
    assert mapping.profile["dataset"] == "job_dataset"
    assert "schema" not in mapping.profile


def test_example_conn_gets_the_bigquery_mapping_and_other_connections_the_real_lookup(monkeypatch, dbt_oss_plugin):
    monkeypatch.setenv("COSMOS_TEST_BIGQUERY_DATASET", "job_dataset")

    mapping = cosmos.profiles.get_automatic_profile_mapping("example_conn", {})

    assert isinstance(mapping, GoogleCloudServiceAccountDictProfileMapping)
    with pytest.raises(AirflowNotFoundException, match="not_a_registered_conn"):
        cosmos.profiles.get_automatic_profile_mapping("not_a_registered_conn", {})

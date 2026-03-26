import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos.dataset import (
    compute_model_outlet_uris,
    construct_dataset_uri,
    get_dataset_alias_name,
    get_dataset_namespace,
)

START_DATE = datetime(2024, 4, 16)
example_dag = DAG("dag", start_date=START_DATE)


@pytest.mark.parametrize(
    "dag, task_group,task_id,result_identifier",
    [
        (example_dag, None, "task_id", "dag__task_id"),
        (None, TaskGroup(dag=example_dag, group_id="inner_tg"), "task_id", "dag__inner_tg__task_id"),
        (
            None,
            TaskGroup(
                dag=example_dag, group_id="child_tg", parent_group=TaskGroup(dag=example_dag, group_id="parent_tg")
            ),
            "task_id",
            "dag__child_tg__task_id",
        ),
        (
            None,
            TaskGroup(
                dag=example_dag,
                group_id="child_tg",
                parent_group=TaskGroup(
                    dag=example_dag, group_id="mum_tg", parent_group=TaskGroup(dag=example_dag, group_id="nana_tg")
                ),
            ),
            "task_id",
            "dag__child_tg__task_id",
        ),
        (
            None,
            TaskGroup(
                dag=example_dag,
                group_id="another_tg",
            ),
            "another_tg.task_id",  # Airflow injects this during task execution time when outside of standalone
            "dag__another_tg__task_id",
        ),
    ],
)
def test_get_dataset_alias_name(dag, task_group, task_id, result_identifier):
    assert get_dataset_alias_name(dag, task_group, task_id) == result_identifier


# --- Tests for get_dataset_namespace ---


class TestGetDatasetNamespace:
    def test_postgres_with_profile_mapping(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "postgres"
        profile_config.profile_mapping.profile = {"host": "myhost", "port": 5432}
        assert get_dataset_namespace(profile_config) == "postgres://myhost:5432"

    def test_bigquery_with_profile_mapping(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "bigquery"
        profile_config.profile_mapping.profile = {"project": "my-project"}
        assert get_dataset_namespace(profile_config) == "bigquery"

    def test_snowflake_with_profile_mapping(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "snowflake"
        profile_config.profile_mapping.profile = {"account": "xy12345.us-east-1"}
        assert get_dataset_namespace(profile_config) == "snowflake://xy12345.us-east-1"

    def test_unknown_adapter_fallback(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "custom_adapter"
        profile_config.profile_mapping.profile = {}
        assert get_dataset_namespace(profile_config) == "custom_adapter://"

    def test_profiles_yml_filepath(self, tmp_path):
        profiles_yml = tmp_path / "profiles.yml"
        profiles_yml.write_text("""
my_profile:
  outputs:
    dev:
      type: postgres
      host: dbhost
      port: 5433
      user: user
      pass: pass
      dbname: mydb
      schema: public
""")
        profile_config = MagicMock()
        profile_config.profile_mapping = None
        profile_config.profiles_yml_filepath = str(profiles_yml)
        profile_config.profile_name = "my_profile"
        profile_config.target_name = "dev"
        assert get_dataset_namespace(profile_config) == "postgres://dbhost:5433"

    def test_returns_none_on_error(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = None
        profile_config.profiles_yml_filepath = "/nonexistent/path.yml"
        assert get_dataset_namespace(profile_config) is None


# --- Tests for construct_dataset_uri ---


class TestConstructDatasetUri:
    @patch("cosmos.dataset.AIRFLOW_VERSION", new=MagicMock(__lt__=lambda self, other: True, major=2))
    @patch("cosmos.dataset.settings")
    def test_airflow2_default_uri(self, mock_settings):
        mock_settings.use_dataset_airflow3_uri_standard = False
        uri = construct_dataset_uri("postgres://host:5432", "mydb", "public", "customers")
        assert uri == "postgres://host:5432/mydb.public.customers"

    @patch("cosmos.dataset.AIRFLOW_VERSION", new=MagicMock(__lt__=lambda self, other: False, major=3))
    def test_airflow3_uri(self):
        uri = construct_dataset_uri("postgres://host:5432", "mydb", "public", "customers")
        assert uri == "postgres://host:5432/mydb/public/customers"


# --- Tests for compute_model_outlet_uris ---


class TestComputeModelOutletUris:
    def test_reads_manifest_and_computes_uris(self, tmp_path):
        manifest = {
            "nodes": {
                "model.jaffle_shop.customers": {
                    "resource_type": "model",
                    "database": "postgres",
                    "schema": "public",
                    "alias": "customers",
                },
                "seed.jaffle_shop.raw_orders": {
                    "resource_type": "seed",
                    "database": "postgres",
                    "schema": "public",
                    "alias": "raw_orders",
                },
                "test.jaffle_shop.not_null": {
                    "resource_type": "test",
                    "database": "postgres",
                    "schema": "public",
                    "alias": "not_null",
                },
            }
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        result = compute_model_outlet_uris(str(manifest_path), "postgres://host:5432")
        # Models and seeds should have URIs, tests should not
        assert "model.jaffle_shop.customers" in result
        assert "seed.jaffle_shop.raw_orders" in result
        assert "test.jaffle_shop.not_null" not in result
        assert len(result) == 2

    def test_missing_manifest_returns_empty(self):
        result = compute_model_outlet_uris("/nonexistent/manifest.json", "postgres://host:5432")
        assert result == {}

    def test_skips_nodes_with_missing_fields(self, tmp_path):
        manifest = {
            "nodes": {
                "model.pkg.incomplete": {
                    "resource_type": "model",
                    "database": "",
                    "schema": "public",
                    "alias": "incomplete",
                },
            }
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        result = compute_model_outlet_uris(str(manifest_path), "postgres://host:5432")
        assert result == {}

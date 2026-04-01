import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from packaging.version import Version

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
        # fix_account_name normalizes account locator format (appends cloud suffix)
        assert get_dataset_namespace(profile_config) == "snowflake://xy12345.us-east-1.aws"

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

    def test_returns_none_when_no_profile_info(self):
        """When neither profile_mapping nor profiles_yml_filepath is set, returns None."""
        profile_config = MagicMock()
        profile_config.profile_mapping = None
        profile_config.profiles_yml_filepath = None
        assert get_dataset_namespace(profile_config) is None

    def test_spark_with_thrift_method(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "spark"
        profile_config.profile_mapping.profile = {"host": "spark-host", "method": "thrift"}
        assert get_dataset_namespace(profile_config) == "spark://spark-host:10001"

    def test_spark_with_http_method(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "spark"
        profile_config.profile_mapping.profile = {"host": "spark-host", "method": "http"}
        assert get_dataset_namespace(profile_config) == "spark://spark-host:443"

    def test_spark_with_explicit_port(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "spark"
        profile_config.profile_mapping.profile = {"host": "spark-host", "port": 9999}
        assert get_dataset_namespace(profile_config) == "spark://spark-host:9999"

    def test_spark_with_unknown_method(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "spark"
        profile_config.profile_mapping.profile = {"host": "spark-host", "method": "session"}
        assert get_dataset_namespace(profile_config) == "spark://spark-host:10000"

    def test_glue_with_account_id(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "glue"
        profile_config.profile_mapping.profile = {"region": "eu-west-1", "account_id": "123456789"}
        assert get_dataset_namespace(profile_config) == "arn:aws:glue:eu-west-1:123456789"

    def test_glue_with_role_arn(self):
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "glue"
        profile_config.profile_mapping.profile = {
            "region": "us-east-1",
            "role_arn": "arn:aws:iam::987654321:role/my-role",
        }
        assert get_dataset_namespace(profile_config) == "arn:aws:glue:us-east-1:987654321"

    @patch("cosmos.dataset._resolve_snowflake_account")
    def test_snowflake_calls_resolve(self, mock_resolve):
        mock_resolve.return_value = "normalized-account"
        profile_config = MagicMock()
        profile_config.profile_mapping = MagicMock()
        profile_config.profile_mapping.dbt_profile_type = "snowflake"
        profile_config.profile_mapping.profile = {"account": "raw-account"}
        result = get_dataset_namespace(profile_config)
        assert result == "snowflake://normalized-account"
        mock_resolve.assert_called_once_with("raw-account")


class TestResolveSnowflakeAccount:
    def test_with_openlineage_available(self):
        from cosmos.dataset import _resolve_snowflake_account

        # OL is installed in test env, so fix_account_name should be used
        result = _resolve_snowflake_account("xy12345.us-east-1")
        assert result == "xy12345.us-east-1.aws"

    @patch.dict("sys.modules", {"openlineage.common.provider.snowflake": None})
    def test_without_openlineage(self):
        """Falls back to returning the raw account when OL is not installed."""
        # Need to reimport to hit the ImportError branch
        import importlib

        import cosmos.dataset

        importlib.reload(cosmos.dataset)
        result = cosmos.dataset._resolve_snowflake_account("xy12345")
        assert result == "xy12345"
        # Reload again to restore normal state
        importlib.reload(cosmos.dataset)


# --- Tests for construct_dataset_uri ---


class TestConstructDatasetUri:
    @patch("cosmos.dataset.AIRFLOW_VERSION", new=Version("2.10.0"))
    @patch("cosmos.dataset.settings")
    def test_airflow2_default_uri(self, mock_settings):
        mock_settings.use_dataset_airflow3_uri_standard = False
        uri = construct_dataset_uri("postgres://host:5432", "mydb.public.customers")
        assert uri == "postgres://host:5432/mydb.public.customers"

    @patch("cosmos.dataset.AIRFLOW_VERSION", new=Version("2.10.0"))
    @patch("cosmos.dataset.settings")
    def test_airflow2_with_airflow3_standard(self, mock_settings):
        mock_settings.use_dataset_airflow3_uri_standard = True
        uri = construct_dataset_uri("postgres://host:5432", "mydb.public.customers")
        assert uri == "postgres://host:5432/mydb/public/customers"

    @patch("cosmos.dataset.AIRFLOW_VERSION", new=Version("3.0.0"))
    def test_airflow3_uri(self):
        uri = construct_dataset_uri("postgres://host:5432", "mydb.public.customers")
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

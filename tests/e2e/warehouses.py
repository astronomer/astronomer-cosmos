"""
Warehouse harnesses for the metric view (Databricks) and semantic view (Snowflake) end-to-end tests.

Credentials are resolved in this order and never printed:

- Databricks: ``AIRFLOW_CONN_DATABRICKS_DEFAULT``; otherwise ``DATABRICKS_HOST``, ``DATABRICKS_TOKEN`` and
  ``DATABRICKS_WAREHOUSE_ID``. ``DATABRICKS_CATALOG`` selects the Unity Catalog catalog (default ``main``).
- Snowflake: ``AIRFLOW_CONN_SNOWFLAKE_DEFAULT``; otherwise ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``,
  ``SNOWFLAKE_PASSWORD`` (or ``SNOWFLAKE_PRIVATE_KEY_FILE``), ``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_WAREHOUSE`` and
  optionally ``SNOWFLAKE_ROLE``; otherwise the Snowflake CLI ``connections.toml`` (``SNOWFLAKE_CONNECTIONS_FILE``
  overrides the path, ``SNOWFLAKE_CONNECTION_NAME`` picks the entry, default ``default`` or the only entry).
  ``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_WAREHOUSE``, ``SNOWFLAKE_ROLE`` and ``SNOWFLAKE_SCHEMA`` override the file.
  A missing database or warehouse is discovered from the user's Snowflake defaults, then from the first
  warehouse and the first non-system database the user can see.

Each harness creates a unique schema and drops it when the session ends. When the role cannot create
schemas, the harness can fall back to the connection's schema (``DATABRICKS_SCHEMA`` / ``SNOWFLAKE_SCHEMA``)
and drop only the objects it created. Because the project uses fixed names (``raw_orders``, ``orders``,
``revenue_by_status`` and the semantic object) with ``full_refresh``, any same-named objects in that schema
are overwritten, so the fallback requires ``COSMOS_E2E_ALLOW_SHARED_SCHEMA=1``.
"""

from __future__ import annotations

import json
import os
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import tomllib
from airflow.models.connection import Connection

from cosmos import ProfileConfig
from cosmos.profiles import DatabricksTokenProfileMapping, get_automatic_profile_mapping

DEV_DAGS_DIR = Path(__file__).parent.parent.parent / "dev" / "dags"
DBT_PROJECTS_DIR = DEV_DAGS_DIR / "dbt"
SCHEMA_ENV_VAR = "COSMOS_DEMO_SCHEMA"
# Identifiers introduced by PR #2885; kept in one place because the naming is under review.
RECLASSIFIED_RESOURCE_TYPE = "semantic_metric_view"
RECLASSIFIED_TASK_SUFFIX = "semantic_metric_run"
RECLASSIFIED_SETTING = "enable_semantic_metric_view_reclassification"
SEEDED_TOTAL_REVENUE = 971.50
SEEDED_TOTAL_ORDERS = 6
SEEDED_REVENUE_BY_STATUS = {"cancelled": 75.25, "completed": 851.25, "pending": 45.00}
SNOWFLAKE_CONNECTIONS_FILE = Path(os.getenv("SNOWFLAKE_CONNECTIONS_FILE", "~/.snowflake/connections.toml")).expanduser()
SNOWFLAKE_INTERACTIVE_AUTHENTICATORS = {"externalbrowser", "oauth", "username_password_mfa"}


def _strip_scheme(host: str) -> str:
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _airflow_connection(conn_id: str) -> Connection:
    return Connection.get_connection_from_secrets(conn_id)


def _as_amount(value: Any) -> float:
    return float(round(Decimal(str(value)), 2))


def _rows_to_amount_by_status(rows: list[tuple[Any, Any]]) -> dict[str, float]:
    return {str(status).lower(): _as_amount(amount) for status, amount in rows}


class WarehouseHarness:
    name: str
    project_name: str
    conn_id: str
    profile_name: str
    semantic_object_model: str
    materialization: str
    status_column: str
    semantic_watcher_operator: str = "DbtSemanticMetricViewWatcherOperator"

    created_objects: tuple[tuple[str, str], ...] = (
        ("TABLE", "revenue_by_status"),
        ("TABLE", "orders"),
        ("TABLE", "raw_orders"),
    )

    def __init__(self) -> None:
        self.schema = f"cosmos_sl_e2e_{uuid.uuid4().hex[:8]}"
        self.owns_schema = True
        self.connection_params: dict[str, Any] = {}

    def ensure_schema(self, fallback_schema: str | None) -> None:
        with self.connect() as connection, connection.cursor() as cursor:
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_qualified()}")
                return
            except Exception as error:
                if not fallback_schema or os.getenv("COSMOS_E2E_ALLOW_SHARED_SCHEMA") != "1":
                    pytest.skip(
                        f"{self.name}: cannot create schema {self.schema} ({error}). Grant CREATE SCHEMA, or set "
                        "COSMOS_E2E_ALLOW_SHARED_SCHEMA=1 to reuse the connection schema (overwrites raw_orders, "
                        "orders, revenue_by_status and the semantic object there)"
                    )
                print(f"{self.name}: cannot create schema {self.schema}, using {fallback_schema} instead ({error})")
        self.schema = fallback_schema
        self.owns_schema = False
        os.environ[SCHEMA_ENV_VAR] = self.schema

    def schema_qualified(self) -> str:
        raise NotImplementedError

    def drop_semantic_object_sql(self) -> str:
        raise NotImplementedError

    def drop_schema(self, cursor: Any) -> None:
        if self.owns_schema:
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.schema_qualified()} CASCADE")
            return
        statements = [self.drop_semantic_object_sql()] + [
            f"DROP {object_type} IF EXISTS {self.qualified(name)}" for object_type, name in self.created_objects
        ]
        errors = []
        for statement in statements:
            try:
                cursor.execute(statement)
            except Exception as error:
                errors.append(f"{statement}: {error}")
        if errors:
            raise RuntimeError("; ".join(errors))

    @property
    def project_dir(self) -> Path:
        return DBT_PROJECTS_DIR / self.project_name

    @property
    def conn_env_var(self) -> str:
        return f"AIRFLOW_CONN_{self.conn_id.upper()}"

    @property
    def semantic_object_unique_id(self) -> str:
        return f"model.{self.project_name}.{self.semantic_object_model}"

    @property
    def downstream_unique_id(self) -> str:
        return f"model.{self.project_name}.revenue_by_status"

    @property
    def orders_unique_id(self) -> str:
        return f"model.{self.project_name}.orders"

    def dag_id(self, variant: str) -> str:
        return f"{self.project_name}_{variant}"

    def configure(self) -> None:
        raise NotImplementedError

    def profile_config(self) -> ProfileConfig:
        raise NotImplementedError

    def connect(self) -> Any:
        raise NotImplementedError

    def qualified(self, name: str) -> str:
        raise NotImplementedError

    def describe_object(self, cursor: Any, name: str) -> dict[str, Any]:
        raise NotImplementedError

    def is_semantic_object(self, description: dict[str, Any]) -> bool:
        raise NotImplementedError

    def fetch_revenue_by_status_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        raise NotImplementedError

    def fetch_downstream_revenue_by_status(self, cursor: Any) -> dict[str, float]:
        raise NotImplementedError


class DatabricksHarness(WarehouseHarness):
    name = "databricks"
    project_name = "databricks_metric_view"
    conn_id = "databricks_default"
    profile_name = "databricks_metric_view"
    semantic_object_model = "order_metrics"
    materialization = "metric_view"
    status_column = "status"

    def __init__(self) -> None:
        super().__init__()
        self.catalog = os.getenv("DATABRICKS_CATALOG", "main")

    def configure(self) -> None:
        if not os.getenv(self.conn_env_var):
            host = os.getenv("DATABRICKS_HOST")
            token = os.getenv("DATABRICKS_TOKEN")
            warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
            if not (host and token and warehouse_id):
                pytest.skip(
                    "Databricks credentials not found: set AIRFLOW_CONN_DATABRICKS_DEFAULT, or DATABRICKS_HOST, "
                    "DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID"
                )
            os.environ[self.conn_env_var] = json.dumps(
                {
                    "conn_type": "databricks",
                    "host": _strip_scheme(host),
                    "password": token,
                    "extra": {"http_path": f"/sql/1.0/warehouses/{warehouse_id}"},
                }
            )
        os.environ[SCHEMA_ENV_VAR] = self.schema
        connection = _airflow_connection(self.conn_id)
        self.connection_params = {
            "server_hostname": _strip_scheme(connection.host),
            "http_path": connection.extra_dejson["http_path"],
            "access_token": connection.password or connection.extra_dejson.get("token"),
        }
        self.ensure_schema(os.getenv("DATABRICKS_SCHEMA") or connection.schema)

    def profile_config(self) -> ProfileConfig:
        return ProfileConfig(
            profile_name=self.profile_name,
            target_name="dev",
            profile_mapping=DatabricksTokenProfileMapping(
                conn_id=self.conn_id,
                profile_args={"catalog": self.catalog, "schema": self.schema},
            ),
        )

    def connect(self) -> Any:
        from databricks import sql

        return sql.connect(**self.connection_params)

    def qualified(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def schema_qualified(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def drop_semantic_object_sql(self) -> str:
        return f"DROP VIEW IF EXISTS {self.qualified(self.semantic_object_model)}"

    def describe_object(self, cursor: Any, name: str) -> dict[str, Any]:
        try:
            cursor.execute(
                f"SELECT table_type FROM {self.catalog}.information_schema.tables "
                f"WHERE table_schema = '{self.schema}' AND table_name = '{name}'"
            )
            rows = cursor.fetchall()
        except Exception:
            rows = []
        cursor.execute(f"DESCRIBE TABLE EXTENDED {self.qualified(name)}")
        details = {row[0]: row[1] for row in cursor.fetchall() if row[0]}
        return {
            "table_type": rows[0][0] if rows else None,
            "type": details.get("Type"),
            "view_text": details.get("View Text"),
        }

    def is_semantic_object(self, description: dict[str, Any]) -> bool:
        markers = " ".join(str(value or "") for value in description.values()).upper()
        return "METRIC" in markers

    def fetch_revenue_by_status_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        cursor.execute(
            f"SELECT status, MEASURE(total_revenue) FROM {self.qualified(self.semantic_object_model)} GROUP BY status"
        )
        return _rows_to_amount_by_status(cursor.fetchall())

    def fetch_downstream_revenue_by_status(self, cursor: Any) -> dict[str, float]:
        cursor.execute(f"SELECT status, total_revenue FROM {self.qualified('revenue_by_status')}")
        return _rows_to_amount_by_status(cursor.fetchall())


def _snowflake_params_from_env() -> dict[str, Any] | None:
    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        return None
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
    }


def _snowflake_params_from_connections_file() -> dict[str, Any] | None:
    if not SNOWFLAKE_CONNECTIONS_FILE.exists():
        return None
    connections = tomllib.loads(SNOWFLAKE_CONNECTIONS_FILE.read_text())
    if not connections:
        return None
    requested_name = os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if requested_name:
        name = requested_name
    elif "default" in connections:
        name = "default"
    else:
        name = next(iter(connections))
    if name not in connections:
        pytest.skip(f"Snowflake connection {name!r} not found in {SNOWFLAKE_CONNECTIONS_FILE}")
    params = dict(connections[name])
    private_key_file = params.pop("private_key_path", None) or params.pop("private_key_file", None)
    if private_key_file:
        params["private_key_file"] = str(Path(private_key_file).expanduser())
    for key in ("database", "warehouse", "role", "schema"):
        if os.getenv(f"SNOWFLAKE_{key.upper()}"):
            params[key] = os.getenv(f"SNOWFLAKE_{key.upper()}")
    return params


SNOWFLAKE_SYSTEM_DATABASES = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}


def _discover_snowflake_namespace(params: dict[str, Any]) -> None:
    import snowflake.connector

    connect_params = {
        key: params[key]
        for key in ("account", "user", "password", "role", "private_key_file", "authenticator")
        if params.get(key)
    }
    with snowflake.connector.connect(**connect_params) as connection, connection.cursor() as cursor:
        if not params.get("warehouse"):
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            warehouse = cursor.fetchone()[0]
            if not warehouse:
                cursor.execute("SHOW WAREHOUSES")
                rows = cursor.fetchall()
                warehouse = rows[0][0] if rows else None
            params["warehouse"] = warehouse
        if not params.get("database"):
            cursor.execute("SELECT CURRENT_DATABASE()")
            database = cursor.fetchone()[0]
            if not database:
                cursor.execute("SHOW DATABASES")
                names = [row[1] for row in cursor.fetchall()]
                candidates = [name for name in names if name.upper() not in SNOWFLAKE_SYSTEM_DATABASES]
                database = candidates[0] if candidates else None
            params["database"] = database


class SnowflakeHarness(WarehouseHarness):
    name = "snowflake"
    project_name = "snowflake_semantic_view"
    conn_id = "snowflake_default"
    profile_name = "snowflake_semantic_view"
    semantic_object_model = "orders_semantic_view"
    materialization = "semantic_view"
    status_column = "order_status"

    def __init__(self) -> None:
        super().__init__()
        self.database = ""

    def configure(self) -> None:
        if not os.getenv(self.conn_env_var):
            params = _snowflake_params_from_env() or _snowflake_params_from_connections_file()
            if params is None:
                pytest.skip(
                    "Snowflake credentials not found: set AIRFLOW_CONN_SNOWFLAKE_DEFAULT, SNOWFLAKE_* variables, "
                    f"or provide {SNOWFLAKE_CONNECTIONS_FILE}"
                )
            missing = [key for key in ("account", "user") if not params.get(key)]
            if missing:
                pytest.skip(f"Snowflake connection is missing {missing}")
            authenticator = str(params.get("authenticator") or "").lower()
            if authenticator in SNOWFLAKE_INTERACTIVE_AUTHENTICATORS:
                pytest.skip(f"Snowflake authenticator {authenticator!r} needs interactive login")
            if not params.get("password") and not params.get("private_key_file"):
                pytest.skip("Snowflake connection needs a password or a private key file")
            if not params.get("database") or not params.get("warehouse"):
                _discover_snowflake_namespace(params)
            missing = [key for key in ("database", "warehouse") if not params.get(key)]
            if missing:
                pytest.skip(f"Snowflake connection is missing {missing}; set SNOWFLAKE_DATABASE / SNOWFLAKE_WAREHOUSE")
            print(f"Snowflake namespace: database={params['database']} warehouse={params['warehouse']}")
            extra = {
                key: params[key]
                for key in ("account", "warehouse", "database", "role", "private_key_file", "authenticator")
                if params.get(key)
            }
            os.environ[self.conn_env_var] = json.dumps(
                {
                    "conn_type": "snowflake",
                    "login": params["user"],
                    "password": params.get("password") or "",
                    "schema": params.get("schema") or "PUBLIC",
                    "extra": extra,
                }
            )
        os.environ[SCHEMA_ENV_VAR] = self.schema
        connection = _airflow_connection(self.conn_id)
        extra = connection.extra_dejson
        self.database = extra["database"]
        candidate_params = {
            "account": extra.get("account"),
            "user": connection.login,
            "password": connection.password,
            "warehouse": extra.get("warehouse"),
            "database": self.database,
            "role": extra.get("role"),
            "private_key_file": extra.get("private_key_file"),
            "authenticator": extra.get("authenticator"),
        }
        self.connection_params = {key: value for key, value in candidate_params.items() if value}
        self.ensure_schema(os.getenv("SNOWFLAKE_SCHEMA") or connection.schema)

    def profile_config(self) -> ProfileConfig:
        return ProfileConfig(
            profile_name=self.profile_name,
            target_name="dev",
            profile_mapping=get_automatic_profile_mapping(self.conn_id, profile_args={"schema": self.schema}),
        )

    def connect(self) -> Any:
        import snowflake.connector

        return snowflake.connector.connect(**self.connection_params)

    def qualified(self, name: str) -> str:
        return f"{self.database}.{self.schema}.{name}"

    def schema_qualified(self) -> str:
        return f"{self.database}.{self.schema}"

    def drop_semantic_object_sql(self) -> str:
        return f"DROP SEMANTIC VIEW IF EXISTS {self.qualified(self.semantic_object_model)}"

    def describe_object(self, cursor: Any, name: str) -> dict[str, Any]:
        cursor.execute(f"SHOW SEMANTIC VIEWS LIKE '{name.upper()}' IN SCHEMA {self.database}.{self.schema}")
        semantic_views = cursor.fetchall()
        cursor.execute(
            f"SELECT table_type FROM {self.database}.information_schema.tables "
            f"WHERE table_schema = '{self.schema.upper()}' AND table_name = '{name.upper()}'"
        )
        rows = cursor.fetchall()
        return {"semantic_views": len(semantic_views), "table_type": rows[0][0] if rows else None}

    def is_semantic_object(self, description: dict[str, Any]) -> bool:
        return description["semantic_views"] == 1

    def fetch_revenue_by_status_from_semantic_object(self, cursor: Any) -> dict[str, float]:
        cursor.execute(
            "SELECT order_status, total_revenue FROM SEMANTIC_VIEW("
            f"{self.qualified(self.semantic_object_model)} DIMENSIONS orders.order_status METRICS orders.total_revenue)"
        )
        return _rows_to_amount_by_status(cursor.fetchall())

    def fetch_downstream_revenue_by_status(self, cursor: Any) -> dict[str, float]:
        cursor.execute(f"SELECT order_status, total_revenue FROM {self.qualified('revenue_by_status')}")
        return _rows_to_amount_by_status(cursor.fetchall())


HARNESSES = {"snowflake": SnowflakeHarness, "databricks": DatabricksHarness}

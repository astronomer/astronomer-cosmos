"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from .base import BaseProfileMapping


class DatabricksTokenProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Databricks connections to dbt profiles.
    """

    connection_type: str = "databricks"

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.

        Required by dbt:
        https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
        - schema
        - host
        - http_path
        - token
        """
        if self.conn.conn_type != self.connection_type:
            return False

        if not self.schema:
            return False

        if not self.conn.host:
            return False

        if not self.conn.extra_dejson.get("http_path") and not self.profile_args.get(
            "http_path"
        ):
            return False

        if not self.conn.password:
            return False

        return True

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Databricks profile based on the Airflow Databricks connection.

        Token is stored in an environment variable to avoid writing it to disk.

        https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
        https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
        """
        return {
            "type": "databricks",
            "schema": self.schema,
            "host": self.conn.host,
            "http_path": self.conn.extra_dejson.get("http_path"),
            "token": self.get_env_var_format("token"),
            **self.profile_args,
        }

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {
            self.get_env_var_name("token"): str(self.conn.password),
        }

    @property
    def token(self) -> str | None:
        """
        Returns the token.
        """
        return self.conn.password or self.profile_args.get("token")

    @property
    def catalog(self) -> str | None:
        """
        Returns the catalog. Order of precedence:
        1. profile_args
        2. conn.schema
        3. default catalog: "hive_metastore"

        https://docs.databricks.com/data-governance/unity-catalog/hive-metastore.html#default-catalog
        """
        if self.profile_args.get("catalog"):
            return self.profile_args.get("catalog")

        if self.conn.schema:
            return self.conn.schema

        return "hive_metastore"

    @property
    def host(self) -> str:
        """
        Returns the host. Strips out https:// if it's there.
        """
        return self.conn.host.replace("https://", "")

"""
Contains the Airflow Databricks connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from ..base import BaseProfileMapping

logger = getLogger(__name__)


class DatabricksTokenProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Databricks connections to dbt profiles.
    """

    airflow_connection_type: str = "databricks"

    # https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
    required_fields = [
        "host",
        "schema",
        "token",
        "http_path",
    ]

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
            "host": self.host,
            "http_path": self.http_path,
            "token": self.get_env_var_format("token"),
            **self.profile_args,
        }

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        env_vars = {}

        if self.token:
            env_vars[self.get_env_var_name("token")] = self.token

        return env_vars

    @property
    def token(self) -> str | None:
        """
        Token can come from:
        - profile_args.password
        - Airflow's conn.password
        """
        if self.profile_args.get("token"):
            return str(self.profile_args["token"])

        if self.conn.password:
            return str(self.conn.password)

        return None

    @property
    def host(self) -> str | None:
        """
        Host can come from:
        - profile_args.host
        - Airflow's conn.host

        Removes the https:// prefix.
        """
        if self.profile_args.get("host"):
            return str(self.profile_args["host"]).replace("https://", "")

        if self.conn.host:
            return str(self.conn.host).replace("https://", "")

        return None

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args.schema
        - Airflow's conn.schema
        """
        if self.profile_args.get("schema"):
            return str(self.profile_args["schema"])

        if self.conn.schema:
            return str(self.conn.schema)

        return None

    @property
    def http_path(self) -> str | None:
        """
        http_path can come from:
        - profile_args.http_path
        - Airflow's conn.extra_dejson.http_path
        """
        if self.profile_args.get("http_path"):
            return str(self.profile_args["http_path"])

        if self.conn.extra_dejson.get("http_path"):
            return str(self.conn.extra_dejson["http_path"])

        return None

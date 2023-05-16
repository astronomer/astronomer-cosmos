"""
Contains the Airflow Spark connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from ..base import BaseProfileMapping

logger = getLogger(__name__)


class SparkThriftProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow Spark connections to dbt profiles.
    """

    airflow_connection_type: str = "spark"
    is_community: bool = True

    # https://docs.getdbt.com/reference/warehouse-setups/spark-setup#thrift
    required_fields = [
        "schema",
        "host",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Spark profile based on the Airflow Spark connection.
        """
        profile_vars = {
            "type": "spark",
            "method": "thrift",
            "schema": self.schema,
            "host": self.host,
            "port": self.conn.port,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)

    @property
    def schema(self) -> str | None:
        """
        Schema can come from:
        - profile_args.schema
        """
        if self.profile_args.get("schema"):
            return str(self.profile_args.get("schema"))

        return None

    @property
    def host(self) -> str | None:
        """
        Host can come from:
        - profile_args.host
        - Airflow conn.host
        """
        if self.profile_args.get("host"):
            return str(self.profile_args.get("host"))

        if self.conn.host:
            return str(self.conn.host)

        return None

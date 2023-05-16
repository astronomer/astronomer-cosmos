"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from logging import getLogger
from typing import Any

from cosmos.providers.dbt.core.profiles.base import BaseProfileMapping

logger = getLogger(__name__)


class GoogleCloudServiceAccountFileProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow GCP connections to dbt profiles.
    Used when there's a service account file.
    """

    airflow_connection_type: str = "google-cloud-platform"

    # https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    required_fields = [
        "project",
        "dataset",
        "keyfile",
    ]

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt BigQuery profile based on the Airflow GCP connection.

        https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
        https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
        """
        return {
            "type": "bigquery",
            "method": "service-account",
            "project": self.project,
            "dataset": self.dataset,
            "threads": self.profile_args.get("threads") or 1,
            "keyfile": self.keyfile,
            **self.profile_args,
        }

    @property
    def project(self) -> str | None:
        """
        Project ID can come from:
        - profile_args.project
        - Airflow conn.extra.project
        """
        if self.profile_args.get("project"):
            return str(self.profile_args.get("project"))

        if self.conn.extra_dejson.get("project"):
            return str(self.conn.extra_dejson.get("project"))

        return None

    @property
    def dataset(self) -> str | None:
        """
        Dataset can come from:
        - profile_args.dataset
        """
        if self.profile_args.get("dataset"):
            return str(self.profile_args.get("dataset"))

        return None

    @property
    def keyfile(self) -> str | None:
        """
        Keyfile can come from:
        - profile_args.keyfile
        - Airflow conn.extra.key_path
        """
        if self.profile_args.get("keyfile"):
            return str(self.profile_args.get("keyfile"))

        if self.conn.extra_dejson.get("key_path"):
            return str(self.conn.extra_dejson.get("key_path"))

        return None

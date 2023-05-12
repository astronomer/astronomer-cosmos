"""
Contains the Airflow Snowflake connection -> dbt profile mapping.
"""
from __future__ import annotations

from typing import Any

from cosmos.providers.dbt.core.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountFileProfileMapping(BaseProfileMapping):
    """
    Class responsible for mapping Airflow GCP connections to dbt profiles.
    Used when there's a service account file.
    """

    connection_type: str = "google-cloud-platform"

    def validate_connection(self) -> bool:
        """
        Return whether the connection is valid for this profile mapping.

        Required by dbt:
        https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
        - project
        - dataset
        - keyfile
        """
        if self.conn.conn_type != self.connection_type:
            return False

        if not self.project:
            return False

        if not self.dataset:
            return False

        if not self.key_path:
            return False

        return True

    def get_profile(self) -> dict[str, Any | None]:
        """
        Return a dbt BigQuery profile based on the Airflow GCP connection.

        https://docs.getdbt.com/reference/warehouse-setups/databricks-setup
        https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
        """
        return {
            "type": "bigquery",
            "method": "service-account",
            "project": self.project,
            "dataset": self.dataset,
            "threads": self.profile_args.get("threads") or 1,
            "keyfile": self.key_path,
            **self.profile_args,
        }

    def get_env_vars(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that should be set.
        """
        return {}

    @property
    def project(self) -> str:
        """
        Returns the project ID.
        """
        return str(self.profile_args.get("project") or self.conn.extra_dejson.get("project"))

    @property
    def dataset(self) -> str:
        """
        Returns the dataset ID.
        """
        return str(self.profile_args.get("dataset"))

    @property
    def key_path(self) -> str:
        """
        Returns the path to the service account file.
        """
        return str(self.profile_args.get("key_path") or self.conn.extra_dejson.get("key_path"))

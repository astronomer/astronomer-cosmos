"Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account keyfile dict/json."
from __future__ import annotations

from typing import Any

from cosmos.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountDictProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account keyfile dict/json.

    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """

    airflow_connection_type: str = "google_cloud_platform"

    required_fields = [
        "project",
        "dataset",
        "keyfile_dict",
    ]

    airflow_param_mapping = {
        "project": "extra.project",
        # multiple options for dataset because of older Airflow versions
        "dataset": ["extra.dataset", "dataset"],
        # multiple options for keyfile_dict param name because of older Airflow versions
        "keyfile_dict": ["extra.keyfile_dict", "keyfile_dict", "extra__google_cloud_platform__keyfile_dict"],
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """
        Generates a GCP profile.
        Even though the Airflow connection contains hard-coded Service account credentials,
        we generate a temporary file and the DBT profile uses it.
        """
        return {
            "type": "bigquery",
            "method": "service-account-json",
            "project": self.project,
            "dataset": self.dataset,
            "threads": self.profile_args.get("threads") or 1,
            "keyfile_json": self.keyfile_dict,
            **self.profile_args,
        }

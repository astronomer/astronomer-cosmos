"Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file."
from __future__ import annotations

from typing import Any

from cosmos.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountFileProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file.

    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """

    airflow_connection_type: str = "google_cloud_platform"

    required_fields = [
        "project",
        "dataset",
        "keyfile",
    ]

    airflow_param_mapping = {
        "project": "extra.project",
        "dataset": "extra.dataset",
        "keyfile": "extra.key_path",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Generates profile. Defaults `threads` to 1."
        return {
            **self.mapped_params,
            "type": "bigquery",
            "method": "service-account",
            "threads": 1,
            **self.profile_args,
        }

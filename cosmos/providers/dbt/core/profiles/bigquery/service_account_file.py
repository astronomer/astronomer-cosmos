"Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file."
from __future__ import annotations

from typing import Any

from cosmos.providers.dbt.core.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountFileProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file.

    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html
    """

    airflow_connection_type: str = "google-cloud-platform"

    required_fields = [
        "project",
        "dataset",
        "keyfile",
    ]

    airflow_param_mapping = {
        "project": "extra.project",
        "dataset": "dataset",
        "keyfile": "extra.key_path",
    }

    def get_profile(self) -> dict[str, Any | None]:
        "Generates profile. Defaults `threads` to 1."
        return {
            "type": "bigquery",
            "method": "service-account",
            "project": self.project,
            "dataset": self.dataset,
            "threads": self.profile_args.get("threads") or 1,
            "keyfile": self.keyfile,
            **self.profile_args,
        }

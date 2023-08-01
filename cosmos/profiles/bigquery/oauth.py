"Maps Airflow GCP connections to dbt BigQuery profiles that uses oauth via gcloud, if they don't use key file or JSON."
from typing import Any

from cosmos.profiles.base import BaseProfileMapping


class GoogleCloudOauthProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles that uses oauth via gcloud, if they don't use key file or JSON.

    https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup#oauth-via-gcloud
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """

    airflow_connection_type: str = "google_cloud_platform"

    required_fields = [
        "project",
        "dataset",
    ]

    airflow_param_mapping = {
        "project": "extra.project",
        "dataset": "extra.dataset",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Generates profile. Defaults `threads` to 1."
        return {
            **self.mapped_params,
            "type": "bigquery",
            "method": "oauth",
            "threads": 1,
            **self.profile_args,
        }

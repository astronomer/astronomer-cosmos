"Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account keyfile dict/json."
from __future__ import annotations

from typing import Any
import json
from cosmos.exceptions import CosmosValueError

from cosmos.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountDictProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account keyfile dict/json.

    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """

    airflow_connection_type: str = "google_cloud_platform"
    dbt_profile_type: str = "bigquery"
    dbt_profile_method: str = "service-account-json"

    required_fields = [
        "project",
        "dataset",
        "keyfile_json",
    ]

    secret_fields = ["private_key_id", "private_key"]

    airflow_param_mapping = {
        "project": "extra.project",
        # multiple options for dataset because of older Airflow versions
        "dataset": "extra.dataset",
        # multiple options for keyfile_dict param name because of older Airflow versions
        "keyfile_json": ["extra.keyfile_dict", "keyfile_dict", "extra__google_cloud_platform__keyfile_dict"],
    }

    _env_vars: dict[str, str] = {}

    @property
    def profile(self) -> dict[str, Any | None]:
        """
        Generates a GCP profile.
        Even though the Airflow connection contains hard-coded Service account credentials,
        we generate a temporary file and the DBT profile uses it.
        """
        return {
            **self.mapped_params,
            "threads": 1,
            **self.profile_args,
        }

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        "Generates mock profile. Defaults `threads` to 1."
        parent_mock_profile = super().mock_profile

        return {
            **parent_mock_profile,
            "threads": 1,
        }

    def transform_keyfile_json(self, keyfile_json: str | dict[str, str]) -> dict[str, str]:
        """
        Transforms the keyfile_json param to a dict if it is a string, and sets environment
        variables for the service account json secret fields.
        """
        if isinstance(keyfile_json, dict):
            keyfile_json_dict = keyfile_json
        else:
            keyfile_json_dict = json.loads(keyfile_json)
            if not isinstance(keyfile_json_dict, dict):
                raise CosmosValueError("keyfile_json cannot be loaded as a dict.")

        for field in self.secret_fields:
            value = keyfile_json_dict.get(field)
            if value is None:
                raise CosmosValueError(f"Could not find a value in service account json field: {field}.")
            env_var_name = self.get_env_var_name(field)
            self._env_vars[env_var_name] = value
            keyfile_json_dict[field] = self.get_env_var_format(field)

        return keyfile_json_dict

    @property
    def env_vars(self) -> dict[str, str]:
        "Returns a dictionary of environment variables that should be set based on self.secret_fields."
        return self._env_vars

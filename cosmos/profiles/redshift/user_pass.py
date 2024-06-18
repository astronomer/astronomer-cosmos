"""Maps Airflow Redshift connections to dbt Redshift profiles if they use a username and password."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class RedshiftUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Redshift connections to dbt Redshift profiles if they use a username and password.
    https://docs.getdbt.com/reference/warehouse-setups/redshift-setup
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html
    """

    airflow_connection_type: str = "redshift"
    dbt_profile_type: str = "redshift"

    required_fields = [
        "host",
        "user",
        "password",
        "dbname",
        "schema",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "port": "port",
        "dbname": "schema",
        "timeout": "extra.timeout",
        "sslmode": "extra.sslmode",
        "region": "extra.region",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        profile = {
            "port": 5439,
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile. Defaults port to 5439."""
        parent_mock = super().mock_profile

        return {
            "port": 5439,
            **parent_mock,
        }

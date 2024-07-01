"""Maps Airflow Snowflake connections to dbt profiles if they use a user/password."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..base import BaseProfileMapping

if TYPE_CHECKING:
    pass


class TeradataUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Teradata connections using user + password authentication to dbt profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/teradata-setup
    https://airflow.apache.org/docs/apache-airflow-providers-teradata/stable/connections/teradata.html
    """

    airflow_connection_type: str = "teradata"
    dbt_profile_type: str = "teradata"

    required_fields = [
        "host",
        "user",
        "password",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "schema": "schema",
        "tmode": "extra.tmode",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            "port": 1025,
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile. Defaults port to 1025."""
        parent_mock = super().mock_profile

        return {
            "port": 1025,
            **parent_mock,
        }

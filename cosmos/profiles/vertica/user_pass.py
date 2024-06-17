"""Maps Airflow Vertica connections using username + password authentication to dbt profiles."""

from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class VerticaUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Vertica connections using username + password authentication to dbt profiles.
    .. note::
       Use Airflow connection ``schema`` for vertica ``database`` to keep it consistent with other connection types and profiles. \
       The Vertica Airflow provider hook `assumes this <https://github.com/apache/airflow/blob/395ac463494dba1478a05a32900218988495889c/airflow/providers/vertica/hooks/vertica.py#L72>`_.
       This seems to be a common approach also for `Postgres <https://github.com/apache/airflow/blob/0953e0f844fa5db81c2b461ec2433de1935260b3/airflow/providers/postgres/hooks/postgres.py#L138>`_, \
       Redshift and Exasol since there is no ``database`` field in Airflow connection and ``schema`` is not required for the database connection.
    .. seealso::
       https://docs.getdbt.com/reference/warehouse-setups/vertica-setup
       https://airflow.apache.org/docs/apache-airflow-providers-vertica/stable/connections/vertica.html
    """

    airflow_connection_type: str = "vertica"
    dbt_profile_type: str = "vertica"

    required_fields = [
        "host",
        "username",
        "password",
        "database",
        "schema",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "username": "login",
        "password": "password",
        "port": "port",
        "database": "schema",
        "autocommit": "extra.autocommit",
        "backup_server_node": "extra.backup_server_node",
        "binary_transfer": "extra.binary_transfer",
        "connection_load_balance": "extra.connection_load_balance",
        "connection_timeout": "extra.connection_timeout",
        "disable_copy_local": "extra.disable_copy_local",
        "kerberos_host_name": "extra.kerberos_host_name",
        "kerberos_service_name": "extra.kerberos_service_name",
        "log_level": "extra.log_level",
        "log_path": "extra.log_path",
        "oauth_access_token": "extra.oauth_access_token",
        "request_complex_types": "extra.request_complex_types",
        "session_label": "extra.session_label",
        "ssl": "extra.ssl",
        "unicode_error": "extra.unicode_error",
        "use_prepared_statements": "extra.use_prepared_statements",
        "workload": "extra.workload",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""
        profile = {
            "port": 5433,
            **self.mapped_params,
            **self.profile_args,
            # password should always get set as env var
            "password": self.get_env_var_format("password"),
        }

        return self.filter_null(profile)

    @property
    def mock_profile(self) -> dict[str, Any | None]:
        """Gets mock profile. Defaults port to 5433."""
        parent_mock = super().mock_profile

        return {
            "port": 5433,
            **parent_mock,
        }

"""Maps Airflow AWS connections to a dbt Athena profile using an access key id and secret access key."""

from __future__ import annotations

from typing import Any

from cosmos.exceptions import CosmosValueError

from ..base import BaseProfileMapping


class AthenaAccessKeyProfileMapping(BaseProfileMapping):
    """
    Uses the Airflow AWS Connection provided to get_credentials() to generate the profile for dbt.

    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html


    This behaves similarly to other provider operators such as the AWS Athena Operator.
    Where you pass the aws_conn_id and the operator will generate the credentials for you.

    https://registry.astronomer.io/providers/amazon/versions/latest/modules/athenaoperator

    Information about the dbt Athena profile that is generated can be found here:

    https://github.com/dbt-athena/dbt-athena?tab=readme-ov-file#configuring-your-profile
    https://docs.getdbt.com/docs/core/connect-data-platform/athena-setup
    """

    airflow_connection_type: str = "aws"
    dbt_profile_type: str = "athena"
    is_community: bool = True
    temporary_credentials = None

    required_fields = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "database",
        "region_name",
        "s3_staging_dir",
        "schema",
    ]
    airflow_param_mapping = {
        "aws_profile_name": "extra.aws_profile_name",
        "database": "extra.database",
        "debug_query_state": "extra.debug_query_state",
        "lf_tags_database": "extra.lf_tags_database",
        "num_retries": "extra.num_retries",
        "poll_interval": "extra.poll_interval",
        "region_name": "extra.region_name",
        "s3_data_dir": "extra.s3_data_dir",
        "s3_data_naming": "extra.s3_data_naming",
        "s3_staging_dir": "extra.s3_staging_dir",
        "schema": "extra.schema",
        "seed_s3_upload_args": "extra.seed_s3_upload_args",
        "work_group": "extra.work_group",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile. The password is stored in an environment variable."""

        self.temporary_credentials = self._get_temporary_credentials()  # type: ignore

        profile = {
            **self.mapped_params,
            **self.profile_args,
            "aws_access_key_id": self.temporary_credentials.access_key,
            "aws_secret_access_key": self.get_env_var_format("aws_secret_access_key"),
        }

        if self.temporary_credentials.token:
            profile["aws_session_token"] = self.get_env_var_format("aws_session_token")

        return self.filter_null(profile)

    @property
    def env_vars(self) -> dict[str, str]:
        """Overwrites the env_vars for athena, Returns a dictionary of environment variables that should be set based on the self.temporary_credentials."""

        if self.temporary_credentials is None:
            raise CosmosValueError(f"Could not find the athena credentials.")

        env_vars = {}

        env_secret_key_name = self.get_env_var_name("aws_secret_access_key")
        env_session_token_name = self.get_env_var_name("aws_session_token")

        env_vars[env_secret_key_name] = str(self.temporary_credentials.secret_key)
        env_vars[env_session_token_name] = str(self.temporary_credentials.token)

        return env_vars

    def _get_temporary_credentials(self):  # type: ignore
        """
        Helper function to retrieve temporary short lived credentials
        Returns an object including access_key, secret_key and token
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

        hook = AwsGenericHook(self.conn_id)  # type: ignore
        credentials = hook.get_credentials()
        return credentials

"Maps Airflow AWS connections to a dbt Athena profile using an access key id and secret access key."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class AthenaAccessKeyProfileMapping(BaseProfileMapping):
    """
    Maps Airflow AWS connections to a dbt Athena profile using an access key id and secret access key.

    https://docs.getdbt.com/docs/core/connect-data-platform/athena-setup
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
    """

    airflow_connection_type: str = "aws"
    dbt_profile_type: str = "athena"
    is_community: bool = True

    required_fields = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "database",
        "region_name",
        "s3_staging_dir",
        "schema",
    ]
    secret_fields = ["aws_secret_access_key", "aws_session_token"]
    airflow_param_mapping = {
        "aws_access_key_id": "login",
        "aws_secret_access_key": "password",
        "aws_session_token": "extra.aws_session_token",
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
        "Gets profile. The password is stored in an environment variable."

        credentials = self.get_temporary_credentials()

        profile = {
            **self.mapped_params,
            **self.profile_args,
            "aws_secret_access_key": credentials.access_key,
            "aws_session_token": credentials.token,
            "aws_access_key_id": credentials.secret_key,
        }

        return self.filter_null(profile)

    def get_temporary_credentials(self):  # type: ignore
        """
        Helper function to retrieve temporary short lived credentials
        Returns an object including access_key, secret_key and token
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

        # Generic hook takes care of secret obsufication
        hook = AwsGenericHook(self.conn_id)  # type: ignore
        credentials = hook.get_credentials()
        return credentials

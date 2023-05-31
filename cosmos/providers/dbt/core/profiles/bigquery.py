from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import Connection

bigquery_profile = {
    "outputs": {
        "dev": {
            "type": "bigquery",
            "method": "service-account-json",
            "project": "{{ env_var('BIGQUERY_PROJECT') }}",
            "dataset": "{{ env_var('BIGQUERY_DATASET') }}",
            "keyfile_json": {
                "type": "{{ env_var('BIGQUERY_TYPE') }}",
                "project_id": "{{ env_var('BIGQUERY_PROJECT_ID') }}",
                "private_key_id": "{{ env_var('BIGQUERY_PRIVATE_KEY_ID') }}",
                "private_key": "{{ env_var('BIGQUERY_PRIVATE_KEY') }}",
                "client_email": "{{ env_var('BIGQUERY_CLIENT_EMAIL') }}",
                "client_id": "{{ env_var('BIGQUERY_CLIENT_ID') }}",
                "auth_uri": "{{ env_var('BIGQUERY_AUTH_URI') }}",
                "token_uri": "{{ env_var('BIGQUERY_TOKEN_URI') }}",
                "auth_provider_x509_cert_url": "{{ env_var('BIGQUERY_AUTH_PROVIDER_X509_CERT_URL') }}",
                "client_x509_cert_url": "{{ env_var('BIGQUERY_CLIENT_X509_CERT_URL') }}",
            },
        }
    },
    "target": "dev",
}


def create_profile_vars_google_cloud_platform(
    conn: Connection,
    database_override: str | None = None,
    schema_override: str | None = None,
) -> tuple[str, dict[str, str]]:
    """
    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """
    bigquery_key_file = conn.extra_dejson.get("extra__google_cloud_platform__keyfile_dict") or conn.extra_dejson.get(
        "keyfile_dict"
    )
    bigquery_key_file = json.loads(bigquery_key_file or "{}")

    if not bigquery_key_file:
        raise ValueError("BigQuery key file not found in connection parameters")

    if not schema_override:
        raise ValueError("A bigquery dataset must be provided via the `schema` parameter")

    profile_vars = {
        "BIGQUERY_DATASET": schema_override,
        "BIGQUERY_PROJECT": database_override if database_override else bigquery_key_file["project_id"],
        "BIGQUERY_TYPE": bigquery_key_file["type"],
        "BIGQUERY_PROJECT_ID": bigquery_key_file["project_id"],
        "BIGQUERY_PRIVATE_KEY_ID": bigquery_key_file["private_key_id"],
        "BIGQUERY_PRIVATE_KEY": bigquery_key_file["private_key"],
        "BIGQUERY_CLIENT_EMAIL": bigquery_key_file["client_email"],
        "BIGQUERY_CLIENT_ID": bigquery_key_file["client_id"],
        "BIGQUERY_AUTH_URI": bigquery_key_file["auth_uri"],
        "BIGQUERY_TOKEN_URI": bigquery_key_file["token_uri"],
        "BIGQUERY_AUTH_PROVIDER_X509_CERT_URL": bigquery_key_file["auth_provider_x509_cert_url"],
        "BIGQUERY_CLIENT_X509_CERT_URL": bigquery_key_file["client_x509_cert_url"],
    }
    return "bigquery_profile", profile_vars

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

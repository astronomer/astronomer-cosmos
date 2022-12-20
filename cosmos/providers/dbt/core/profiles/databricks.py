databricks_profile = {
    "outputs": {
        "dev": {
            "type": "databricks",
            "host": "{{ env_var('DATABRICKS_HOST') }}",
            "schema": "{{ env_var('DATABRICKS_SCHEMA') }}",
            "http_path": "{{ env_var('DATABRICKS_HTTP_PATH') }}",
            "token": "{{ env_var('DATABRICKS_TOKEN') }}",
        }
    },
    "target": "dev",
}

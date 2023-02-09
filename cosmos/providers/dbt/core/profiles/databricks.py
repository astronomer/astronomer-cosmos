databricks_profile = {
    "outputs": {
        "dev": {
            "type": "databricks",
            "host": "{{ env_var('DATABRICKS_HOST') }}",
            "catalog": "{{ env_var('DATABRICKS_CATALOG') }}",
            "schema": "{{ env_var('DATABRICKS_DATABASE') }}",
            "http_path": "{{ env_var('DATABRICKS_HTTP_PATH') }}",
            "token": "{{ env_var('DATABRICKS_TOKEN') }}",
        }
    },
    "target": "dev",
}

snowflake_profile = {
    "target": "dev",
    "outputs": {
        "dev": {
            "type": "snowflake",
            "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
            "user": "{{ env_var('SNOWFLAKE_USER') }}",
            "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
            "role": "{{ env_var('SNOWFLAKE_ROLE') }}",
            "database": "{{ env_var('SNOWFLAKE_DATABASE') }}",
            "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE') }}",
            "schema": "{{ env_var('SNOWFLAKE_SCHEMA') }}",
            "client_session_keep_alive": False,
        }
    },
}

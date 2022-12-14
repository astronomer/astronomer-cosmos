redshift_profile = {
    "outputs": {
        "dev": {
            "type": "redshift",
            "host": "{{ env_var('REDSHIFT_HOST') }}",
            "port": "{{ env_var('REDSHIFT_PORT') | as_number }}",
            "user": "{{ env_var('REDSHIFT_USER') }}",
            "password": "{{ env_var('REDSHIFT_PASSWORD') }}",
            "dbname": "{{ env_var('REDSHIFT_DATABASE') }}",
            "schema": "{{ env_var('REDSHIFT_SCHEMA') }}",
            "ra3_node": True,
        }
    },
    "target": "dev",
}

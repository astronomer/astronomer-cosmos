postgres_profile = {
    "outputs": {
        "dev": {
            "type": "postgres",
            "host": "{{ env_var('POSTGRES_HOST') }}",
            "port": "{{ env_var('POSTGRES_PORT') | as_number }}",
            "user": "{{ env_var('POSTGRES_USER') }}",
            "pass": "{{ env_var('POSTGRES_PASSWORD') }}",
            "dbname": "{{ env_var('POSTGRES_DATABASE') }}",
            "schema": "{{ env_var('POSTGRES_SCHEMA') }}",
        }
    },
    "target": "dev",
}

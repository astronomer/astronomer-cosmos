import os
import yaml

profile_contents = {
    "postgres_profile": {
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
}

def get_profile_path():
    """
    Get the path to the dbt profiles.yml file.
    """
    # get the path to the dbt profiles.yml file
    profile_path = os.path.join(
        os.path.expanduser("~"),
        ".dbt",
        "profile.yml",
    )

    # if it doesn't exist, create it
    if not os.path.exists(profile_path):
        # create the directory if it doesn't exist
        if not os.path.exists(os.path.dirname(profile_path)):
            os.makedirs(os.path.dirname(profile_path))

        with open(profile_path, "w") as f:
            yaml.dump(profile_contents, f)

    return profile_path
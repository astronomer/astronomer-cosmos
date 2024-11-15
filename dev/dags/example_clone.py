from datetime import datetime

from airflow import DAG

from cosmos import DbtCloneLocalOperator, DbtRunLocalOperator, DbtSeedLocalOperator, ProfileConfig

DBT_PROJ_DIR = "/usr/local/airflow/dbt/jaffle_shop"

profile_config1 = ProfileConfig(
    profile_name="bigquery_dev",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dbt/jaffle_shop/profiles.yml",
)

profile_config2 = ProfileConfig(
    profile_name="bigquery_clone",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dbt/jaffle_shop/profiles.yml",
)


with DAG("test-id-1", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    seed_operator = DbtSeedLocalOperator(
        profile_config=profile_config1,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
    )
    run_operator = DbtRunLocalOperator(
        profile_config=profile_config1,
        project_dir=DBT_PROJ_DIR,
        task_id="run",
        dbt_cmd_flags=["--models", "stg_customers"],
        install_deps=True,
        append_env=True,
    )

    # [START clone_example]
    clone_operator = DbtCloneLocalOperator(
        profile_config=profile_config2,
        project_dir=DBT_PROJ_DIR,
        task_id="clone",
        dbt_cmd_flags=["--models", "stg_customers", "--state", "/usr/local/airflow/dbt/jaffle_shop/target"],
        install_deps=True,
        append_env=True,
    )
    # [END clone_example]

    seed_operator >> run_operator >> clone_operator

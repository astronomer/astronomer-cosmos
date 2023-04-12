docker_compose('dev/docker-compose.yaml')

sync_pyproj_toml = sync('./pyproject.toml', '/usr/local/airflow/cosmos/pyproject.toml')
sync_readme = sync('./README.md', '/usr/local/airflow/cosmos/README.md')
sync_src = sync('./src', '/usr/local/airflow/cosmos/src')
sync_dev_dir = sync('./dev', '/usr/local/airflow/cosmos/dev')

docker_build(
    'cosmos',
    context='.',
    dockerfile='dev/Dockerfile',
    live_update=[
        sync_pyproj_toml,
        sync_src,
        sync_readme,
        sync_dev_dir,
        run(
            'cd /usr/local/airflow/astro_databricks && pip install -e .',
            trigger=['pyproject.toml']
        ),
    ]
)
docker_compose('dev/docker-compose.yaml')

sync_pyproj_toml = sync('./pyproject.toml', '/usr/local/airflow/astronomer_cosmos/pyproject.toml')
sync_readme = sync('./README.md', '/usr/local/airflow/astronomer_cosmos/README.md')
sync_src = sync('./cosmos', '/usr/local/airflow/astronomer_cosmos/cosmos')
sync_dev_dir = sync('./dev', '/usr/local/airflow/astronomer_cosmos/dev')

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
            'cd /usr/local/airflow/astronomer_cosmos && pip install -e .',
            trigger=['pyproject.toml']
        ),
    ]
)

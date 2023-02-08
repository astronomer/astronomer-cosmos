FROM quay.io/astronomer/astro-runtime:7.2.0

# install dbt into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# install cosmos from dev branch
RUN pip install git+https://github.com/astronomer/astronomer-cosmos.git@main#egg=astronomer-cosmos

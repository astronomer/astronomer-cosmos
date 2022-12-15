FROM quay.io/astronomer/astro-runtime:7.0.0

#Install then venv /usr/local/airflow/dbt_venv/bin/activate
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

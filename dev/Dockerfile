FROM quay.io/astronomer/astro-runtime:13.1.0-base

USER root


# dbt-postgres 1.8.0 requires building psycopg2 from source
RUN /bin/sh -c set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends build-essential libpq-dev; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

RUN pip install -U uv

COPY ./pyproject.toml  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./README.rst  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./cosmos/  ${AIRFLOW_HOME}/astronomer_cosmos/cosmos/
COPY ./dev/requirements.txt ${AIRFLOW_HOME}/requirements.txt
# install the package in editable mode
RUN uv pip install --system -e "${AIRFLOW_HOME}/astronomer_cosmos"[dbt-postgres,dbt-databricks,dbt-bigquery] && \
    uv pip install --system -r ${AIRFLOW_HOME}/requirements.txt


# make sure astro user owns the package
RUN chown -R astro:astro ${AIRFLOW_HOME}/astronomer_cosmos

USER astro

# add a connection to the airflow db for testing
ENV AIRFLOW_CONN_AIRFLOW_DB=postgres://airflow:pg_password@postgres:5432/airflow
ENV AIRFLOW_CONN_EXAMPLE_CONN=postgres://airflow:pg_password@postgres:5432/airflow
ENV DBT_ROOT_PATH=/usr/local/airflow/dags/dbt
ENV DBT_DOCS_PATH=/usr/local/airflow/dbt-docs

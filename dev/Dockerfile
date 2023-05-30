FROM quay.io/astronomer/astro-runtime:7.3.0-base

USER root

COPY ./pyproject.toml  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./README.rst  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./cosmos/  ${AIRFLOW_HOME}/astronomer_cosmos/cosmos/

# install the package in editable mode
RUN pip install -e "${AIRFLOW_HOME}/astronomer_cosmos"[dbt-postgres]

# make sure astro user owns the package
RUN chown -R astro:astro ${AIRFLOW_HOME}/astronomer_cosmos

USER astro

# add a connection to the airflow db for testing
ENV AIRFLOW_CONN_AIRFLOW_DB=postgres://airflow:pg_password@postgres:5432/airflow
ENV DBT_ROOT_PATH=/usr/local/airflow/dags/dbt
ENV DBT_DOCS_PATH=/usr/local/airflow/dbt-docs
